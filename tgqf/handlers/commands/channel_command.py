# handlers/commands/channel_command.py
# -*- coding: utf-8 -*-
"""
批量频道/超级群 创建、邀请管理员和机器人，账号轮询分配、全链 trace_id、权限配置、异常日志、参数结构一次传到底
并发升级版：
- 批量创建：Semaphore 有界并发 + 抖动
- 邀请/赋权：对多个受邀者并发（同一账号下仍有限流/退避）
- Telethon 风格升级：build_reply_markup / action('typing') / client.edit_admin
"""

from __future__ import annotations

import asyncio
import random
from itertools import cycle, islice
from typing import Any, Dict, Iterable, List, Optional, Tuple

from telethon import Button, TelegramClient, events
from telethon.tl.functions.channels import (
    CreateChannelRequest,
    EditAdminRequest,
    GetParticipantRequest,
    InviteToChannelRequest,
)
from telethon.tl.types import (
    ChannelParticipant,
    ChannelParticipantAdmin,
    ChannelParticipantBanned,
    ChannelParticipantCreator,
    ChannelParticipantLeft,
    ChannelParticipantSelf,
    ChatAdminRights,
)

from core.decorators import super_command
from core.redis_fsm import RedisCommandFSM
from core.telethon_errors import TE, detect_flood_seconds
from router.command_router import main_router
from tg.conversation_flow import ConversationFlow
from tg.entity_utils import get_standard_input_peer, get_input_entity_strict
from tg.utils_username import ensure_unique_username
from unified.config import MAX_CONCURRENCY
from unified.context import get_client_manager, get_fsm, get_scheduler
from unified.logger import log_exception, log_info, log_warning
from unified.trace_context import (
    ctx_create_task,
    generate_trace_id,
    get_log_context,
    set_log_context,
)

# ───────────────────────── 常量/配置 ─────────────────────────

CONV_TIMEOUT_SEC = 3600
STEP_TIMEOUT_NAME_SEC = 300
STEP_TIMEOUT_USERNAME_SEC = 300
STEP_TIMEOUT_ADMIN_SEC = 180
STEP_TIMEOUT_PERM_SEC = 180

STEP_DELAY_INVITE_SEC = 1.0  # 单账号内部发送邀请的基础延时

try:
    _maxc = int(getattr(MAX_CONCURRENCY, "value", MAX_CONCURRENCY))
except Exception:
    _maxc = 5
CREATE_MAX_CONCURRENCY = _maxc
INVITE_MAX_CONCURRENCY_PER_CLIENT = _maxc
REPAIR_MAX_CONCURRENCY = _maxc

JITTER_SEC_MIN = 0.15
JITTER_SEC_MAX = 0.45

FLOOD_EXTRA_BACKOFF_SEC = 2.0
ONLY_ADD_RIGHTS_DEFAULT = True  # 仅补差


def spawn_bg(coro):
    """
    带上下文复制的后台任务启动器，替代 asyncio.create_task。
    会自动注入 trace 与 action 便于排查。
    参数应为“协程对象”，例如：spawn_bg(run_one())
    """
    set_log_context({"phase": "channel_command_bg", "trace_id": generate_trace_id()})
    t = ctx_create_task(coro)
    try:
        task_name = getattr(t, "get_name", lambda: "")()
    except Exception:
        task_name = ""
    log_info("📤 已提交后台任务", extra={**(get_log_context() or {}), "task_name": task_name})
    return t


# ───────────────────────── 权限映射与默认集 ─────────────────────────

PERMISSION_MAP: Dict[str, Tuple[str, str]] = {
    "1": ("post_messages", "发布消息"),
    "2": ("edit_messages", "编辑消息"),
    "3": ("delete_messages", "删除消息"),
    "4": ("ban_users", "封禁用户"),
    "5": ("invite_users", "邀请用户"),
    "6": ("pin_messages", "置顶消息"),
    "7": ("add_admins", "添加管理员"),
    "8": ("manage_call", "管理通话"),
    "9": ("change_info", "更改信息"),
    "10": ("manage_topics", "管理话题"),
    "11": ("anonymous", "匿名"),
}
RIGHTS_CN_MAP: Dict[str, str] = {v[0]: v[1] for v in PERMISSION_MAP.values()}

DEFAULT_USER_ADMIN_RIGHTS = {
    "post_messages": True,
    "edit_messages": True,
    "delete_messages": True,
    "ban_users": True,
    "invite_users": True,
    "pin_messages": True,
    "change_info": True,
    "manage_topics": True,
    "add_admins": True,
    "anonymous": False,
    "manage_call": False,
}

KNOWN_RIGHT_FIELDS = {
    "change_info",
    "post_messages",
    "edit_messages",
    "delete_messages",
    "ban_users",
    "invite_users",
    "pin_messages",
    "add_admins",
    "anonymous",
    "manage_call",
    "manage_topics",
}

# ───────────────────────── 会话取消支持 ─────────────────────────


class ConversationCancelled(Exception):
    """用户在对话过程中点击取消按钮，主动终止。"""
    pass


def _get_raw_client(event):
    return getattr(event, "client", None) or getattr(event, "_client", None)


async def _wait_cancel_click(event, data_bytes):
    """
    更严格过滤：限定 chats=event.chat_id + sender_id 相同，避免误触。
    """
    client = _get_raw_client(event)
    if client is None:
        raise RuntimeError("No client on event")

    builder = events.CallbackQuery(
        chats=event.chat_id,
        func=lambda e: e.sender_id == event.sender_id and e.data == data_bytes,
    )
    return await client.wait_event(builder)


async def _ask_text_cancelable(
    event,
    flow,
    prompt: str,
    *,
    step_timeout: int,
    parse_mode: str = "html",
    trace_id: str,
):
    """
    发送带【❌ 取消】按钮的提问；并发等待文本或取消。
    """
    from telethon import Button  # 局部导入以兼容上层运行环境

    cancel_data = f"conv_cancel:{trace_id}".encode()
    markup = event.client.build_reply_markup(
        [[Button.inline("❌ 取消", data=cancel_data)]], inline_only=True
    )

    async with event.client.action(event.chat_id, "typing"):
        await flow.conv.send_message(prompt, parse_mode=parse_mode, buttons=markup)

    async def _wait_text_only():
        msg = await flow.conv.get_response(timeout=step_timeout)
        return (msg.text or "").strip()

    ask_task = spawn_bg(_wait_text_only())
    wait_cancel = spawn_bg(_wait_cancel_click(event, cancel_data))

    done, pending = await asyncio.wait(
        {ask_task, wait_cancel},
        return_when=asyncio.FIRST_COMPLETED,
        timeout=step_timeout,
    )
    for t in pending:
        t.cancel()

    if wait_cancel in done and not wait_cancel.cancelled():
        cb = await wait_cancel
        try:
            await cb.answer("已取消本次操作")
        except Exception:
            pass
        await flow.conv.send_message("🛑 已取消并退出对话。")
        try:
            await flow.conv.cancel_all()
        except Exception:
            pass
        raise ConversationCancelled()

    if ask_task in done and not ask_task.cancelled():
        return await ask_task

    raise asyncio.TimeoutError("等待用户输入超时")


# ───────────────────────── 权限工具 ─────────────────────────


async def _get_existing_admin_rights(
    client: TelegramClient, channel, user_input
) -> Tuple[Optional[ChatAdminRights], str]:
    """
    返回 (existing_rights, role)
    role: "creator" / "admin" / "member" / "left_or_banned" / "unknown"
    """
    try:
        res = await client(GetParticipantRequest(channel=channel, participant=user_input))
    except Exception:
        return None, "unknown"

    p = getattr(res, "participant", None)
    if isinstance(p, ChannelParticipantCreator):
        return None, "creator"
    if isinstance(p, ChannelParticipantAdmin):
        return getattr(p, "admin_rights", None), "admin"
    if isinstance(p, (ChannelParticipant, ChannelParticipantSelf)):
        return None, "member"
    if isinstance(p, (ChannelParticipantLeft, ChannelParticipantBanned)):
        return None, "left_or_banned"
    return None, "unknown"


def _rights_superset(
    existing: Optional[ChatAdminRights], required: Dict[str, bool]
) -> bool:
    if not existing:
        return False
    for k, need in (required or {}).items():
        if need and not bool(getattr(existing, k, False)):
            return False
    return True


def _rights_to_labels(rights_map: Dict[str, bool]) -> List[str]:
    return [RIGHTS_CN_MAP.get(k, k) for k, v in (rights_map or {}).items() if v]


def _rights_to_dict(existing: Optional[ChatAdminRights]) -> Dict[str, bool]:
    if not existing:
        return {}
    return {k: bool(getattr(existing, k, False)) for k in KNOWN_RIGHT_FIELDS}


def _merge_rights(
    existing: Optional[ChatAdminRights], required: Dict[str, bool], only_add: bool
) -> Dict[str, bool]:
    """
    only_add=True  → merged = existing(True) ∪ required(True)
    only_add=False → required 的全量映射（覆盖）
    """
    req = {k: bool(v) for k, v in (required or {}).items() if k in KNOWN_RIGHT_FIELDS}
    if not only_add:
        return {k: req.get(k, False) for k in KNOWN_RIGHT_FIELDS}
    cur = _rights_to_dict(existing)
    merged = {k: bool(cur.get(k, False) or req.get(k, False)) for k in KNOWN_RIGHT_FIELDS}
    return {k: v for k, v in merged.items() if v}


def build_admin_rights(
    required: Dict[str, bool], *, strict: bool = False
) -> ChatAdminRights:
    """
    strict=True 时“覆盖”：所有字段显式 True/False；
    strict=False 时“增量”：只传 True 的键。
    """
    if strict:
        full = {k: bool(required.get(k, False)) for k in KNOWN_RIGHT_FIELDS}
        return ChatAdminRights(**full)
    partial = {k: True for k, v in (required or {}).items() if k in KNOWN_RIGHT_FIELDS and v}
    return ChatAdminRights(**partial)


# ───────────────────────── 小工具 ─────────────────────────


def _normalize_lines(lines: Iterable[str], *, strip_at: bool = False) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in lines or []:
        s = (raw or "").strip()
        if not s:
            continue
        if strip_at:
            s = s.lstrip("@")
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def permissions_by_ids(ids: List[str]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    for i in ids or []:
        key = (i or "").strip()
        if key and key in PERMISSION_MAP:
            out[PERMISSION_MAP[key][0]] = True
    return out


def round_robin_clients(
    clients: Dict[str, TelegramClient], n: int
) -> List[TelegramClient]:
    if not clients or n <= 0:
        return []
    vals = list(clients.values())
    m = len(vals)
    return [vals[i % m] for i in range(n)]


def round_robin_list(items: List[Any], n: int) -> List[Any]:
    if not items or n <= 0:
        return []
    return list(islice(cycle(items), n))


def _jitter() -> float:
    return random.uniform(JITTER_SEC_MIN, JITTER_SEC_MAX)


async def _run_with_semaphore(sem: asyncio.Semaphore, coro_factory):
    async with sem:
        return await coro_factory()


# ───────────────────────── 业务函数（并发化） ─────────────────────────


async def _invite_one(
    client: TelegramClient,
    channel,
    invitee: str,
    perms: Dict[str, bool],
    rank: str,
    delay: float,
    trace_id: Optional[str],
    *,
    is_megagroup: bool,
    promote_only_add: bool,
):
    """
    单个受邀对象的逻辑（供并发 worker 使用）
    """
    try:
        # 解析实体（严格：缓存 miss 自动预热 dialogs）
        entity_full = await client.get_entity(invitee)
        entity = await get_input_entity_strict(client, entity_full)
        is_bot = bool(getattr(entity_full, "bot", False))

        # 频道里的机器人：只能直接设管理员
        if not is_megagroup and is_bot:
            rights_map = perms or {"post_messages": True}
            try:
                await client.edit_admin(
                    channel,
                    entity,
                    is_admin=True,
                    title=rank or "机器人",
                    **{k: True for k, v in rights_map.items() if v},
                )
            except TE.FloodWaitError as fw:
                sec = detect_flood_seconds(fw) or getattr(fw, "seconds", 0) or 0
                wait = float(sec) + FLOOD_EXTRA_BACKOFF_SEC + _jitter()
                log_warning(
                    "频道添加机器人为管理员触发 FloodWait",
                    extra={"trace_id": trace_id, "invitee": invitee, "wait": wait},
                )
                await asyncio.sleep(wait)
                # 兜底使用显式请求
                rights = build_admin_rights(rights_map, strict=False)
                await client(
                    EditAdminRequest(
                        channel=channel,
                        user_id=entity,
                        admin_rights=rights,
                        rank=rank or "机器人",
                    )
                )
            except Exception:
                # 兜底使用显式请求
                rights = build_admin_rights(rights_map, strict=False)
                await client(
                    EditAdminRequest(
                        channel=channel,
                        user_id=entity,
                        admin_rights=rights,
                        rank=rank or "机器人",
                    )
                )

            log_info(
                "频道添加机器人为管理员成功",
                extra={
                    "trace_id": trace_id,
                    "invitee": invitee,
                    "rank": rank,
                    "perms": list(rights_map.keys()),
                    "perms_cn": _rights_to_labels(rights_map),
                },
            )
            await asyncio.sleep(delay + _jitter())
            return

        # 普通用户路径：先邀请
        invited = False
        try:
            await client(InviteToChannelRequest(channel=channel, users=[entity]))
            invited = True
            await asyncio.sleep(delay + _jitter())
        except TE.FloodWaitError as fw:
            sec = detect_flood_seconds(fw) or getattr(fw, "seconds", 0) or 0
            wait = float(sec) + FLOOD_EXTRA_BACKOFF_SEC + _jitter()
            log_warning(
                "邀请触发 FloodWait（尝试继续赋权）",
                extra={"trace_id": trace_id, "invitee": invitee, "wait": wait},
            )
            await asyncio.sleep(wait)
        except Exception as e:
            log_warning(
                "邀请失败（但尝试继续赋权）",
                extra={"trace_id": trace_id, "invitee": invitee, "err": f"{type(e).__name__}: {e}"},
            )

        looks_admin = ("管" in (rank or "")) or ((rank or "").lower() in ("admin", "administrator"))
        should_promote = bool(perms) or looks_admin
        if not should_promote:
            if invited:
                log_info(
                    "邀请成功（未赋权）",
                    extra={"trace_id": trace_id, "invitee": invitee, "rank": rank, "perms": []},
                )
            return

        required_map = perms if perms else DEFAULT_USER_ADMIN_RIGHTS

        existing_rights, role = await _get_existing_admin_rights(client, channel, entity)
        if role == "creator":
            log_info("已是创建者，跳过赋权", extra={"trace_id": trace_id, "invitee": invitee})
            return
        if role == "admin" and _rights_superset(existing_rights, required_map):
            log_info(
                "已具备目标管理员权限，跳过赋权",
                extra={
                    "trace_id": trace_id,
                    "invitee": invitee,
                    "role": role,
                    "need": list(required_map.keys()),
                    "need_cn": _rights_to_labels(required_map),
                },
            )
            return

        final_map = _merge_rights(existing_rights, required_map, promote_only_add)

        # 设管理员
        try:
            await client.edit_admin(
                channel,
                entity,
                is_admin=True,
                title=rank or "管理员",
                **{k: True for k, v in final_map.items() if v},
            )
        except TE.FloodWaitError as fw:
            sec = detect_flood_seconds(fw) or getattr(fw, "seconds", 0) or 0
            wait = float(sec) + FLOOD_EXTRA_BACKOFF_SEC + _jitter()
            log_warning(
                "赋权触发 FloodWait",
                extra={"trace_id": trace_id, "invitee": invitee, "wait": wait},
            )
            await asyncio.sleep(wait)
            rights = build_admin_rights(final_map, strict=(not promote_only_add))
            await client(
                EditAdminRequest(
                    channel=channel,
                    user_id=entity,
                    admin_rights=rights,
                    rank=rank or "管理员",
                )
            )
        except Exception:
            rights = build_admin_rights(final_map, strict=(not promote_only_add))
            await client(
                EditAdminRequest(
                    channel=channel,
                    user_id=entity,
                    admin_rights=rights,
                    rank=rank or "管理员",
                )
            )

        log_info(
            "邀请并赋权成功" if invited else "直接赋权成功",
            extra={
                "trace_id": trace_id,
                "invitee": invitee,
                "rank": rank,
                "perms": list(final_map.keys()),
                "perms_cn": _rights_to_labels(final_map),
                "prev_role": role,
                "only_add": promote_only_add,
            },
        )

    except Exception as e:
        log_warning(
            "邀请/赋权失败",
            extra={"trace_id": trace_id, "invitee": invitee, "err": f"{type(e).__name__}: {e}"},
        )


async def invite_with_delay(
    client: TelegramClient,
    channel,
    invitees: List[str],
    perms: Dict[str, bool],
    rank: str,
    delay: float = STEP_DELAY_INVITE_SEC,
    trace_id: Optional[str] = None,
    *,
    is_megagroup: bool = False,
    promote_only_add: bool = ONLY_ADD_RIGHTS_DEFAULT,
) -> None:
    """
    并发化邀请/赋权（单账号有界并发）
    """
    clean_invitees = _normalize_lines(invitees, strip_at=True)
    if not clean_invitees:
        return

    sem = asyncio.Semaphore(INVITE_MAX_CONCURRENCY_PER_CLIENT)
    tasks = [
        _run_with_semaphore(
            sem,
            lambda inv=invitee: _invite_one(
                client,
                channel,
                inv,
                perms,
                rank,
                delay,
                trace_id,
                is_megagroup=is_megagroup,
                promote_only_add=promote_only_add,
            ),
        )
        for invitee in clean_invitees
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


async def _ensure_clients_for_user(user_id: int) -> Dict[str, TelegramClient]:
    cm = get_client_manager()
    await cm.load_all_for_user(user_id)
    clients = cm.get_clients_for_user(user_id)

    if clients:
        return clients

    phones = cm.list_user_phones(user_id) or []
    for phone, session_path in phones:
        try:
            await cm.try_create_and_register(user_id, phone, session_path=session_path)
        except Exception:
            pass
    return cm.get_clients_for_user(user_id) or {}


# ───────────────────────── 参数收集（支持取消） ─────────────────────────


async def _collect_params(
    event, *, is_megagroup: bool
) -> Tuple[
    List[str], List[str], List[str], Dict[str, bool], List[str], Dict[str, bool]
]:
    what = "超级群" if is_megagroup else "频道"
    trace_id = get_log_context().get("trace_id") or generate_trace_id()
    flow = await ConversationFlow.start(event, timeout=CONV_TIMEOUT_SEC)
    async with flow.conv:
        try:
            names = _normalize_lines(
                (
                    await _ask_text_cancelable(
                        event,
                        flow,
                        f"请输入<b>{what}名称</b>，每行一个：",
                        step_timeout=STEP_TIMEOUT_NAME_SEC,
                        parse_mode="html",
                        trace_id=trace_id,
                    )
                ).splitlines()
            )
            usernames = _normalize_lines(
                (
                    await _ask_text_cancelable(
                        event,
                        flow,
                        f"请输入<b>{what}用户名</b>，每行一个（可以带@，我会自动去掉）：",
                        step_timeout=STEP_TIMEOUT_USERNAME_SEC,
                        parse_mode="html",
                        trace_id=trace_id,
                    )
                ).splitlines(),
                strip_at=True,
            )

            if not names or not usernames:
                await flow.conv.send_message("❌ 名称或用户名不能为空。")
                await flow.conv.cancel_all()
                return [], [], [], {}, [], {}

            if len(names) != len(usernames):
                await flow.conv.send_message("❌ 名称与用户名数量不一致，已取消。")
                await flow.conv.cancel_all()
                return [], [], [], {}, [], {}

            admin_users = _normalize_lines(
                (
                    await _ask_text_cancelable(
                        event,
                        flow,
                        "请输入<b>管理员 ID/用户名</b>，每行一个（可留空）：",
                        step_timeout=STEP_TIMEOUT_ADMIN_SEC,
                        parse_mode="html",
                        trace_id=trace_id,
                    )
                ).splitlines(),
                strip_at=True,
            )
            perm_cn = "\n".join([f"{k}. {v[1]}" for k, v in PERMISSION_MAP.items()])
            admin_perm_ids = (
                await _ask_text_cancelable(
                    event,
                    flow,
                    f"请选择<b>管理员权限</b>，用点号分隔（如 1.2.3，可留空）：\n{perm_cn}",
                    step_timeout=STEP_TIMEOUT_PERM_SEC,
                    parse_mode="html",
                    trace_id=trace_id,
                )
            ).strip()
            admin_perms = (
                permissions_by_ids(admin_perm_ids.split(".")) if admin_perm_ids else {}
            )

            bots = _normalize_lines(
                (
                    await _ask_text_cancelable(
                        event,
                        flow,
                        "请输入<b>机器人用户名</b>，每行一个（可留空）：",
                        step_timeout=STEP_TIMEOUT_ADMIN_SEC,
                        parse_mode="html",
                        trace_id=trace_id,
                    )
                ).splitlines(),
                strip_at=True,
            )
            bot_perm_ids = (
                await _ask_text_cancelable(
                    event,
                    flow,
                    f"请选择<b>机器人权限</b>，用点号分隔（如 1.2，可留空）：\n{perm_cn}",
                    step_timeout=STEP_TIMEOUT_PERM_SEC,
                    parse_mode="html",
                    trace_id=trace_id,
                )
            ).strip()
            bot_perms = (
                permissions_by_ids(bot_perm_ids.split(".")) if bot_perm_ids else {}
            )

            await flow.conv.cancel_all()
            return names, usernames, admin_users, admin_perms, bots, bot_perms

        except ConversationCancelled:
            await flow.conv.cancel_all()
            return [], [], [], {}, [], {}
        except Exception:
            try:
                await flow.conv.cancel_all()
            except Exception:
                pass
            raise


# ───────────────────────── 创建单个对象 ─────────────────────────


async def _create_one(
    client: TelegramClient,
    *,
    is_megagroup: bool,
    name: str,
    uname: str,
    admin_users: List[str],
    admin_perms: Dict[str, bool],
    bots: List[str],
    bot_perms: Dict[str, bool],
    event,
    idx: int,
    trace_id: str,
) -> None:
    set_log_context(
        {
            "trace_id": trace_id,
            "user_id": event.sender_id,
            "title": name,
            "idx": idx,
            "is_megagroup": is_megagroup,
            "func": "_create_one",
        }
    )
    kind_cn = "超级群" if is_megagroup else "频道"

    try:
        await asyncio.sleep(_jitter())  # 启动抖动
        log_info(f"开始创建{kind_cn}", extra=get_log_context())

        # 创建对象
        async with client.action(event.chat_id, "typing"):
            try:
                result = await client(
                    CreateChannelRequest(
                        title=name,
                        about="",
                        broadcast=not is_megagroup,
                        megagroup=is_megagroup,
                    )
                )
            except TE.FloodWaitError as fw:
                sec = detect_flood_seconds(fw) or getattr(fw, "seconds", 0) or 0
                wait = float(sec) + FLOOD_EXTRA_BACKOFF_SEC + _jitter()
                log_warning("创建触发 FloodWait", extra={**get_log_context(), "wait": wait})
                await asyncio.sleep(wait)
                await event.respond(f"⏳ 创建触发限流，已退避 {int(wait)}s。你可以稍后再试。")
                return
            except Exception as e:
                await event.respond(f"❌ 创建失败：{e}", parse_mode="html")
                return

        chat = result.chats[0]
        input_peer = get_standard_input_peer(chat)

        # 设置/校正用户名（带唯一性保障）
        final_uname, set_ok = await ensure_unique_username(
            client, input_peer, (uname or "").strip().lstrip("@")
        )
        if not set_ok:
            await event.respond(
                f"⚠️ {kind_cn} <b>{name}</b> 用户名已自动调整为 <code>{final_uname}</code>",
                parse_mode="html",
            )

        # 并发邀请/赋权
        awaits = []
        if admin_users:
            awaits.append(
                invite_with_delay(
                    client,
                    input_peer,
                    admin_users,
                    admin_perms,
                    "管理员",
                    STEP_DELAY_INVITE_SEC,
                    trace_id,
                    is_megagroup=is_megagroup,
                    promote_only_add=False,
                )
            )
        if bots:
            awaits.append(
                invite_with_delay(
                    client,
                    input_peer,
                    bots,
                    bot_perms,
                    "机器人",
                    STEP_DELAY_INVITE_SEC,
                    trace_id,
                    is_megagroup=is_megagroup,
                )
            )
        if awaits:
            await asyncio.gather(*awaits, return_exceptions=True)

        await event.respond(
            f"✅ {kind_cn} <b>{name}</b> (<code>@{final_uname}</code>) 创建完毕！",
            parse_mode="html",
        )
        log_info(f"{kind_cn}创建与邀请完成", extra={**get_log_context(), "username": final_uname})

    except Exception as e:
        log_exception("创建失败", e, extra=get_log_context())
        await event.respond(f"❌ 创建失败：{e}", parse_mode="html")


# ───────────────────────── 批量流程（并发） ─────────────────────────


async def _batch_flow(event, *, is_megagroup: bool) -> None:
    """从类型入口（频道/超级群）进入的完整流程"""
    user_id = event.sender_id
    trace_id = generate_trace_id()
    set_log_context(
        {"user_id": user_id, "trace_id": trace_id, "is_megagroup": is_megagroup, "func": "_batch_flow"}
    )

    clients = await _ensure_clients_for_user(user_id)
    if not clients:
        await event.respond("❌ 当前没有可用账号。请先上传/登录。")
        log_warning("无可用账号", extra=get_log_context())
        return

    try:
        names, usernames, admin_users, admin_perms, bots, bot_perms = (
            await _collect_params(event, is_megagroup=is_megagroup)
        )
    except ConversationCancelled:
        await event.respond("ℹ️ 已退出创建流程。")
        return
    except asyncio.TimeoutError:
        await event.respond("⌛ 等待输入超时，已退出创建流程。可重新执行命令开始。")
        return

    if not names:
        await event.respond("ℹ️ 已退出创建流程。")
        return

    # 保存配置（用于补偿）
    try:
        fsm: RedisCommandFSM = get_fsm()
        await fsm.set_kv(
            user_id,
            "channel_batch_config",
            {
                "is_megagroup": is_megagroup,
                "names": names,
                "usernames": usernames,
                "admins": admin_users,
                "admin_perms": admin_perms,
                "bots": bots,
                "bot_perms": bot_perms,
                "only_add_rights": ONLY_ADD_RIGHTS_DEFAULT,
            },
        )
        log_info("批量创建配置已写入 FSM(KV)", extra=get_log_context())
    except Exception:
        log_warning("写入 FSM(KV) 失败", extra=get_log_context())

    n = len(names)
    rr_clients = round_robin_clients(clients, n)
    await event.respond("✅ 配置完成，开始并发创建...")

    sem = asyncio.Semaphore(CREATE_MAX_CONCURRENCY)
    tasks: List[asyncio.Task] = []
    for idx, (name, uname, client) in enumerate(zip(names, usernames, rr_clients), start=1):

        async def run_one(i=idx, nm=name, un=uname, cl=client):
            return await _run_with_semaphore(
                sem,
                lambda: _create_one(
                    cl,
                    is_megagroup=is_megagroup,
                    name=nm.strip(),
                    uname=un.strip(),
                    admin_users=admin_users,
                    admin_perms=admin_perms,
                    bots=bots,
                    bot_perms=bot_perms,
                    event=event,
                    idx=i,
                    trace_id=trace_id,
                ),
            )

        tasks.append(spawn_bg(run_one()))
    await asyncio.gather(*tasks, return_exceptions=True)
    await event.respond("🎉 所有创建与邀请已完成！")


# ───────────────────────── 入口命令 ─────────────────────────


@super_command(trace_action="创建入口", admin_only=True)
async def channel_make_entry(event):
    buttons = [
        [Button.inline("📢 创建频道", data=b"mk_channel"), Button.inline("👥 创建超级群", data=b"mk_group")]
    ]
    markup = event.client.build_reply_markup(buttons, inline_only=True)
    async with event.client.action(event.chat_id, "typing"):
        await event.respond("请选择要创建的类型：", buttons=markup)


@main_router.handler(events.CallbackQuery, data=b"mk_channel")
async def _on_choose_channel(event):
    try:
        await event.answer("创建频道")
    except Exception:
        pass
    try:
        await _batch_flow(event, is_megagroup=False)
    except Exception as e:
        log_exception("入口异常 _on_choose_channel", e, extra={"user_id": getattr(event, "sender_id", None)})
        await event.respond(f"❌ 出错：{e}")


@main_router.handler(events.CallbackQuery, data=b"mk_group")
async def _on_choose_group(event):
    try:
        await event.answer("创建超级群")
    except Exception:
        pass
    try:
        await _batch_flow(event, is_megagroup=True)
    except Exception as e:
        log_exception("入口异常 _on_choose_group", e, extra={"user_id": getattr(event, "sender_id", None)})
        await event.respond(f"❌ 出错：{e}")


# ───────────────────────── 老命令保留 ─────────────────────────


@super_command(trace_action="批量频道创建", admin_only=True)
async def channel_batch_legacy(event):
    await _batch_flow(event, is_megagroup=False)


# ───────────────────────── 修复/补偿（并发） ─────────────────────────


async def _get_input_peer_by_username(client: TelegramClient, username: str):
    """根据 @username 获取已存在的频道/群的 InputPeer（不经错误处理器）。"""
    uname = (username or "").strip().lstrip("@")
    if not uname:
        return None, None

    try:
        full = await client.get_entity(uname)
    except Exception:
        return None, None

    try:
        inp = await get_input_entity_strict(client, full)
        return inp, full
    except Exception:
        return None, None


async def _repair_one(
    client: TelegramClient,
    *,
    uname: str,
    admin_users: List[str],
    admin_perms: Dict[str, bool],
    bots: List[str],
    bot_perms: Dict[str, bool],
    event,
    trace_id: str,
):
    """对单个已存在频道/群执行：邀请管理员、邀请机器人（或设为管理员）、补齐权限。"""
    input_peer, full = await _get_input_peer_by_username(client, uname)
    if not input_peer or not full:
        await event.respond(f"⚠️ 找不到已创建对象：@{uname}，已跳过")
        log_warning("补偿：找不到频道/群", extra={"trace_id": trace_id, "username": uname})
        return False

    is_megagroup = bool(getattr(full, "megagroup", False))
    kind_cn = "超级群" if is_megagroup else "频道"

    try:
        awaits = []
        if admin_users:
            awaits.append(
                invite_with_delay(
                    client,
                    input_peer,
                    admin_users,
                    admin_perms,
                    "管理员",
                    STEP_DELAY_INVITE_SEC,
                    trace_id,
                    is_megagroup=is_megagroup,
                )
            )
        if bots:
            awaits.append(
                invite_with_delay(
                    client,
                    input_peer,
                    bots,
                    bot_perms,
                    "机器人",
                    STEP_DELAY_INVITE_SEC,
                    trace_id,
                    is_megagroup=is_megagroup,
                )
            )
        if awaits:
            await asyncio.gather(*awaits, return_exceptions=True)

        await event.respond(f"🔁 {kind_cn} <b>@{uname}</b> 已完成补偿邀请/赋权。", parse_mode="html")
        log_info("补偿完成", extra={"trace_id": trace_id, "username": uname, "is_megagroup": is_megagroup})
        return True
    except Exception as e:
        log_warning(
            "补偿失败",
            extra={"trace_id": trace_id, "username": uname, "err": f"{type(e).__name__}: {e}"},
        )
        await event.respond(f"❌ 补偿 <b>@{uname}</b> 失败：{e}", parse_mode="html")
        return False


@super_command(trace_action="补偿邀请与权限", admin_only=True)
async def channel_repair_invites(event):
    """
    /fixjq —— 使用上次批量配置，对已创建的频道/群补偿邀请与权限（并发）。
    """
    user_id = event.sender_id
    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "trace_id": trace_id, "func": "channel_repair_invites"})

    fsm: RedisCommandFSM = get_fsm()
    cfg = await fsm.get_kv(user_id, "channel_batch_config")
    if not cfg:
        await event.respond("⚠️ 没找到上一次的批量配置，无法补偿。请先重新执行 /jq。")
        return

    usernames: List[str] = [u.strip().lstrip("@") for u in (cfg.get("usernames") or []) if u and u.strip()]
    admin_users: List[str] = [u.strip().lstrip("@") for u in (cfg.get("admins") or []) if u and u.strip()]
    admin_perms: Dict[str, bool] = dict(cfg.get("admin_perms") or {})
    bots: List[str] = [u.strip().lstrip("@") for u in (cfg.get("bots") or []) if u and u.strip()]
    bot_perms: Dict[str, bool] = dict(cfg.get("bot_perms") or {})

    if not usernames:
        await event.respond("⚠️ 上次配置中没有待补偿的用户名列表。请先执行 /jq。")
        return

    scheduler = get_scheduler()
    validator = getattr(scheduler, "validator", None)

    cm = get_client_manager()
    clients_dict = await cm.prepare_clients_for_user(
        user_id,
        release_existing=False,
        cleanup_journals=True,
        validator=validator,
        set_cooldown=False,
        log_prefix="fixjq",
    )
    if not clients_dict:
        await event.respond("❌ 当前没有可用账号。请先上传/登录。")
        return

    clients_list = list(clients_dict.values())
    rr_clients = round_robin_list(clients_list, len(usernames))

    await event.respond(f"🔁 开始并发补偿邀请与权限（账号数 {len(clients_list)}，目标 {len(usernames)}）…")

    sem = asyncio.Semaphore(REPAIR_MAX_CONCURRENCY)
    tasks = []
    for uname, cl in zip(usernames, rr_clients):

        async def run_fix(un=uname, client=cl):
            return await _run_with_semaphore(
                sem=sem,
                coro_factory=lambda: _repair_one(
                    client=client,
                    uname=un,
                    admin_users=admin_users,
                    admin_perms=admin_perms,
                    bots=bots,
                    bot_perms=bot_perms,
                    event=event,
                    trace_id=trace_id,
                ),
            )

        tasks.append(spawn_bg(run_fix()))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = sum(1 for r in results if r is True)
    fail = len(results) - ok
    await event.respond(f"🎉 补偿流程完成！\n✅ 成功：{ok}\n❌ 失败：{fail}")


# ───────────────────────── 注册命令 ─────────────────────────


def register_commands():
    main_router.command("/jq", trace_name="创建入口", admin_only=True)(channel_make_entry)
    main_router.command("/jf", trace_name="批量频道创建", admin_only=True)(channel_batch_legacy)
    main_router.command("/fixjq", trace_name="补偿邀请与权限", admin_only=True)(channel_repair_invites)
    return True
