# -*- coding: utf-8 -*-
# handlers/commands/group_command.py
from __future__ import annotations

from typing import List, Tuple, Optional, Dict
import re
import asyncio

from telethon import events, Button
from telethon.tl import types as t
from telethon.utils import add_surrogate, del_surrogate

from core.decorators import command_safe
from core.defaults.bot_utils import BotUtils
from core.registry_guard import safe_add_event_handler, mark_callbacks_registered
from router.command_router import main_router
from tg.link_utils import standardize_and_dedup_links  # 返回 List[Tuple[storage, ParsedLink]]
from unified.context import get_fsm
from unified.logger import log_exception, log_info, log_debug, log_warning
from unified.trace_context import generate_trace_id, set_log_context, get_log_context
from unified.accounts_ui import send_task_menu
from ui.constants import TASK_SET_GROUPS, ACCOUNTS_OPEN,TASKMENU_OPEN
from core.redis_fsm import RedisCommandFSM

# ===== 本模块私有回调常量 =====
GROUPS_ADD_OPEN = b"groups:add"     # 叠加
GROUPS_SET_OPEN = b"groups:set"     # 覆盖
PATTERN_GROUPS  = rb"^groups:(?:add|set)$"
GROUPS_ENTRY    = b"groups:entry"   # 供 unified/accounts_ui.py 调用

fsm: Optional[RedisCommandFSM] = None

# ===== 进程内会话互斥（确保同一用户唯一会话）=====
# 多进程/多实例部署请改成 Redis 分布式锁
_SESSION_LOCKS: Dict[int, asyncio.Lock] = {}
def _get_user_lock(uid: int) -> asyncio.Lock:
    lock = _SESSION_LOCKS.get(uid)
    if lock is None:
        lock = asyncio.Lock()
        _SESSION_LOCKS[uid] = lock
    return lock

# ===== 文案 =====
ADD_HINT = (
    "⭐️【添加链接】请输入目标群组 【多个用分行隔开】 。\n\n支持 群组链接 / @username / txt文件"

)
SET_HINT = (
    "⭐️【覆盖链接】请输入目标群组 【多个用分行隔开】 。\n\n支持 群组链接 / @username / txt文件"
)

INPUT_TIMEOUT = 600
MAX_TXT_BYTES = 1_000_000  # 1 MB：上传txt文件大小上限

# ===== 工具函数 =====

def _split_lines_keep_nonempty(text: str) -> List[str]:
    return [s.strip() for s in (text or "").splitlines() if s.strip()]

def _html_anchor_to_href(s: str) -> str:
    return re.sub(r'<a[^>]+href="([^"]+)"[^>]*>.*?</a>', r'\1', s, flags=re.I | re.S)

def _uniq_preserve_order(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _extract_candidates_from_message(msg) -> List[str]:
    """
    同时兼容：
    - 文本行（URL / @username / t.me/c/...）
    - 富文本实体：MessageEntityTextUrl / MessageEntityUrl / MessageEntityMention
    - 富文本 anchor：<a href="..."> 文本 </a>
    （.txt 文件另行处理）
    """
    urls: List[str] = []

    try:
        entities = getattr(msg, "entities", []) or []
        raw_text = (getattr(msg, "text", None)
                    or getattr(msg, "raw_text", "")
                    or getattr(msg, "message", "")
                    or "")
        sur = add_surrogate(raw_text)
        for ent in entities:
            if isinstance(ent, t.MessageEntityTextUrl) and getattr(ent, "url", None):
                urls.append(ent.url)
            elif isinstance(ent, t.MessageEntityUrl):
                off = int(getattr(ent, "offset", 0) or 0)
                ln  = int(getattr(ent, "length", 0) or 0)
                if ln > 0 and 0 <= off < len(sur):
                    s = del_surrogate(sur[off:off+ln])
                    if s:
                        urls.append(s)
            elif isinstance(ent, t.MessageEntityMention):
                off = int(getattr(ent, "offset", 0) or 0)
                ln  = int(getattr(ent, "length", 0) or 0)
                if ln > 0 and 0 <= off < len(sur):
                    s = del_surrogate(sur[off:off+ln])
                    if s:
                        urls.append(s)
    except Exception:
        pass

    try:
        raw = (getattr(msg, "text", None)
               or getattr(msg, "raw_text", "")
               or getattr(msg, "message", "")
               or "")
        raw = _html_anchor_to_href(raw)
        urls.extend(_split_lines_keep_nonempty(raw))
    except Exception:
        pass

    return urls

async def _extract_txt_lines(msg) -> List[str]:
    """
    若用户上传 .txt（或 text/*）文件，则读取其内容并按行拆分返回。
    - 支持常见编码：utf-8-sig / utf-16 / gbk / latin-1 / utf-8
    """
    try:
        doc = getattr(msg, "document", None)
        if not doc:
            return []
        try:
            if getattr(doc, "size", 0) and int(doc.size) > MAX_TXT_BYTES:
                log_warning("🧱 文本文件过大，已拒绝", extra={"size": int(doc.size), "limit": MAX_TXT_BYTES})
                return []
        except Exception:
            pass

        filename = ""
        try:
            for attr in getattr(doc, "attributes", []) or []:
                if isinstance(attr, t.DocumentAttributeFilename) and getattr(attr, "file_name", None):
                    filename = attr.file_name or ""
                    break
        except Exception:
            filename = ""

        mime = str(getattr(doc, "mime_type", "") or "").lower()
        is_txt = (filename.lower().endswith(".txt") if filename else False) or mime.startswith("text/")
        if not is_txt:
            return []

        try:
            data = await msg.download_media(bytes)
            size = len(data) if data else 0
            log_debug("📥 收到文本文件", extra={"filename": filename, "mime": mime, "size": size})
        except Exception as e:
            log_exception("读取 txt 文件失败（download_media）", exc=e, extra={"filename": filename, "mime": mime})
            return []

        if not data:
            return []

        candidates = ("utf-8-sig", "utf-16", "gbk", "latin-1", "utf-8")
        text = None
        for enc in candidates:
            try:
                text = data.decode(enc, errors="ignore")
                if text:
                    log_debug("📄 文本文件解码成功", extra={"encoding": enc, "char_len": len(text)})
                    break
            except Exception:
                continue
        if not text:
            log_warning("⚠️ 文本文件解码失败", extra={"filename": filename, "mime": mime})
            return []

        lines = _split_lines_keep_nonempty(text)
        log_info("📄 文本文件解析完成", extra={"filename": filename, "lines": len(lines)})
        return lines
    except Exception as e:
        log_exception("提取 txt 行失败", exc=e)
        return []

def _standardize_valid_storage(lines: List[str]) -> List[str]:
    out: List[str] = []
    bad = 0
    try:
        parsed: List[Tuple[str, object]] = standardize_and_dedup_links(lines)
        for storage, pl in parsed:
            try:
                ok = bool(pl.is_valid()) if hasattr(pl, "is_valid") else True
                if ok:
                    out.append(storage)
                else:
                    bad += 1
            except Exception:
                bad += 1
        log_debug("🔧 标准化完成", extra={"in_lines": len(lines), "valid": len(out), "invalid": bad})
        if bad:
            log_warning("标准化后无效链接数量", extra={"count": bad})
    except Exception as e:
        log_exception("standardize_and_dedup_links 异常", exc=e, extra={"in_lines": len(lines)})
    return out

async def _load_existing_groups(user_id: int) -> List[str]:
    f = fsm or get_fsm()
    if not f:
        log_warning("FSM 未初始化，返回空列表", extra={"user_id": user_id})
        return []
    try:
        task = await f.get_state(user_id)
        lst = list(getattr(task, "group_list", []) or [])
        log_debug("📦 载入已存在链接", extra={"user_id": user_id, "count": len(lst)})
        return lst
    except Exception as e:
        log_exception("载入已存在链接失败", exc=e, extra={"user_id": user_id})
        return []

async def _save_groups(user_id: int, groups: List[str]) -> None:
    f = fsm or get_fsm()
    if not f:
        log_warning("FSM 未初始化，无法保存", extra={"user_id": user_id, "to_save": len(groups)})
        return
    try:
        await f.set_groups(user_id, groups)
        log_info("💾 已保存链接集合", extra={"user_id": user_id, "total": len(groups)})
    except Exception as e:
        log_exception("保存链接集合失败", exc=e, extra={"user_id": user_id, "total": len(groups)})

# ===== 统一入口：/a 命令 & 任务菜单按钮 =====

def register_commands():
    main_router.command("/a", trace_name="设置群组", white_only=True)(_on_a_command)
    log_debug("✅ /a 命令已注册（group_command）")
    return True

@command_safe(white_only=True)
async def _on_a_command(event) -> None:
    user_id = int(getattr(event, "sender_id", 0) or 0)
    set_log_context({"user_id": user_id, "trace_id": generate_trace_id(), "module": __name__, "signal": "/a"})
    log_info("➡️ 进入 /a 模式选择", extra=get_log_context())
    await _open_mode_selector(event)

@command_safe(white_only=True)
async def handle_group_input(event) -> None:
    log_debug("🔁 从任务菜单进入设置群组")
    await _open_mode_selector(event)

async def _open_mode_selector(event) -> None:
    try:
        await event.answer()
    except Exception:
        pass
    text = (
        "⭐️【添加链接】会与前面设置的链接去重后叠加。\n\n"
        "⭐️【覆盖链接】会清空旧链接，仅保留本次提交。"
    )
    btns = [
        [Button.inline("➕ 添加链接", data=GROUPS_ADD_OPEN),
         Button.inline("🧹 覆盖链接", data=GROUPS_SET_OPEN)],
        [Button.inline("⬅️ 返回菜单", data=TASKMENU_OPEN)],
    ]
    # 优先编辑，失败再发送
    try:
        await event.edit(text, buttons=btns, link_preview=False)
    except Exception:
        await BotUtils.safe_respond(event, text, buttons=btns, parse_mode="html", link_preview=False)
    log_debug("🧭 模式选择已展示（edit-first）")

# ===== 两种模式的回调 → 单条有效提交后自动完成 =====

@command_safe(white_only=True)
async def on_groups_open_cb(event) -> None:
    user_id = int(getattr(event, "sender_id", 0) or 0)
    data: bytes = event.data or b""
    mode = "append" if data == GROUPS_ADD_OPEN else "overwrite"

    try:
        await event.answer()
    except Exception:
        pass

    set_log_context({
        "user_id": user_id,
        "trace_id": generate_trace_id(),
        "module": __name__,
        "signal": "groups_open_cb",
        "mode": mode,
    })
    log_info("➡️ 进入群组导入会话", extra=get_log_context())

    # —— 会话唯一性：同一用户仅允许一个进行中的导入会话 —— #
    ulock = _get_user_lock(user_id)
    if ulock.locked():
        log_warning("⛔️ 已有进行中的导入会话，拒绝并提示", extra={"user_id": user_id})
        try:
            await BotUtils.safe_respond(event, "⚠️ 你已在导入模式中，请在当前会话里发送链接或稍候再试。")
        except Exception:
            pass
        return

    await ulock.acquire()
    try:
        hint = ADD_HINT if mode == "append" else SET_HINT
        try:
            await event.edit(hint, buttons=None, link_preview=False)
            log_debug("✏️ 已编辑为会话提示", extra={"mode": mode})
        except Exception:
            await BotUtils.safe_respond(event, hint)
            log_debug("✏️ 会话提示已补发", extra={"mode": mode})

        existing = await _load_existing_groups(user_id)
        if mode == "overwrite":
            log_info("🧹 覆盖模式：清空已有集合", extra={"before": len(existing)})
            existing = []

        cli = event.client
        chat_id = event.chat_id

        async def _apply_and_reply(msg_obj) -> int:
            nonlocal existing

            text_lines = _extract_candidates_from_message(msg_obj)
            file_lines: List[str] = []
            try:
                file_lines = await _extract_txt_lines(msg_obj)
            except Exception as e:
                log_exception("TXT 解析异常", exc=e)

            raw_lines = text_lines + file_lines
            if raw_lines:
                log_debug("📝 收到原始输入", extra={
                    "text_lines": len(text_lines),
                    "file_lines": len(file_lines),
                    "raw_total": len(raw_lines),
                })

            add_storage_raw = _standardize_valid_storage(raw_lines)
            add_storage = _uniq_preserve_order(add_storage_raw)
            this_cnt = len(add_storage)
            de_this = len(add_storage_raw) - this_cnt

            if this_cnt == 0:
                # 不结束会话，继续等下一条
                try:
                    await cli.send_message(chat_id, "（未解析到有效链接，请重新发送）", link_preview=False)
                except Exception:
                    pass
                log_warning("⏭️ 本次无有效链接，继续等待", extra={"raw_total": len(raw_lines)})
                return 0

            prev_total = len(existing)
            merged = _uniq_preserve_order(existing + add_storage)
            added_to_total = len(merged) - prev_total
            dup_against_existing = this_cnt - added_to_total

            log_info("🧮 合并统计", extra={
                "this_valid": this_cnt,
                "dedup_in_batch": de_this,
                "prev_total": prev_total,
                "added_to_total": added_to_total,
                "dup_against_existing": dup_against_existing,
                "new_total": len(merged),
            })

            existing = merged
            await _save_groups(user_id, merged)

            summary = (
                "✅ 链接提交成功\n\n"
                f"提交链接: {this_cnt}\n"
                f"重复链接: {dup_against_existing}\n"
                f"库中链接: {len(existing)}\n"
            )
            try:
                await cli.send_message(chat_id, summary, link_preview=False)
            except Exception:
                pass
            return this_cnt

        # —— 只等待“第一条有效提交”，完成后自动收尾 —— #
        try:
            newmsg_filter = events.NewMessage(incoming=True, chats=chat_id, from_users=user_id)
            async with cli.conversation(chat_id, timeout=INPUT_TIMEOUT, exclusive=True) as conv:
                while True:
                    msg = await conv.wait_event(newmsg_filter)
                    # 排除纯指令/空白（防止误触）
                    txt = (getattr(msg, "text", None) or getattr(msg, "raw_text", "") or "").strip()
                    if txt.lower() in {"done", "/done", "完成", "结束"}:
                        # 用户手动结束也允许
                        log_info("🛑 用户主动结束", extra={"token": txt})
                        break

                    added = await _apply_and_reply(msg)
                    if added > 0:
                        # 首条有效提交已处理，自动结束
                        log_info("✅ 首条有效提交已处理，自动结束会话", extra={"user_id": user_id})
                        break

        except asyncio.TimeoutError:
            try:
                await cli.send_message(chat_id, f"⌛️ 超时退出导入模式。当前累积链接 {len(existing)} 个。", link_preview=False)
            except Exception:
                pass
            log_warning("⌛️ 导入会话超时退出", extra={"user_id": user_id, "total": len(existing)})
        except asyncio.CancelledError:
            log_warning("🧵 导入会话被取消", extra={"user_id": user_id})
        except Exception as e:
            log_exception("等待用户输入异常", exc=e, extra={"user_id": user_id})
        except Exception:
            pass
        await send_task_menu(event, ACCOUNTS_OPEN)
        log_info("✅ 导入会话结束", extra={"user_id": user_id, "final_total": len(existing)})

    finally:
        # 释放用户锁
        try:
            if ulock.locked():
                ulock.release()
        except Exception:
            pass

# ===== 回调注册（由 handlers/registry.py 调用）=====

def register_callbacks(bot) -> bool:
    # 仅当“首次标记成功”才继续注册（防重复）
    if not mark_callbacks_registered(bot, "groups_module_v2"):
        log_debug("⚠️ groups.register_callbacks: 命名空间已存在（groups_module_v2），本次跳过")
        return False

    ok1 = safe_add_event_handler(
        bot, on_groups_open_cb,
        events.CallbackQuery(pattern=PATTERN_GROUPS),
        tag="groups:v2:open"
    )
    # 兼容两种入口 data 值：旧 TASK_SET_GROUPS + 新 GROUPS_ENTRY
    ok2 = safe_add_event_handler(
        bot, handle_group_input,
        events.CallbackQuery(data=TASK_SET_GROUPS),
        tag="groups:v2:entry:legacy"
    )
    ok3 = safe_add_event_handler(
        bot, handle_group_input,
        events.CallbackQuery(data=GROUPS_ENTRY),
        tag="groups:v2:entry"
    )
    log_debug("✅ groups callbacks registered", extra={"ok1": ok1, "ok2": ok2, "ok3": ok3})
    return True

def init_group_command(fsm_instance: RedisCommandFSM):
    global fsm
    fsm = fsm_instance
    log_info("🔌 group_command 已注入 FSM 实例")
