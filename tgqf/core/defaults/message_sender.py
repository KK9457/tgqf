# -*- coding: utf-8 -*-
# core/defaults/message_sender.py
from __future__ import annotations

import os
import asyncio
import random
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Sequence, Tuple, Union, TypedDict, List
from telethon.errors import (InviteRequestSentError, RPCError, UserAlreadyParticipantError)
from telethon import TelegramClient, functions, errors as te_errors
from telethon.tl import types as t
from telethon.tl.types import Channel, Chat, User
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    CheckChatInviteRequest,
)
from telethon.tl.functions.channels import JoinChannelRequest
from tg.group_utils import _to_username
from unified.logger import log_debug, log_info, log_warning, log_exception
from unified.trace_context import get_log_context

from typess.link_types import ParsedLink
from typess.message_enums import (
    MessageContent,
    MessageType,
    ensure_entities_objs,  # 兼容保留（新路径主要用 _norm_for_send/_split_for_send）
)
from unified.text_utils import PLACEHOLDER_PATTERN, replace_emoji_placeholders_visible as _replace_vis
from unified.emoji_store import ensure_emoji_store_loaded
from telethon import utils as tg_utils
from core.telethon_errors import (
    classify_send_exception as _classify_exception,
    TE,
)

from tg.group_utils import smart_join
from tg.entity_utils import get_input_peer_for_c, cache_peer, get_cached_peer, get_input_entity_strict

from unified.config import (
    INVITE_COOL_MAX_SECONDS,
    SCHEDULE_MAX_WEEKS_AHEAD,
    FLOOD_AUTOSLEEP_THRESHOLD,
    TG_TIMEOUT as _TG_TIMEOUT_DEFAULT,  # 用于统一发送期超时
)
from typess.fsm_keys import K_BLACKLIST_TARGETS
from core.task_status_reporter import K_PENDING_APPROVAL
from unified.context import get_redis

# ✅ 新 API：统一规范化/分片（已适配 parse_mode + caption 长度）
from tg.telethon_aligned import (
    normalize_for_send as _norm_for_send,
    split_text_for_send as _split_for_send,
)

__all__ = [
    "MessageSender",
]


class MessageSendResult(TypedDict, total=False):
    success: bool
    message_id: Optional[int]
    peer: Any
    type: Optional[str]
    error: Optional[str]
    kind: Optional[str]
    error_code: Optional[str]
    error_meta: Dict[str, Any]


INVITE_COOL_MAX = INVITE_COOL_MAX_SECONDS

# 统一发送期调用超时（复用 TG_TIMEOUT；也可在 config 增设 SEND_OP_TIMEOUT 后切换）
SEND_OP_TIMEOUT = max(5, int(_TG_TIMEOUT_DEFAULT or 15))  # 下限 5s，避免意外设成太小


async def _wait(coro, label: str = "op", timeout: int | None = None):
    """
    统一 wait_for 包装：超时抛 asyncio.TimeoutError（被现有 classify 流程识别为 timeout）；
    取消一律透传；其余异常原样抛出。
    """
    try:
        return await asyncio.wait_for(coro, timeout=(timeout or SEND_OP_TIMEOUT))
    except asyncio.CancelledError:
        # 保持可取消，避免占用并发槽位
        raise
    except asyncio.TimeoutError:
        log_warning(f"{label} 超时", extra=(get_log_context() or {}))
        raise


def resolve_cache_key(pl: ParsedLink) -> str:
    """统一生成缓存键：优先 cache_key()/cache_key → to_link() → username/invite/chat_id。"""
    key = None
    ck = getattr(pl, "cache_key", None)
    if callable(ck):
        try:
            key = ck()
        except Exception:
            key = None
    elif isinstance(ck, str) and ck:
        key = ck
    if not key and hasattr(pl, "to_link"):
        try:
            key = pl.to_link()
        except Exception:
            key = None
    if not key:
        if getattr(pl, "username", None):
            key = f"@{pl.username.lower()}"
        elif getattr(pl, "invite_code", None):
            key = f"invite:{pl.invite_code}"
        elif getattr(pl, "chat_id", None):
            key = f"id:{pl.chat_id}"
    return key or "unknown"


def _safe_redis():
    """兼容新版 unified/context：get_redis 未注入会抛 ContextMissingError。"""
    try:
        return get_redis()
    except Exception:
        return None

# ---------------- 黑名单：永久  可选 TTL（任务级/条目级） ----------------
# 永久集合：K_BLACKLIST_TARGETS.format(uid=self.user_id)
# 任务级集合：<永久集合>:task:<task_id>   （整个集合过期）
# 条目级 Key：<永久集合>:item:<chat_key> （单条目过期）


async def _bl_permanent_key(user_id: int) -> str:
    from typess.fsm_keys import K_BLACKLIST_TARGETS
    return K_BLACKLIST_TARGETS.format(uid=int(user_id))


async def _bl_task_key(user_id: int, task_id: str | None) -> str | None:
    if not task_id:
        return None
    base = await _bl_permanent_key(user_id)
    return f"{base}:task:{task_id}"


async def _bl_item_key(user_id: int, chat_key: str) -> str:
    base = await _bl_permanent_key(user_id)
    return f"{base}:item:{chat_key}"


async def _bl_is_blacklisted(redis, user_id: int, chat_key: str, task_id: str | None) -> bool:
    if not redis or not user_id or not chat_key:
        return False
    perm = await _bl_permanent_key(user_id)
    try:
        if await redis.sismember(perm, chat_key):
            return True
        tkey = await _bl_task_key(user_id, task_id)
        if tkey and await redis.sismember(tkey, chat_key):
            return True
        ikey = await _bl_item_key(user_id, chat_key)
        return bool(await redis.exists(ikey))
    except Exception:
        return False


class _ReadonlyBroadcastError(RuntimeError):
    """公开广播频道（megagroup=False）→ 永久只读。"""
    pass


async def _apply_emoji_fallbacks_for_send(
    client,
    *,
    raw_text: str,
    parse_mode: Optional[str] = None,
    entities: Optional[Sequence[Any]] = None,
    emoji_ids: Optional[Sequence[int]] = None,
) -> str:
    text = raw_text or ""

    # 模板占位符可见化
    if PLACEHOLDER_PATTERN.search(text or ""):
        store = await ensure_emoji_store_loaded(client)
        name_to_char = {}
        if store:
            # 优先使用 name_to_char（如果有）
            name_to_char = getattr(store, "name_to_char", {}) or {}
            # 兜底：用 name_to_id + id_to_char 拼出 name_to_char
            if not name_to_char:
                n2i = getattr(store, "name_to_id", {}) or {}
                i2c = getattr(store, "id_to_char", {}) or {}
                name_to_char = {n: i2c.get(i) for n, i in n2i.items() if i in i2c}
        try:
            text, _ = _replace_vis(text, name_to_char=name_to_char, default_char="•")
        except Exception:
            pass

    # 无实体时，补可见降级后缀
    has_entities = bool(entities) and len(list(entities)) > 0
    if (not has_entities) and emoji_ids:
        store = await ensure_emoji_store_loaded(client)
        ids: List[int] = []
        try:
            ids = [int(i) for i in (emoji_ids or [])]
        except Exception:
            ids = []
        if ids and store and getattr(store, "loaded", False):
            if (parse_mode or "").lower().startswith("html"):
                text = text + store.get_html_fallback_suffix(ids)
            else:
                text = text + store.get_visible_suffix(ids)
    return text


async def _apply_placeholder_visible_only(client, text: str) -> str:
    """仅做 {{emoji}} / {{emoji:😎}} 的可见替换，不追加任何后缀。"""
    if not text or not PLACEHOLDER_PATTERN.search(text):
        return text or ""
    store = await ensure_emoji_store_loaded(client)
    name_to_char = {k: k for k in (getattr(store, "name_to_id", {}) or {}).keys()} if store else {}
    try:
        text, _ = _replace_vis(text, name_to_char=name_to_char, default_char="•")
    except Exception:
        pass
    return text


def _pm_for_send(parse_mode: Optional[str]):
    """
    按项目语义归一化 → 交给 Telethon 的 sanitize_parse_mode
    - auto/auto → None（不解析）
    - 末尾 '+' 的增强模式降级为基础模式
    """
    if not parse_mode:
        return None
    s = str(parse_mode).strip().lower()
    if s in {"auto", "auto+"}:
        return None
    if s.endswith("+"):
        s = s[:-1]
    if s in {"html", "htm"}:
        base = "html"
    elif s in {"md", "markdown"}:
        base = "md"
    elif s in {"mdv2", "markdownv2", "markdown2"}:
        base = "mdv2"
    else:
        return None
    try:
        return tg_utils.sanitize_parse_mode(base)
    except Exception:
        return base


def _extract_username(s: str) -> Optional[str]:
    if not s:
        return None
    # 官方会同时识别 @、t.me/、telegram.me/、telegram.dog/ 以及 tg://join?… 等
    uname, is_invite = tg_utils.parse_username(s.strip())
    return uname  # 只要普通用户名；若要识别“是否invite”，可以同时返回 is_invite


async def _resolve_forward_from_peer(client, f_peer: Any) -> Any:
    try:
        if isinstance(f_peer, (Channel, Chat, User)):
            try:
                return await _wait(client.get_input_entity(f_peer), "get_input_entity(forward_peer)")
            except Exception:
                uname = None
                try:
                    for u in getattr(f_peer, "usernames", []) or []:
                        if getattr(u, "active", True) and getattr(u, "username", None):
                            uname = u.username
                            break
                except Exception:
                    pass
                if not uname:
                    uname = getattr(f_peer, "username", None)
                if uname:
                    return await _wait(client.get_input_entity(uname), "get_input_entity(uname)")
    except Exception:
        pass

    if isinstance(f_peer, str):
        s = f_peer.strip()
        uname = _to_username(s)
        if uname:
            return await _wait(client.get_input_entity(uname), "get_input_entity(uname)")

        m = re.search(r"tg://(?:openmessage\?user_id|user)\?id=(\d+)", s, flags=re.I)
        if m:
            try:
                uid = int(m.group(1))
                return await _wait(client.get_input_entity(uid), "get_input_entity(uid)")
            except Exception:
                pass

        m = re.search(r"(?:https?://)?t\.me/c/(\d+)", s, flags=re.I)
        if m:
            try:
                internal_id = int(m.group(1))
                ent = await _wait(get_input_peer_for_c(client, internal_id), "get_input_peer_for_c")
                if ent:
                    return ent
                chat_id = int(f"-100{internal_id}")
                try:
                    return await _wait(client.get_input_entity(chat_id), "get_input_entity(chat_id)")
                except Exception:
                    pass
            except Exception:
                pass

        if s.startswith("@") or "t.me/" in s:
            try:
                return await _wait(client.get_input_entity(s), "get_input_entity(url_or_uname)")
            except Exception:
                pass

        m = re.search(r"\bid=(-?\d{6,})\b", s)
        if m:
            try:
                cid = int(m.group(1))
                try:
                    ent = await _wait(get_input_peer_for_c(client, cid), "get_input_peer_for_c")
                    if ent:
                        return ent
                except Exception:
                    pass
                return await _wait(client.get_input_entity(cid), "get_input_entity(cid)")
            except Exception:
                pass

    if isinstance(f_peer, int):
        try:
            return await _wait(client.get_input_entity(f_peer), "get_input_entity(int)")
        except Exception:
            pass
    return None


# ============== 入群小助手 ==============
async def _rejoin_public_if_possible(client, username: str) -> Tuple[bool, bool]:
    """
    返回 (joined_or_already, is_megagroup)
    - 广播频道（megagroup=False）→ 抛 _ReadonlyBroadcastError
    """
    full = await _wait(client.get_entity(username), "get_entity(username)")
    if isinstance(full, Channel):
        is_group = bool(getattr(full, "megagroup", False))
        if not is_group:
            raise _ReadonlyBroadcastError("readonly_channel")
        inp = await _wait(client.get_input_entity(username), "get_input_entity(username)")
        try:
            await _wait(client(JoinChannelRequest(inp)), "JoinChannelRequest")
            return True, True
        except UserAlreadyParticipantError:
            return True, True
        except InviteRequestSentError:
            # 需管理员批准
            raise
        except RPCError:
            # 交由外层分类处理
            raise
    return False, False


def _clamp_schedule_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    kk = dict(kwargs or {})
    key = "schedule" if "schedule" in kk else ("schedule_date" if "schedule_date" in kk else None)
    if key:
        max_dt = datetime.utcnow() + timedelta(weeks=SCHEDULE_MAX_WEEKS_AHEAD)
        try:
            dt = kk[key]
            if isinstance(dt, datetime) and dt > max_dt:
                kk[key] = max_dt
        except Exception:
            pass
    return kk


class MessageSender:
    """统一发送器：异常归一 + 自愈/兜底 + Peer缓存/黑名单 + Pending 入列。"""

    def __init__(
        self,
        client: TelegramClient,
        phone: str = "",
        task_id: str | None = None,
        user_id: int | None = None,
        *,
        fsm: Any | None = None,
        reporter: Any | None = None,
        flood: Any | None = None,
    ):
        self.client = client
        self.phone = phone
        self.task_id = task_id
        self.user_id = int(user_id or 0)
        self.fsm = fsm
        self.reporter = reporter
        self.flood = flood

    # ---------------- Redis：邀请检查冷却 & 黑名单 ----------------
    async def _invite_cooldown_left(self, phone: str, code: str) -> int:
        redis = _safe_redis()
        if not redis:
            return 0
        key = f"invite:cool:{phone}:{code}"
        ttl = await redis.ttl(key)
        return max(0, int(ttl or 0))

    async def _set_invite_cooldown(self, phone: str, code: str, seconds: int) -> None:
        redis = _safe_redis()
        if not redis:
            return
        key = f"invite:cool:{phone}:{code}"
        ttl = min(int(seconds * 1.1), INVITE_COOL_MAX)
        await redis.set(key, "1", ex=ttl)

    async def _is_blacklisted(self, key: str) -> bool:
        redis = _safe_redis()
        if not redis:
            return False
        return await _bl_is_blacklisted(redis, int(self.user_id or 0), key, self.task_id)

    async def _add_blacklist(self, key: str, ttl_seconds: int | None = None) -> None:
        redis = _safe_redis()
        if not redis:
            return
        try:
            uid = int(self.user_id or 0)
            if uid <= 0:
                return
            # 带 TTL → 优先写“任务级临时集合”；若无 task_id，则回退“条目级临时 Key”
            if ttl_seconds and int(ttl_seconds) > 0:
                tkey = await _bl_task_key(uid, self.task_id)
                if tkey:
                    await redis.sadd(tkey, key)
                    await redis.expire(tkey, int(ttl_seconds))
                else:
                    ikey = await _bl_item_key(uid, key)
                    await redis.set(ikey, "1", ex=int(ttl_seconds))
                return
            # 否则写永久集合
            perm = await _bl_permanent_key(uid)
            await redis.sadd(perm, key)
        except Exception:
            pass

    async def add_temp_blacklist(self, key: str, ttl_seconds: int) -> None:
        """
        对外可用：加入“临时黑名单”。
        - 若存在 task_id：加入 <perm>:task:<task_id> 并设置集合 TTL；
        - 否则：写入 <perm>:item:<chat_key> 单条目 TTL。
        """
        await self._add_blacklist(key, ttl_seconds=ttl_seconds)

    # ---------------- 实体解析 ----------------
    async def _resolve_entity(self, client: TelegramClient, phone: str, pl: ParsedLink):
        # 公开会话 / 公开消息所在会话
        if pl.is_public() or pl.is_public_msg():
            uname = pl.username
            if uname:
                try:
                    return await _wait(get_input_entity_strict(client, uname), "get_input_entity_strict(uname)")
                except Exception as e:
                    log_warning(f"解析 username 失败：@{uname} -> {e}")

        # /c/<cid>/<mid> / chat_id / userid
        if pl.is_channel_msg() or pl.is_chat_id() or pl.is_userid():
            try:
                pid = pl.peer_id
                if pid is not None:
                    return await _wait(get_input_entity_strict(client, pid), "get_input_entity_strict(peer_id)")
            except Exception as e:
                log_warning(f"解析 peer_id 失败：{pl.short()} -> {e}")

        # 邀请链接
        if pl.is_invite():
            code = str(pl.value).strip()
            if not code:
                raise TE.InviteHashInvalidError(request=None)
            left = await self._invite_cooldown_left(phone, code)
            if left > 0:
                raise TE.FloodWaitError(request=None, seconds=left)
            try:
                res = await _wait(client(CheckChatInviteRequest(hash=code)), "CheckChatInviteRequest")
                # 若未加入则尝试导入
                try:
                    joined = await _wait(client(ImportChatInviteRequest(hash=code)), "ImportChatInviteRequest")
                    return await _wait(get_input_entity_strict(client, joined.chats[0]), "get_input_entity_strict(joined)")
                except te_errors.UserAlreadyParticipantError:
                    return await _wait(get_input_entity_strict(client, res.chat), "get_input_entity_strict(res.chat)")
            except (te_errors.InviteHashExpiredError, te_errors.InviteHashInvalidError) as ie:
                raise ie
            except te_errors.FloodWaitError as fe:
                await self._set_invite_cooldown(phone, code, fe.seconds)
                raise
        raise TE.PeerIdInvalidError()

    # ---------------- 只读频道检测 ----------------
    async def _check_readonly_channel(self, client: TelegramClient, input_peer) -> None:
        try:
            ent = await _wait(client.get_entity(input_peer), "get_entity(input_peer)")
            if isinstance(ent, t.Channel) and not ent.megagroup:
                raise _ReadonlyBroadcastError("readonly_channel")
        except _ReadonlyBroadcastError:
            raise
        except Exception:
            pass

    # ---------------- 发送核心 ----------------
    async def send(
        self,
        *,
        entity: Union[int, str, None] = None,
        msg: Optional[MessageContent] = None,
        pl: Optional[ParsedLink] = None,
        link_preview: bool = False,
        **kwargs: Any,
    ) -> MessageSendResult:
        if msg is None:
            raise ValueError("缺少 msg（MessageContent）")

        redis = _safe_redis()
        key = resolve_cache_key(pl) if pl else (entity if isinstance(entity, str) else str(entity))
        chat_key = resolve_cache_key(pl) if pl else (str(entity) if entity is not None else "unknown")

        if key and await self._is_blacklisted(key):
            log_info("命中黑名单，跳过", extra={"chat_key": chat_key, **(get_log_context() or {})})
            return {
                "success": False,
                "message_id": None,
                "peer": key,
                "type": getattr(msg.type, "value", str(msg.type)),
                "error": "blacklisted",
                "kind": "Blacklisted",
                "error_code": "blacklisted",
                "error_meta": {},
            }

        try:
            entity_resolved = None
            used_cache = False
            if self.user_id and pl:
                try:
                    entity_resolved = await get_cached_peer(pl.cache_key(), self.user_id)
                    used_cache = bool(entity_resolved)
                except Exception:
                    pass

            if entity_resolved is None:
                log_debug("send() 开始解析目标", extra={"entity": entity, "pl": (pl.to_link() if pl else None), "chat_key": chat_key, **(get_log_context() or {})})
                if isinstance(entity, str):
                    uname = _extract_username(entity)
                    if uname:
                        entity = uname
                if pl is not None:
                    entity_resolved = await self._resolve_entity(self.client, self.phone, pl)
                else:
                    if entity is None:
                        raise ValueError("未提供有效的 entity 或 ParsedLink")
                    entity_resolved = await _wait(get_input_entity_strict(self.client, entity), "get_input_entity_strict(entity)")

            if (not used_cache) and self.user_id and pl and entity_resolved is not None:
                try:
                    await cache_peer(pl.cache_key(), entity_resolved, self.user_id)
                except Exception:
                    pass

            await self._check_readonly_channel(self.client, entity_resolved)

            try:
                mid = await self._do_send_once(entity_resolved, msg, link_preview=link_preview, **kwargs)
                log_info("✅ 消息发送成功", extra={"message_id": mid, "peer": repr(entity_resolved), "chat_key": chat_key, "type": getattr(msg.type, "value", str(msg.type)), "task_id": self.task_id, **(get_log_context() or {})})
                return {"success": True, "message_id": mid, "peer": entity_resolved, "type": getattr(msg.type, "value", str(msg.type))}
            except Exception as first:
                code, meta = _classify_exception(first)
                if isinstance(first, ValueError) and ("无法解析源 peer" in str(first) or "unresolvable source peer" in str(first).lower()):
                    code = "from_peer_unresolvable"
                    meta = {"peer": getattr(msg, "forward_peer", None)}
                if isinstance(first, TE.FloodWaitError) or (code == "flood_wait"):
                    seconds = 0
                    try:
                        seconds = int(getattr(first, "seconds", 0) or (meta or {}).get("seconds") or 0)
                    except Exception:
                        seconds = int((meta or {}).get("seconds") or 0)
                    code, meta = "flood_wait", {"seconds": seconds, "source": (meta or {}).get("source") or "unknown"}

                log_warning("首次发送失败", extra={"phone": self.phone, "task_id": self.task_id, "chat_key": chat_key, "error_kind": type(first).__name__, "error_code": code, "error_meta": meta, **(get_log_context() or {})})

                if code == "you_blocked_user":
                    try:
                        from telethon.tl.types import InputPeerUser
                        if hasattr(entity_resolved, "user_id") or isinstance(entity_resolved, InputPeerUser):
                            await _wait(self.client(functions.contacts.UnblockRequest(entity_resolved)), "contacts.UnblockRequest")
                            mid = await self._do_send_once(entity_resolved, msg, link_preview=link_preview, **kwargs)
                            return {"success": True, "message_id": mid, "peer": entity_resolved, "type": getattr(msg.type, "value", str(msg.type))}
                    except Exception:
                        pass

                if code in {"slowmode", "flood_wait"}:
                    seconds = int((meta or {}).get("seconds") or 2)
                    if seconds <= max(1, int(FLOOD_AUTOSLEEP_THRESHOLD)):
                        try:
                            if self.reporter and self.user_id:
                                await self.reporter.notify_flood_wait(self.user_id, self.phone, seconds)
                        except Exception:
                            pass
                        await asyncio.sleep(seconds + random.uniform(0.3, 1.0))
                        try:
                            mid = await self._do_send_once(entity_resolved, msg, link_preview=link_preview, **kwargs)
                            try:
                                if self.reporter and self.user_id:
                                    await self.reporter.notify_recovered(self.user_id, self.phone)
                            except Exception:
                                pass
                            return {"success": True, "message_id": mid, "peer": entity_resolved, "type": getattr(msg.type, "value", str(msg.type))}
                        except Exception as second:
                            first = second
                            code, meta = _classify_exception(second)
                            if isinstance(second, ValueError) and ("无法解析源 peer" in str(second) or "unresolvable source peer" in str(second).lower()):
                                code = "from_peer_unresolvable"
                                meta = {"peer": getattr(msg, "forward_peer", None)}

                if code in {"timeout", "network_dc_error", "random_id_duplicate"}:
                    await asyncio.sleep(2 + random.uniform(0.2, 0.8))
                    try:
                        mid = await self._do_send_once(entity_resolved, msg, link_preview=link_preview, **kwargs)
                        return {"success": True, "message_id": mid, "peer": entity_resolved, "type": getattr(msg.type, "value", str(msg.type))}
                    except Exception as second:
                        first = second
                        code, meta = _classify_exception(second)
                        if isinstance(second, ValueError) and ("无法解析源 peer" in str(second) or "unresolvable source peer" in str(second).lower()):
                            code = "from_peer_unresolvable"
                            meta = {"peer": getattr(msg, "forward_peer", None)}

                need_join = bool(pl) and (code in {"not_in_group", "chat_write_forbidden", "channel_private", "channel_invalid"})
                if need_join and isinstance(pl, ParsedLink):
                    try:
                        if pl.is_public() and (pl.username or ""):
                            _joined, is_group = await _rejoin_public_if_possible(self.client, pl.username)
                            if _joined and is_group:
                                entity_resolved = await _wait(get_input_entity_strict(self.client, pl.username), "get_input_entity_strict(rejoined)")
                                if self.user_id and pl and entity_resolved is not None:
                                    try:
                                        await cache_peer(pl.cache_key(), entity_resolved, self.user_id)
                                    except Exception:
                                        pass
                                mid = await self._do_send_once(entity_resolved, msg, link_preview=link_preview, **kwargs)
                                return {"success": True, "message_id": mid, "peer": entity_resolved, "type": getattr(msg.type, "value", str(msg.type))}
                        else:
                            ok, reason, peer = await smart_join(self.client, pl, skip_check=pl.is_invite())
                            if ok:
                                entity_resolved = peer or await _wait(get_input_entity_strict(self.client, entity or pl.to_link()), "get_input_entity_strict(join)")
                                if self.user_id and pl and entity_resolved is not None:
                                    try:
                                        await cache_peer(pl.cache_key(), entity_resolved, self.user_id)
                                    except Exception:
                                        pass
                                mid = await self._do_send_once(entity_resolved, msg, link_preview=link_preview, **kwargs)
                                return {"success": True, "message_id": mid, "peer": entity_resolved, "type": getattr(msg.type, "value", str(msg.type))}
                            if "等待管理员审批" in (reason or "") or "join" in (reason or "").lower():
                                raise TE.InviteRequestSentError(request=None)
                    except _ReadonlyBroadcastError:
                        if self.user_id and redis and pl:
                            try:
                                await self._add_blacklist(pl.cache_key())  # 永久
                            except Exception:
                                pass
                        return {
                            "success": False,
                            "message_id": None,
                            "peer": pl.to_link() if pl else entity,
                            "type": getattr(msg.type, "value", str(msg.type)),
                            "error": "readonly_channel",
                            "kind": "ReadonlyBroadcast",
                            "error_code": "readonly_channel",
                            "error_meta": {},
                        }
                    except TE.InviteRequestSentError:
                        if self.user_id and redis and pl:
                            try:
                                await redis.sadd(K_PENDING_APPROVAL.format(uid=self.user_id), pl.cache_key())
                            except Exception:
                                pass
                        return {
                            "success": False,
                            "message_id": None,
                            "peer": pl.to_link() if pl else entity,
                            "type": getattr(msg.type, "value", str(msg.type)),
                            "error": "pending_approval",
                            "kind": "InviteRequestSentError",
                            "error_code": "join_request_sent",
                            "error_meta": {},
                        }
                    except Exception as je:
                        j_code, j_meta = _classify_exception(je)
                        log_warning("自动入群/重入失败", extra={"phone": self.phone, "task_id": self.task_id, "chat_key": chat_key, "error_code": j_code, "error_meta": j_meta, **(get_log_context() or {})})

                code, meta = _classify_exception(first)
                if isinstance(first, ValueError) and ("无法解析源 peer" in str(first) or "unresolvable source peer" in str(first).lower()):
                    code = "from_peer_unresolvable"
                    meta = {"peer": getattr(msg, "forward_peer", None)}

                PERM_CODES = {"username_not_occupied", "invite_invalid", "invite_expired", "invite_hash_expired", "channel_private", "readonly_channel"}
                if self.user_id and redis and (code in PERM_CODES) and pl:
                    try:
                        await self._add_blacklist(pl.cache_key())  # 永久
                    except Exception:
                        pass

                log_exception("❌ 发送失败", exc=first, extra={"phone": self.phone, "task_id": self.task_id, "chat_key": chat_key, "error_kind": type(first).__name__, "error_code": code, "error_meta": meta, **(get_log_context() or {})})
                return {
                    "success": False,
                    "message_id": None,
                    "peer": entity if entity else (pl.to_link() if pl else None),
                    "type": getattr(msg.type, "value", str(msg.type)),
                    "error": str(first),
                    "kind": type(first).__name__,
                    "error_code": code,
                    "error_meta": meta,
                }

        except Exception as e:
            code, meta = _classify_exception(e)
            PERM_CODES = {"invite_invalid", "invite_expired", "invite_hash_expired", "channel_private", "readonly_channel", "username_not_occupied"}
            if self.user_id and _safe_redis() and (code in PERM_CODES) and pl:
                try:
                    await self._add_blacklist(pl.cache_key())  # 永久
                except Exception:
                    pass
            log_exception("❌ 发送失败（解析或总体异常）", exc=e, extra={"phone": self.phone, "task_id": self.task_id, "chat_key": chat_key, "error_kind": type(e).__name__, "error_code": code, "error_meta": meta, **(get_log_context() or {})})
            return {
                "success": False,
                "message_id": None,
                "peer": entity if entity else (pl.to_link() if pl else None),
                "type": getattr(msg.type, "value", str(msg.type)) if msg is not None else None,
                "error": str(e),
                "kind": type(e).__name__,
                "error_code": code,
                "error_meta": meta,
            }

    async def _do_send_once(self, dest_peer: Any, mc: MessageContent, *, link_preview: bool = False, **kwargs) -> int:
        client = self.client

        # 统一 parse_mode 字符串（保持项目语义）
        pm_str = getattr(mc, "parse_mode", None)

        # 先做占位可视化，避免后续实体位移
        text_raw = (getattr(mc, "content", None) or getattr(mc, "caption", None) or "")
        text_raw = await _apply_placeholder_visible_only(client, text_raw)

        kwargs = _clamp_schedule_kwargs(kwargs)

        if mc.type == MessageType.TEXT:
            # ✅ 分片发送：split_text_for_send(text, pm, ents, is_caption=False)
            last_mid = 0
            for part_text, part_ents in _split_for_send(
                text_raw,
                pm_str,
                getattr(mc, "entities", None),
                is_caption=False,
            ):
                # 无实体时才补可见降级后缀，并允许 parse_mode 渲染
                if not part_ents:
                    part_text = await _apply_emoji_fallbacks_for_send(
                        client,
                        raw_text=part_text,
                        parse_mode=_pm_for_send(pm_str),
                        entities=None,
                        emoji_ids=getattr(mc, "emoji_ids", None),
                    )
                m = await _wait(
                    client.send_message(
                        dest_peer,
                        part_text,
                        parse_mode=(None if part_ents else _pm_for_send(pm_str)),
                        entities=part_ents,
                        link_preview=link_preview,
                        **kwargs,
                    ),
                    "send_message",
                )
                last_mid = int(getattr(m, "id", 0) or 0)
            return last_mid

        if mc.type in (MessageType.MEDIA, MessageType.ALBUM):
            files_raw: Sequence[Any] = []
            if mc.type == MessageType.MEDIA:
                media = getattr(mc, "media", None)
                files_raw = media if isinstance(media, (list, tuple)) else [media]
            else:
                files_raw = getattr(mc, "media_group", None) or []
            files = [f for f in files_raw if f]

            # ✅ Caption 统一规范化 + 限长 + 实体修正
            cap_text, cap_ents, _cap_pm = _norm_for_send(
                getattr(mc, "caption", None) or "",
                pm_str,
                getattr(mc, "entities", None),
                is_caption=True,
            )
            if not cap_ents:
                cap_text = await _apply_emoji_fallbacks_for_send(
                    client,
                    raw_text=cap_text,
                    parse_mode=_pm_for_send(pm_str),
                    entities=None,
                    emoji_ids=getattr(mc, "emoji_ids", None),
                )

            log_debug(
                "发送文件",
                extra={
                    "files": len(files),
                    "has_entities": bool(cap_ents),
                    "len": len(cap_text or ""),
                    **(get_log_context() or {}),
                },
            )

            send_kwargs = dict(
                caption=cap_text,
                parse_mode=(None if cap_ents else _pm_for_send(pm_str)),
                caption_entities=(cap_ents if cap_ents else None),
                supports_streaming=True,
            )
            send_kwargs.update(kwargs)
            # 不显式传 album 参数，传入 list 交给 Telethon 自动组相册

            m = await _wait(client.send_file(dest_peer, files, **send_kwargs), "send_file")
            if isinstance(m, (list, tuple)):
                last = m[-1] if m else None
                return int(getattr(last, "id", 0) or 0)
            return int(getattr(m, "id", 0) or 0)

        if mc.type == MessageType.FORWARD:
            f_peer = getattr(mc, "forward_peer", None) or getattr(mc, "peer", None)
            f_id = getattr(mc, "forward_id", None) or getattr(mc, "msg_id", None) or getattr(mc, "message_id", None)
            if not (f_peer and f_id):
                raise ValueError("转发缺少 peer/msg_id（或 forward_peer/forward_id）")
            # 统一消息号解析（Message/int/None）
            fid = tg_utils.get_message_id(f_id)
            if fid is None:
                raise ValueError(f"无法解析消息 ID：{f_id!r}")
            f_id = fid

            if isinstance(f_peer, str):
                uname = _extract_username(f_peer)
                if uname:
                    f_peer = uname

            from_peer_resolved = await _resolve_forward_from_peer(client, f_peer)
            if from_peer_resolved is None:
                raise ValueError(f"无法解析源 peer: {f_peer!r}")

            # ✅ 计算 drop_author（优先 forward_as_copy；否则看 forward_show_author）
            drop_author = None
            try:
                if getattr(mc, "forward_as_copy", None) is True:
                    drop_author = True
                elif getattr(mc, "forward_as_copy", None) is False:
                    drop_author = False
                else:
                    # 未显式给 as_copy，则根据 show_author 推断：show_author=False → drop_author=True
                    drop_author = (getattr(mc, "forward_show_author", False) is False)
            except Exception:
                drop_author = None

            send_opts = dict(kwargs) if kwargs else {}
            if drop_author is not None:
                send_opts["drop_author"] = bool(drop_author)

            m = await _wait(
                client.forward_messages(dest_peer, f_id, from_peer=from_peer_resolved, **send_opts),
                "forward_messages",
            )
            mid = int(getattr(m, "id", 0) or 0)

            # 可选评论：默认“回复到”刚转发出去的消息
            comment = (getattr(mc, "content", None) or "").strip()
            if comment:
                ctext, cents, _cpm = _norm_for_send(
                    comment,
                    pm_str,
                    getattr(mc, "entities", None),
                    is_caption=False,
                )
                if not cents:
                    ctext = await _apply_emoji_fallbacks_for_send(
                        client,
                        raw_text=ctext,
                        parse_mode=_pm_for_send(pm_str),
                        entities=None,
                        emoji_ids=getattr(mc, "emoji_ids", None),
                    )
                comment_opts = dict(send_opts)
                comment_opts.pop("drop_author", None)
                if "reply_to" not in comment_opts and mid:
                    comment_opts["reply_to"] = mid
                await _wait(
                    client.send_message(
                        dest_peer,
                        ctext,
                        parse_mode=(None if cents else _pm_for_send(pm_str)),
                        entities=cents,
                        **comment_opts,
                    ),
                    "send_message(comment)",
                )

            return mid

        raise ValueError(f"未支持的消息类型：{mc.type}")
