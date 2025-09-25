# -*- coding: utf-8 -*-
# tg/link_utils.py
from __future__ import annotations

from typing import List, Tuple, Optional, Callable, Awaitable
import asyncio
import re
from html import unescape as html_unescape
import inspect

from telethon import TelegramClient, functions
from telethon import errors as TE
from telethon.errors import ChannelPrivateError, InviteHashInvalidError, InviteRequestSentError
from core.telethon_errors import unwrap_error
from unified.context import get_client_manager
from unified.trace_context import get_log_context
from typess.link_types import ParsedLink, LinkType
from tg.entity_utils import get_input_entity_strict, resolve_marked_peer
from unified.logger import log_debug, log_warning, log_exception
from unified.config import MAX_CONCURRENCY
from typess.health_types import HealthState

__all__ = [
    "normalize_link", "key_from_parsed", "standardize_and_dedup_links",
    "parse_group_links", "parse_links_with_interval", "check_single_link_availability",
    "check_links_availability", "mark_username_not_occupied",
]

# ---------- 内部：清洗相关正则 ----------
_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_TME_RE = re.compile(r"(https?://)?t\.me/(https?://)?t\.me/", re.I)
_ZERO_WIDTH = (
    "\u200b", "\u200c", "\u200d", "\ufeff", "\u2060",
    "\u202a", "\u202b", "\u202c", "\u202d", "\u202e",
    "\u2066", "\u2067", "\u2068", "\u2069"
)

def _extract_href_or_text(raw: str) -> str:
    """提取 href 内容或清除 HTML 标签"""
    match = _HREF_RE.search(raw or "")
    return match.group(1) if match else _TAG_RE.sub("", raw or "")

def _strip_tme_prefix(s: str) -> str:
    """修复 t.me//username 类异常链接"""
    return s.replace("://t.me//", "://t.me/")

def _sanitize_raw_link(raw: str) -> str:
    """强力清洗原始文本成标准化链接"""
    s = html_unescape((raw or "").strip())
    s = _extract_href_or_text(s)

    # 去空白与控制字符
    for ch in (" ", "\t", "\r", "\n", " ", *(_ZERO_WIDTH or ())):
        s = s.replace(ch, "")

    # 处理 @username → username
    if s.startswith("@") and len(s) > 1:
        s = s[1:]

    # 统一修正链接
    s = s.replace("telegram.me/", "t.me/").replace("telegram.dog/", "t.me/")
    s = _MULTI_TME_RE.sub("t.me/", s).replace("http://", "https://")
    s = s.replace("t.me/s/", "t.me/")

    # 若以 t.me/ 开头但没协议，补 https://
    if s.startswith("t.me/"):
        s = "https://" + s

    return _strip_tme_prefix(s)

async def _redis_setex_compat(redis, key: str, seconds: int, value: str) -> None:
    """兼容 redis.asyncio.Redis（需要 await）与同步 Redis 客户端。"""
    if not redis:
        return
    try:
        res = redis.setex(key, seconds, value)
        if inspect.isawaitable(res):
            await res
    except Exception:
        pass

def _is_placeholder_link(s: str) -> bool:
    ss = (s or "").strip().lower()
    return ss in {"-", "t.me/-", "https://t.me/-", "@-"}



# ---------- 解析与规范化 ----------
def normalize_link(raw: str) -> ParsedLink:
    """强力清洗并使用 ParsedLink 解析"""
    s = _sanitize_raw_link(raw)
    log_debug("开始解析链接", extra={"raw": raw, "sanitized": s, **(get_log_context() or {})})

    # 🚫 丢弃占位/无效 “-” 链接，防止进入后续分配/统计
    if _is_placeholder_link(s):
        raise ValueError("【清洗链接】 检测到占位符 '-' ")

    try:
        return ParsedLink.parse(s)
    except Exception as e:
        log_exception("❌【清洗链接】 解析链接失败", exc=e, extra={"sanitized": s, **(get_log_context() or {})})
        raise ValueError(f"【清洗链接】 无法解析链接格式: {s} ({type(e).__name__}: {e})") from e

# 兼容层：对外仍提供 key_from_parsed，但内部统一使用 ParsedLink.cache_key() 作为强 key
def key_from_parsed(pl: ParsedLink) -> str:
    """用于历史兼容：返回短 key（不含类型前缀）"""
    return pl.short()

def _strong_key(pl: ParsedLink) -> str:
    """内部使用的强 key，带类型前缀，避免跨类型碰撞"""
    return pl.cache_key()

def standardize_and_dedup_links(raw_lines: List[str]) -> List[Tuple[str, ParsedLink]]:
    """标准化并去重原始链接（按强 key 去重）"""
    seen_keys = set()
    results: List[Tuple[str, ParsedLink]] = []

    for raw in raw_lines:
        try:
            pl = normalize_link(raw)
            if not pl.is_valid():
                continue
            key = _strong_key(pl)
            if key not in seen_keys:
                seen_keys.add(key)
                results.append((raw, pl))
        except Exception:
            continue
    return results

async def parse_group_links(group_list: List[str]) -> List[ParsedLink]:
    """快速批量解析（无节流）"""
    return [pl for _, pl in standardize_and_dedup_links(group_list)]

async def parse_links_with_interval(group_list: List[str], interval: float = 0.5) -> List[ParsedLink]:
    """批量解析链接，并按间隔执行以节流"""
    links = []
    for _, pl in standardize_and_dedup_links(group_list):
        links.append(pl)
        await asyncio.sleep(interval)
    return links

def _mask_invite(code: str) -> str:
    return code[:3] + "***" + code[-3:] if len(code) > 6 else "***"

# ---------- 可用性预检 ----------
async def mark_username_not_occupied(
    redis,
    user_id: Optional[int],
    uname: Optional[str],
    e: Exception,
    link: ParsedLink,
    on_username_invalid: Optional[Callable[[str], Awaitable[None]]] = None
) -> str:
    """标记用户名为未被占用并缓存"""
    if not uname:
        return "Username missing"
    await _redis_setex_compat(redis, f"user:{user_id}:tg:username_not_occupied:{uname}", 24 * 3600, "1")
    if on_username_invalid:
        try:
            await on_username_invalid(uname)
        except Exception:
            pass
    log_warning("❌无效用户名", extra={"username": uname, "reason": str(e), "link": link.to_link()})
    return f"UsernameNotOccupied: {uname}"

async def check_single_link_availability(
    client: TelegramClient,
    link: ParsedLink,
    *,
    redis=None,
    user_id: Optional[int] = None,
    # 可选：若你要在这里直接把 client 标成 AUTH_EXPIRED，就把这两项传进来
    client_manager=None,
    phone: Optional[str] = None,
    skip_invite_check: bool = False,
    on_username_invalid: Optional[Callable[[str], Awaitable[None]]] = None
) -> Tuple[bool, Optional[str]]:
    """检查单个链接的可用性（确保任何路径都有返回，防止 None 解包错误）"""
    try:
        # 保障：client 已连接且 get_me 正常（防止未授权 client 直接跑解析）
        if not client.is_connected():
            await client.connect()
        # 可选小超时，避免卡住
        try:
            await asyncio.wait_for(client.get_me(), timeout=5)
        except asyncio.TimeoutError:
            return False, "client_timeout"
        # 邀请链接
        if link.type == LinkType.INVITE and not skip_invite_check:
            invite_hash = str(link.value)
            await client(functions.messages.CheckChatInviteRequest(invite_hash))
            log_debug("✅【检查链接】有效邀请链接", extra={"code": _mask_invite(invite_hash), "user_id": user_id, **(get_log_context() or {})})
            return True, None

        # 公开会话
        if link.type == LinkType.PUBLIC:
            uname = link.username or ""
            await get_input_entity_strict(client, uname)
            log_debug("✅【检查链接】有效公开链接", extra={"username": uname, "user_id": user_id, **(get_log_context() or {})})
            return True, None

        # 公开消息所属会话
        if link.type == LinkType.PUBLIC_MSG:
            uname, _mid = link.value  # noqa: F841
            await get_input_entity_strict(client, str(uname))
            log_debug("✅【检查链接】有效消息链接", extra={"username": uname, "user_id": user_id, **(get_log_context() or {})})
            return True, None

        # /c/<id>/<mid> 所属会话
        if link.type == LinkType.CHANNEL_MSG:
            cid, _mid = link.value  # noqa: F841
            peer_id = int(f"-100{cid}")
            await get_input_entity_strict(client, peer_id)
            log_debug("✅【检查链接】有效 chat ID", extra={"link": link.to_link(), "user_id": user_id, **(get_log_context() or {})})
            return True, None

        # 数字 ID
        if link.type == LinkType.USERID:
            try:
                val = int(link.value)
                peer_or_id = resolve_marked_peer(val) if str(val).startswith("-") else val
                await get_input_entity_strict(client, peer_or_id)
                log_debug("✅【检查链接】有效 chat ID", extra={"id": str(val), "user_id": user_id, **(get_log_context() or {})})
                return True, None
            except Exception:
                log_debug("ℹ️【检查链接】未知 chat ID", extra={"id": str(link.value), "user_id": user_id, **(get_log_context() or {})})
                return True, None

        # chat_id（-100 前缀或纯 chat_id）
        if link.type == LinkType.CHAT_ID:
            try:
                val = int(link.value)
                # 允许传 -100xxxx 或 xxxx，交给严格解析处理
                peer = val if str(val).startswith("-100") else int(f"-100{val}")
                await get_input_entity_strict(client, peer)
                log_debug("✅【检查链接】有效 chat ID", extra={"chat_id": str(val), "user_id": user_id, **(get_log_context() or {})})
                return True, None
            except Exception as e:
                log_warning("ℹ️【检查链接】解析失败", extra={"chat_id": str(link.value), "err": str(e), "user_id": user_id, **(get_log_context() or {})})
                return True, None

        # 未识别类型：保守放行
        log_debug("ℹ️【检查链接】无法识别，保守放行", extra={"type": link.type.value, "user_id": user_id, **(get_log_context() or {})})
        return True, None

    except (TE.FloodWaitError, InviteHashInvalidError, ChannelPrivateError, InviteRequestSentError) as e:
        log_warning("❌ 【检查链接】不可用链接", extra={"link": link.to_link(), "reason": str(e), "user_id": user_id, **(get_log_context() or {})})

        return False, f"{type(e).__name__}: {e}"
    except Exception as e:
        e0 = unwrap_error(e)
        # 未授权 / 密钥未注册 / 会话重复使用等 → 提示上层更换 client
        if isinstance(e0, (TE.AuthKeyUnregisteredError, TE.AuthKeyDuplicatedError)):
            # 可选：若传入 client_manager & phone，可在此直接标记 AUTH_EXPIRED
            if client_manager and user_id is not None and phone is not None:
                try:
                    client_manager.set_health_state(user_id, phone, HealthState.AUTH_EXPIRED)
                except Exception:
                    pass
            log_warning("❌【检查链接】异常账号：密钥失效",
                        extra={"err": type(e0).__name__, "link": link.to_link(), **(get_log_context() or {})})
            return False, f"{type(e0).__name__}"
        # 其它：记录并回传通用错误码，减少爆栈
        log_exception(" 【检查链接】异常错误", exc=e0,
                      extra={"link": link.to_link(), **(get_log_context() or {})})
        return False, f"{type(e0).__name__}"
    
async def check_links_availability(
    client: TelegramClient,
    groups: List[ParsedLink],
    *,
    max_concurrency: int = MAX_CONCURRENCY,
    skip_invite_check: bool = True,
    redis=None,
    user_id: Optional[int] = None,
    on_username_invalid: Optional[Callable[[str], Awaitable[None]]] = None
) -> Tuple[List[ParsedLink], List[Tuple[ParsedLink, str]]]:
    """并发检查多个链接的可用性（fail-open 不在此处，失败直接计入 invalid）"""
    client_manager = get_client_manager()
    # 唯一化（强 key）
    seen = set()
    uniq: List[ParsedLink] = []
    for pl in groups:
        if not isinstance(pl, ParsedLink):
            continue
        k = _strong_key(pl)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(pl)

    if not uniq:
        return [], []

    sem = asyncio.Semaphore(max(1, int(max_concurrency or 5)))
    results: List[Tuple[ParsedLink, bool, Optional[str]]] = []

    async def worker(pl: ParsedLink) -> None:
        async with sem:
            ok, reason = await check_single_link_availability(
                client, pl,
                redis=redis,
                user_id=user_id,
                client_manager=client_manager,   # 可选：若在类方法里
                phone=getattr(client, "phone", None), # 可选
                skip_invite_check=skip_invite_check,
                on_username_invalid=on_username_invalid
            )
            # 始终三元组，避免 None 解包
            results.append((pl, bool(ok), (None if ok else (reason or "unknown"))))

    tasks = [asyncio.create_task(worker(pl)) for pl in uniq]
    await asyncio.gather(*tasks)

    valid = [pl for pl, ok, _ in results if ok]
    invalid = [(pl, reason or "unknown") for pl, ok, reason in results if not ok]

    log_debug(
        "🔎 【并发检查链接】完成",
        extra={
            "input": len(groups),
            "unique": len(uniq),
            "valid": len(valid),
            "invalid": len(invalid),
            **(get_log_context() or {})
        },
    )
    return valid, invalid
