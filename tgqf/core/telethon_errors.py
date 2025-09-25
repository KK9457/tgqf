# -*- coding: utf-8 -*-
# core/telethon_errors.py
from __future__ import annotations

import re
from dataclasses import dataclass
from types import SimpleNamespace
from typing import (
    Any, Dict, Tuple, Optional, Union, Iterable,
    Callable, Awaitable, TYPE_CHECKING, Protocol, runtime_checkable
)
from enum import Enum

from telethon import errors as te, functions
from telethon.errors import common as common
from dataclasses import field
from typess.health_types import HealthState
from typess.ttl_types import TTL
from core.event_bus import bus

if TYPE_CHECKING:
    from scheduler.ratelimit import FloodController
    from core.redis_fsm import RedisCommandFSM

# === 黑名单集合 Key & 统一上下文 Redis 入口 ===
from typess.fsm_keys import K_BLACKLIST_TARGETS
from unified.context import get_redis


# ---------------------------------------------------------------------------
# Unified RPC call entry
# ---------------------------------------------------------------------------
async def call(
    client,
    req: Union[Any, Iterable[Any]],
    *,
    ordered: bool = False,
    no_updates: bool = False,
    flood_threshold: int | None = None,
):
    if no_updates:
        if isinstance(req, list):
            req = [functions.InvokeWithoutUpdatesRequest(r) for r in req]
        else:
            req = functions.InvokeWithoutUpdatesRequest(req)

    kwargs: Dict[str, Any] = {}
    if flood_threshold is not None:
        kwargs["flood_sleep_threshold"] = int(flood_threshold)

    return await client(req, ordered=ordered, **kwargs)


# ---------------------------------------------------------------------------
# 1) Centralized error class resolver (namespace `TE`)
# ---------------------------------------------------------------------------
class _SentinelError(Exception):
    pass


def _resolve_err(name: str):
    try:
        cls = getattr(te, name)
        if isinstance(cls, type):
            return cls
    except Exception:
        pass
    try:
        rpc = getattr(te, "rpcerrorlist", None)
        if rpc is not None:
            cls = getattr(rpc, name, None)
            if isinstance(cls, type):
                return cls
    except Exception:
        pass
    return _SentinelError


TE = SimpleNamespace(
    SlowModeWaitError=_resolve_err("SlowModeWaitError"),
    UsernameInvalidError=_resolve_err("UsernameInvalidError"),
    UsernameNotOccupiedError=_resolve_err("UsernameNotOccupiedError"),
    UnauthorizedError=_resolve_err("UnauthorizedError"),
    AuthKeyUnregisteredError=_resolve_err("AuthKeyUnregisteredError"),
    FloodPremiumWaitError=_resolve_err("FloodPremiumWaitError"),
    FloodTestPhoneWaitError=_resolve_err("FloodTestPhoneWaitError"),
    ChatWriteForbiddenError=_resolve_err("ChatWriteForbiddenError"),
    UserBannedInChannelError=_resolve_err("UserBannedInChannelError"),
    InviteHashInvalidError=_resolve_err("InviteHashInvalidError"),
    InviteHashExpiredError=_resolve_err("InviteHashExpiredError"),
    InviteRequestSentError=_resolve_err("InviteRequestSentError"),
    UserAlreadyParticipantError=_resolve_err("UserAlreadyParticipantError"),
    FloodWaitError=_resolve_err("FloodWaitError"),
    FloodError=_resolve_err("FloodError"),
    PeerIdInvalidError=_resolve_err("PeerIdInvalidError"),
    ChannelPrivateError=_resolve_err("ChannelPrivateError"),
    ChannelInvalidError=_resolve_err("ChannelInvalidError"),
    PeerFloodError=_resolve_err("PeerFloodError"),
    ChannelsTooMuchError=_resolve_err("ChannelsTooMuchError"),
    ChatAdminRequiredError=_resolve_err("ChatAdminRequiredError"),
    GroupedMediaInvalidError=_resolve_err("GroupedMediaInvalidError"),
    MediaEmptyError=_resolve_err("MediaEmptyError"),
    MessageIdInvalidError=_resolve_err("MessageIdInvalidError"),
    MessageIdsEmptyError=_resolve_err("MessageIdsEmptyError"),
    RandomIdInvalidError=_resolve_err("RandomIdInvalidError"),
    RandomIdDuplicateError=_resolve_err("RandomIdDuplicateError"),
    ScheduleDateTooLateError=_resolve_err("ScheduleDateTooLateError"),
    ScheduleTooMuchError=_resolve_err("ScheduleTooMuchError"),
    QuizAnswerMissingError=_resolve_err("QuizAnswerMissingError"),
    TopicDeletedError=_resolve_err("TopicDeletedError"),
    InputUserDeactivatedError=_resolve_err("InputUserDeactivatedError"),
    UserDeactivatedBanError=_resolve_err("UserDeactivatedBanError"),
    ChatRestrictedError=_resolve_err("ChatRestrictedError"),
    ChatForbiddenError=_resolve_err("ChatForbiddenError"),
    FilePartsInvalidError=_resolve_err("FilePartsInvalidError"),
    PhotoInvalidDimensionsError=_resolve_err("PhotoInvalidDimensionsError"),
    SessionRevokedError=_resolve_err("SessionRevokedError"),
    AuthKeyDuplicatedError=_resolve_err("AuthKeyDuplicatedError"),
    RPCError=getattr(te, "RPCError", _SentinelError),
    ServerError=getattr(te, "ServerError", _SentinelError),
)

# ---------------------------------------------------------------------------
# 2) Utilities
# ---------------------------------------------------------------------------
def detect_flood_seconds(e: BaseException) -> Optional[int]:
    if isinstance(e, (TE.FloodWaitError, TE.FloodPremiumWaitError, TE.FloodTestPhoneWaitError)):
        return int(getattr(e, "seconds", 0) or 0)
    if isinstance(e, TE.SlowModeWaitError):
        return int(getattr(e, "seconds", 0) or 0)
    return None


def _is_auth_expired(e: BaseException) -> bool:
    return isinstance(e, (
        TE.AuthKeyUnregisteredError,
        TE.SessionRevokedError,
        TE.UnauthorizedError,
        TE.AuthKeyDuplicatedError,
    ))


def _is_media_invalid(e: BaseException) -> bool:
    return isinstance(e, (TE.MediaEmptyError, TE.FilePartsInvalidError, TE.PhotoInvalidDimensionsError))


def _err_has(e: Exception, *needles: str) -> bool:
    s = (str(e) or "").lower()
    cname = e.__class__.__name__.lower()
    return any(n.lower() in s or n.lower() in cname for n in needles)


def _is_server_side_retryable(e: BaseException) -> bool:
    code = getattr(e, "code", None)
    if isinstance(code, int) and code >= 500:
        return True
    # 更稳的 ServerError 判断，避免在缺失 ServerError 类时把所有 RPCError 当作 5xx
    _ServerError = getattr(te, "ServerError", None)
    if _ServerError and isinstance(e, _ServerError):
        return True
    if isinstance(e, common.TimeoutError):
        return True
    return False


class ErrorKind(Enum):
    OK = "ok"
    RETRY = "retry"
    FLOOD_WAIT = "flood_wait"
    SLOWMODE = "slowmode_wait"
    PEER_FLOOD = "peer_flood"
    PEER_INVALID = "peer_invalid"
    NOT_JOINED = "not_joined"
    WRITE_FORBIDDEN = "write_forbidden"
    CHANNEL_PRIVATE = "channel_private"
    BANNED_IN_CHANNEL = "banned_in_channel"
    USERNAME_INVALID = "username_invalid"
    USERNAME_NOT_OCCUPIED = "username_not_occupied"
    AUTH_EXPIRED = "auth_expired"
    DEACTIVATED = "deactivated"
    MESSAGE_TOO_LONG = "message_too_long"
    MEDIA_INVALID = "media_invalid"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ErrorDecision:
    kind: ErrorKind
    seconds: Optional[int] = None
    blacklist: bool = False
    retry: bool = False
    reason: str = ""

def unwrap_error(e: BaseException) -> BaseException:
    """
    统一异常解包：
    - Trio/AnyIO 的 MultiError: 有 .exceptions(list/tuple)
    - asyncio/py 错误链: __cause__ / __context__
    取第一个“最有信息量”的叶子异常返回。
    """
    # MultiError 风格
    ex = getattr(e, "exceptions", None)
    if isinstance(ex, (list, tuple)) and ex:
        return unwrap_error(ex[0])
    # 错误链
    if getattr(e, "__cause__", None):
        return unwrap_error(e.__cause__)  # type: ignore[attr-defined]
    if getattr(e, "__context__", None):
        return unwrap_error(e.__context__)  # type: ignore[attr-defined]
    return e

def classify_error_decision(e: BaseException) -> ErrorDecision:
    e = unwrap_error(e)
    if isinstance(e, TE.FloodWaitError):
        sec = int(getattr(e, "seconds", 0) or 0)
        return ErrorDecision(kind=ErrorKind.FLOOD_WAIT, seconds=sec, retry=True, reason=f"FloodWait {sec}s")
    # Premium/TestPhone 也携带 seconds → 走 FLOOD_WAIT
    if isinstance(e, (TE.FloodPremiumWaitError, TE.FloodTestPhoneWaitError)):
        sec = int(getattr(e, "seconds", 0) or 0)
        return ErrorDecision(kind=ErrorKind.FLOOD_WAIT, seconds=sec, retry=True, reason=f"FloodWait {sec}s (variant)")
    if isinstance(e, TE.SlowModeWaitError):
        sec = int(getattr(e, "seconds", 0) or 0)
        return ErrorDecision(kind=ErrorKind.SLOWMODE, seconds=sec, retry=True, reason=f"SlowMode {sec}s")
    if isinstance(e, TE.PeerFloodError):
        # 对话级限流（无秒数）→ 标记为 LIMITED，让上游降级/换号
        return ErrorDecision(kind=ErrorKind.PEER_FLOOD, retry=True, reason="Peer flood")

    if isinstance(e, TE.ChatWriteForbiddenError):
        return ErrorDecision(kind=ErrorKind.WRITE_FORBIDDEN, blacklist=True, retry=False, reason="Chat write forbidden")
    if isinstance(e, TE.UserBannedInChannelError):
        return ErrorDecision(kind=ErrorKind.BANNED_IN_CHANNEL, blacklist=True, retry=False, reason="User banned in channel")
    if isinstance(e, TE.ChannelPrivateError):
        return ErrorDecision(kind=ErrorKind.CHANNEL_PRIVATE, blacklist=True, retry=False, reason="Channel is private")
    if isinstance(e, TE.ChatAdminRequiredError):
        return ErrorDecision(kind=ErrorKind.WRITE_FORBIDDEN, blacklist=True, retry=False, reason="Admin required")

    if isinstance(e, TE.UsernameInvalidError):
        return ErrorDecision(kind=ErrorKind.USERNAME_INVALID, blacklist=True, retry=False, reason="Username invalid")
    if isinstance(e, TE.UsernameNotOccupiedError):
        return ErrorDecision(kind=ErrorKind.USERNAME_NOT_OCCUPIED, blacklist=True, retry=False, reason="Username not occupied")
    if isinstance(e, TE.PeerIdInvalidError):
        return ErrorDecision(kind=ErrorKind.PEER_INVALID, retry=True, reason="Peer id invalid (need re-resolve)")
    if isinstance(e, TE.ChannelInvalidError):
        return ErrorDecision(kind=ErrorKind.PEER_INVALID, retry=True, reason="Channel invalid (need re-resolve)")

    if _is_auth_expired(e):
        return ErrorDecision(kind=ErrorKind.AUTH_EXPIRED, blacklist=False, retry=False, reason="Authorization expired/session revoked")
    if isinstance(e, (TE.InputUserDeactivatedError, TE.UserDeactivatedBanError)):
        return ErrorDecision(kind=ErrorKind.DEACTIVATED, blacklist=True, retry=False, reason="Account deactivated/banned")

    if isinstance(e, getattr(te, "MessageTooLongError", _SentinelError)):
        return ErrorDecision(kind=ErrorKind.MESSAGE_TOO_LONG, blacklist=False, retry=False, reason="Message too long")
    if _is_media_invalid(e):
        return ErrorDecision(kind=ErrorKind.MEDIA_INVALID, blacklist=False, retry=False, reason="Media invalid")

    if isinstance(e, TE.ChatForbiddenError):
        return ErrorDecision(kind=ErrorKind.NOT_JOINED, blacklist=False, retry=False, reason="Not joined or forbidden")

    if _is_server_side_retryable(e):
        return ErrorDecision(kind=ErrorKind.RETRY, blacklist=False, retry=True, reason=f"Retryable RPC/Server error ({e.__class__.__name__})")

    return ErrorDecision(kind=ErrorKind.UNKNOWN, blacklist=False, retry=True, reason=f"Unknown error: {e.__class__.__name__}")


def _decision_to_health(decision: ErrorDecision) -> HealthState:
    k = decision.kind
    if k in (ErrorKind.FLOOD_WAIT,):
        return HealthState.FLOOD_WAIT
    if k in (ErrorKind.SLOWMODE, ErrorKind.PEER_FLOOD, ErrorKind.WRITE_FORBIDDEN, ErrorKind.BANNED_IN_CHANNEL,
             ErrorKind.CHANNEL_PRIVATE, ErrorKind.PEER_INVALID, ErrorKind.NOT_JOINED,
             ErrorKind.MESSAGE_TOO_LONG, ErrorKind.MEDIA_INVALID, ErrorKind.PERMANENT,
             ErrorKind.USERNAME_INVALID, ErrorKind.USERNAME_NOT_OCCUPIED):
        return HealthState.LIMITED
    if k in (ErrorKind.AUTH_EXPIRED, ErrorKind.DEACTIVATED):
        return HealthState.AUTH_EXPIRED if k == ErrorKind.AUTH_EXPIRED else HealthState.BANNED
    if k in (ErrorKind.RETRY,):
        return HealthState.NETWORK_ERROR
    return HealthState.UNKNOWN


def classify_telethon_error(e: BaseException) -> Tuple[HealthState, Dict[str, Any]]:
    e0 = unwrap_error(e)
    d = classify_error_decision(e0)
    return _decision_to_health(d), {
        "kind": d.kind.value,
        "seconds": d.seconds,
        "retry": d.retry,
        "blacklist": d.blacklist,
        "reason": d.reason,
    }


# ---------------------------------------------------------------------------
# 4) Send-stage classification → (error_code, meta)
# ---------------------------------------------------------------------------
def classify_send_exception(e: Exception) -> Tuple[Optional[str], Dict[str, Any]]:
    e0 = unwrap_error(e)
    s_up = (str(e0) or "").upper()

    if "CHAT_SEND_WEBPAGE_FORBIDDEN" in s_up:
        return "chat_send_webpage_forbidden", {}
    if "CHAT_SEND_VIDEOS_FORBIDDEN" in s_up:
        return "chat_send_videos_forbidden", {}
    if "CHAT_SEND_PLAIN_FORBIDDEN" in s_up or "SEND_PLAIN_FORBIDDEN" in s_up:
        return "chat_send_plain_forbidden", {}

    if "FILE_REFERENCE_EXPIRED" in s_up:
        return "file_reference_expired", {}
    if "BOTGROUPS_BLOCKED" in s_up:
        return "bot_groups_blocked", {}

    if "BROADCASTPUBLICVOTERSFORBIDDEN" in s_up:
        return "poll_public_voters_forbidden", {}

    if isinstance(e0, TE.ChannelInvalidError) or "CHANNELINVALID" in s_up:
        return "channel_invalid", {}
    if isinstance(e, TE.ChatAdminRequiredError) or "CHATADMINREQUIRED" in s_up:
        return "admin_required", {}
    if "CHATIDINVALID" in s_up:
        return "chat_id_invalid", {}

    if (
        isinstance(e, TE.InviteRequestSentError)
        or "INVITE_REQUEST_SENT" in s_up
        or "INVITEREQUESTSENT" in s_up
        or "REQUESTED TO JOIN" in s_up
        or "等待管理员审批" in s_up
    ):
        return "join_request_sent", {}

    if isinstance(e, TE.ChannelsTooMuchError) or "CHANNELS_TOO_MUCH" in s_up or "CHANNELSTOOMUCH" in s_up:
        return "channels_too_much", {}
    if isinstance(e, TE.PeerFloodError) or "PEER_FLOOD" in s_up:
        return "peer_flood", {}

    if isinstance(e0, (TE.FloodWaitError, TE.FloodPremiumWaitError, TE.FloodTestPhoneWaitError)) or "FLOOD_WAIT" in s_up:
        seconds = getattr(e0, "seconds", None)
        return "flood_wait", {"seconds": seconds}
    if isinstance(e, TE.FloodError) and ("FLOOD" in s_up) and ("FLOOD_WAIT" not in s_up):
        return "flood", {}

    if isinstance(e0, TE.SlowModeWaitError) or "SLOWMODE" in s_up or "SLOW MODE" in s_up or ("WAIT OF" in s_up and "FLOOD_WAIT" not in s_up):
        seconds = getattr(e0, "seconds", None)
        if not seconds:
            m = re.search(r"WAIT OF (\d+)", s_up)
            if m:
                try:
                    seconds = int(m.group(1))
                except Exception:
                    seconds = None
        return "slowmode", {"seconds": seconds}

    if "ALLOW_PAYMENT_REQUIRED" in s_up or "PAYMENT" in s_up:
        return "payment_required", {}
    if isinstance(e, TE.UsernameNotOccupiedError) or "USERNAME_NOT_OCCUPIED" in s_up or 'NO USER HAS "' in s_up:
        m = re.search(r'NO USER HAS "([^"]+)" AS USERNAME', s_up)
        username = m.group(1) if m else None
        return "username_not_occupied", {"username": username}

    if isinstance(e0, TE.ChatWriteForbiddenError) or "CHATWRITEFORBIDDEN" in s_up or "YOU CAN'T WRITE" in s_up:
        return "chat_write_forbidden", {}
    if _err_has(e, "readonly_channel", "read only channel", "read-only channel"):
        return "readonly_channel", {}

    if isinstance(e0, TE.ChannelPrivateError) or "CHANNEL_PRIVATE" in s_up:
        return "channel_private", {}
    if isinstance(e, TE.PeerIdInvalidError) or "PEER_ID_INVALID" in s_up:
        return "peer_id_invalid", {}
    if isinstance(e, TE.MessageIdInvalidError) or "MESSAGE_ID_INVALID" in s_up:
        return "message_id_invalid", {}
    if isinstance(e, TE.MessageIdsEmptyError) or "MESSAGE_IDS_EMPTY" in s_up:
        return "message_ids_empty", {}
    if isinstance(e, TE.MediaEmptyError) or "MEDIA_EMPTY" in s_up:
        return "media_empty", {}
    if isinstance(e, TE.GroupedMediaInvalidError) or "GROUPED_MEDIA_INVALID" in s_up:
        return "grouped_media_invalid", {}
    if isinstance(e, TE.ScheduleDateTooLateError) or "SCHEDULE_DATE_TOO_LATE" in s_up:
        return "schedule_date_too_late", {}
    if isinstance(e, TE.ScheduleTooMuchError) or "SCHEDULE_TOO_MUCH" in s_up:
        return "schedule_too_much", {}
    if isinstance(e, TE.TopicDeletedError) or "TOPIC_DELETED" in s_up:
        return "topic_deleted", {}
    if isinstance(e, TE.QuizAnswerMissingError) or "QUIZ_ANSWER_MISSING" in s_up:
        return "quiz_answer_missing", {}

    if _err_has(e, "TIMEOUT"):
        return "timeout", {}
    if _err_has(e, "NETWORK", "INVALIDDC", "DISCONNECT", "MIGRATE"):
        return "network_dc_error", {}

    if "CHATSENDGIFSFORBIDDEN" in s_up:
        return "chat_send_gifs_forbidden", {}
    if "CHATSENDMEDIAFORBIDDEN" in s_up:
        return "chat_send_media_forbidden", {}
    if "CHATSENDSTICKERSFORBIDDEN" in s_up:
        return "chat_send_stickers_forbidden", {}

    if isinstance(e0, TE.UserBannedInChannelError) or "BANNED_IN_CHANNEL" in s_up:
        return "banned_in_channel", {}

    if isinstance(e0, TE.InviteHashExpiredError) or ("INVITE" in s_up and "EXPIRED" in s_up):
        return "invite_expired", {}
    if isinstance(e0, TE.InviteHashInvalidError) or "INVITE_HASH_INVALID" in s_up or "INVITE_INVALID" in s_up:
        return "invite_invalid", {}

    if _err_has(e, "无法解析源 peer", "unresolvable source peer", "unresolvable peer", "from_peer_unresolvable"):
        peer = None
        m = re.search(r"源\s*peer:\s*'([^']+)'", str(e))
        if not m:
            m = re.search(r"SOURCE\s*PEER[:\s]*'([^']+)'", str(e), flags=re.IGNORECASE)
        if not m:
            m = re.search(r"\bPEER[:\s]*'([^']+)'", str(e), flags=re.IGNORECASE)
        if m:
            peer = m.group(1)
        return "from_peer_unresolvable", ({"peer": peer} if peer else {})

    if "CANNOT FIND ANY ENTITY CORRESPONDING" in s_up or "GET_INPUT_ENTITY" in s_up:
        return "from_peer_unresolvable", {}
    if "CANNOT GET ENTITY FROM A CHANNEL (OR GROUP) THAT YOU ARE NOT PART OF" in s_up:
        return "not_in_group", {}

    if "INTERDCCALLERROR" in s_up or "COMMUNICATING WITH DC" in s_up:
        return "network_dc_error", {}
    if "PTSCHANGEEMPTY" in s_up:
        return "pts_change_empty", {}
    if "HISTORY_GET_FAILED" in s_up:
        return "history_get_failed", {}
    if "CANCELLEDERROR" in s_up:
        return "timeout", {}

    if "INPUTUSERDEACTIVATED" in s_up:
        return "user_deleted", {}
    if "USERISBLOCKED" in s_up:
        return "user_is_blocked", {}
    if "USERISBOT" in s_up:
        return "user_is_bot", {}
    if "YOUBLOCKEDUSER" in s_up:
        return "you_blocked_user", {}

    if "AUTHBYTESINVALID" in s_up:
        return "auth_invalid", {}
    if "AUTHKEYDUPLICATED" in s_up:
        return "session_conflict", {}
    if "CDNMETHODINVALID" in s_up:
        return "cdn_method_invalid", {}
    if "CONNECTIONAPIIDINVALID" in s_up:
        return "api_id_invalid", {}
    if "CONNECTIONDEVICEMODELEMPTY" in s_up:
        return "device_model_empty", {}
    if "CONNECTIONLANGPACKINVALID" in s_up:
        return "lang_pack_invalid", {}
    if "CONNECTIONNOTINITED" in s_up:
        return "connection_not_inited", {}
    if "CONNECTIONSYSTEMEMPTY" in s_up:
        return "system_empty", {}
    if "INPUTLAYERINVALID" in s_up:
        return "input_layer_invalid", {}
    if "NEEDMEMBERINVALID" in s_up:
        return "need_member_invalid", {}

    if _err_has(e, "join request sent", "等待管理员审批"):
        return "join_request_sent", {}

    return None, {}


# ---------------------------------------------------------------------------
# 5) Error → Strategy handlers（含 Reassigner 抽象）
# ---------------------------------------------------------------------------
ActionFn = Callable[["StrategyContext", Dict[str, Any]], Awaitable[None]]

@runtime_checkable
class Reassigner(Protocol):
    async def reassign(
        self,
        user_id: int,
        chat_key: str,
        *,
        phone: str = "",
        reason: str = "",
        error_code: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> bool: ...

    async def mark_rejoin(
        self,
        user_id: int,
        chat_key: str,
        *,
        phone: str = "",
        reason: str = "",
    ) -> bool: ...


@dataclass
class StrategyContext:
    fsm: "RedisCommandFSM"
    flood: "FloodController"
    user_id: int
    phone: str
    chat_key: str
    reporter: Optional[Any] = None
    scheduler: Optional[Any] = None
    reassigner: Optional[Reassigner] = None


async def _maybe_call_reporter(ctx: StrategyContext, method: str, *args) -> tuple[bool, Optional[bool]]:
    rep = getattr(ctx, "reporter", None)
    if not rep:
        return False, None
    fn = getattr(rep, method, None)
    if not callable(fn):
        return False, None
    res = fn(*args)
    if hasattr(res, "__await__"):
        res = await res
    return True, (bool(res) if isinstance(res, bool) else None)


async def _try_fsm(ctx: StrategyContext, *method_names: str, **kwargs) -> bool:
    fsm = getattr(ctx, "fsm", None)
    if not fsm:
        return False
    for name in method_names:
        fn = getattr(fsm, name, None)
        if callable(fn):
            try:
                res = fn(ctx.user_id, ctx.chat_key, **kwargs)
                if hasattr(res, "__await__"):
                    await res
                return True
            except Exception:
                continue
    return False


async def _add_to_blacklist(ctx: StrategyContext, ttl_seconds: int | None = None) -> bool:  
    ok = False
    try:
        redis = get_redis()
        if redis and ctx.chat_key:
            base = K_BLACKLIST_TARGETS.format(uid=ctx.user_id)
            # 若指定 TTL：
            if ttl_seconds and int(ttl_seconds) > 0:
                # 优先任务级集合（若 StrategyContext 提供 task_id）
                task_id = getattr(ctx, "task_id", None)
                if task_id:
                    tkey = f"{base}:task:{task_id}"
                    await redis.sadd(tkey, ctx.chat_key)
                    await redis.expire(tkey, int(ttl_seconds))
                    ok = True
                else:
                    # 回退到“条目级临时 Key”（单个目标过期）
                    ikey = f"{base}:item:{ctx.chat_key}"
                    await redis.set(ikey, "1", ex=int(ttl_seconds))
                    ok = True
            else:
                # 默认永久
                await redis.sadd(base, ctx.chat_key)
                ok = True
    except Exception:
        pass
    await _maybe_call_reporter(ctx, "mark_blacklisted", ctx.user_id, ctx.chat_key)
    return ok


async def _fallback_mark_reassign(ctx: StrategyContext, reason: str = "") -> bool:
    ok, _ = await _maybe_call_reporter(ctx, "mark_reassign_pending", ctx.user_id, ctx.chat_key or reason or "-")
    if not ok:
        await bus.dispatch("REASSIGN_PENDING", {
            "type": "REASSIGN_PENDING",
            "user_id": ctx.user_id,
            "phone": ctx.phone,
            "ref": ctx.chat_key or reason or "-",
        })
    return ok


async def _fallback_mark_rejoin(ctx: StrategyContext, reason: str = "") -> bool:
    ok, _ = await _maybe_call_reporter(ctx, "mark_rejoin_pending", ctx.user_id, ctx.chat_key or reason or "-")
    if not ok:
        await bus.dispatch("REJOIN_PENDING", {
            "type": "REJOIN_PENDING",
            "user_id": ctx.user_id,
            "phone": ctx.phone,
            "ref": ctx.chat_key or reason or "-",
        })
    return ok


async def _do_reassign(ctx: StrategyContext, reason: str, *, error_code: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> bool:
    r = ctx.reassigner
    handled = False
    if r and isinstance(r, Reassigner):
        try:
            handled = await r.reassign(
                ctx.user_id, ctx.chat_key,
                phone=ctx.phone, reason=reason,
                error_code=error_code, meta=meta or {}
            )
        except Exception:
            handled = False
    if not handled:
        await _fallback_mark_reassign(ctx, reason)
    return handled


async def _do_mark_rejoin(ctx: StrategyContext, reason: str) -> bool:
    r = ctx.reassigner
    handled = False
    if r and isinstance(r, Reassigner):
        try:
            handled = await r.mark_rejoin(ctx.user_id, ctx.chat_key, phone=ctx.phone, reason=reason)
        except Exception:
            handled = False
    if not handled:
        await _fallback_mark_rejoin(ctx, reason)
    return handled


# ---- 具体策略原子动作 ----
async def _act_flood_wait(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    secs = int(meta.get("seconds") or 0)
    if secs > 0:
        ctx.flood.on_floodwait(ctx.phone, secs)
        out_meta["cooldown_phone"] = secs

async def _act_peer_flood(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    ctx.flood.on_peer_flood(ctx.phone, ctx.chat_key, hours=TTL.PEER_FLOOD_COOLDOWN_HOURS)
    out_meta["peer_flood_hours"] = TTL.PEER_FLOOD_COOLDOWN_HOURS

async def _act_slowmode(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    secs = int(meta.get("seconds") or 0) or TTL.SLOWMODE_DEFAULT_SECONDS
    await ctx.fsm.set_slowmode(ctx.user_id, ctx.chat_key, secs, phone=ctx.phone)
    ctx.flood.on_slowmode(ctx.chat_key, secs)
    out_meta["slowmode_seconds"] = secs

async def _act_payment_required(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    await ctx.fsm.mark_payment_required(ctx.user_id, ctx.chat_key)
    out_meta["did_mark_payment_required"] = True

async def _act_write_forbidden(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    await ctx.fsm.mark_write_forbidden(ctx.user_id, ctx.chat_key, hours=TTL.WRITE_FORBIDDEN_HOURS)
    out_meta["write_forbidden_hours"] = TTL.WRITE_FORBIDDEN_HOURS

async def _act_banned_in_channel(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    await ctx.fsm.mark_banned_in_channel(ctx.user_id, ctx.chat_key, hours=TTL.BANNED_IN_CHANNEL_HOURS)
    out_meta["banned_hours"] = TTL.BANNED_IN_CHANNEL_HOURS

async def _act_channels_too_much(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    ctx.flood.on_channels_too_much(ctx.phone, hours=TTL.CHANNELS_TOO_MUCH_HOURS)
    out_meta["channels_too_much_hours"] = TTL.CHANNELS_TOO_MUCH_HOURS

async def _act_username_not_occupied(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    username = (meta or {}).get("username")
    if username:
        await ctx.fsm.mark_username_invalid(username)
        out_meta["username"] = username

async def _act_permanent_invalid_and_blacklist(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    await _try_fsm(
        ctx,
        "mark_link_invalid",
        "mark_target_invalid",
        "mark_group_invalid",
    )
    out_meta["did_blacklist"] = await _add_to_blacklist(ctx)

async def _act_need_reassign(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    out_meta["did_reassign"] = await _do_reassign(ctx, "need_reassign", error_code="need_reassign", meta=meta)

async def _act_need_rejoin_and_reassign(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    out_meta["did_mark_rejoin"] = await _do_mark_rejoin(ctx, "need_rejoin")
    out_meta["did_reassign"] = await _do_reassign(ctx, "need_reassign", error_code="not_in_group", meta=meta)

async def _act_join_request_sent(ctx: StrategyContext, meta: Dict[str, Any], out_meta: Dict[str, Any]) -> None:
    ok, _ = await _maybe_call_reporter(ctx, "mark_approval_pending", ctx.user_id, ctx.chat_key or "-")
    if not ok:
        await bus.dispatch("APPROVAL_PENDING", {
            "type": "APPROVAL_PENDING",
            "user_id": ctx.user_id,
            "phone": ctx.phone,
            "ref": ctx.chat_key or "-",
        })
    out_meta["did_mark_approval_pending"] = True


HANDLERS: Dict[str, Callable[[StrategyContext, Dict[str, Any], Dict[str, Any]], Awaitable[None]]] = {
    "flood_wait": _act_flood_wait,
    "peer_flood": _act_peer_flood,
    "slowmode": _act_slowmode,
    "channels_too_much": _act_channels_too_much,

    "chat_write_forbidden": _act_write_forbidden,
    "banned_in_channel": _act_banned_in_channel,
    "payment_required": _act_payment_required,
    "username_not_occupied": _act_username_not_occupied,

    "invite_expired": _act_permanent_invalid_and_blacklist,
    "invite_invalid": _act_permanent_invalid_and_blacklist,
    "channel_private": _act_permanent_invalid_and_blacklist,
    "readonly_channel": _act_permanent_invalid_and_blacklist,

    "admin_required": _act_need_reassign,
    "channel_invalid": _act_need_reassign,
    "from_peer_unresolvable": _act_need_reassign,
    "chat_id_invalid": _act_need_reassign,
    "not_in_group": _act_need_rejoin_and_reassign,
    "join_request_sent": _act_join_request_sent,
}


@dataclass
class StrategyAction:
    attr: str = "unknown"
    cooldown_chat: Optional[int] = None
    cooldown_phone: Optional[int] = None
    blacklist: Optional[bool] = None
    reassign: Optional[bool] = None
    meta: Dict[str, Any] = field(default_factory=dict)  # noqa: ANN401


async def classify_attr(code: Optional[str], *, link_kind: str = "") -> str:
    c = (code or "").lower()
    PERM = {
        "readonly_channel", "admin_required", "banned_in_channel", "channel_private",
        "username_not_occupied", "invite_invalid", "invite_expired", "chat_id_invalid", "channel_invalid",
        "media_empty", "message_id_invalid", "message_ids_empty",
        "poll_public_voters_forbidden", "quiz_answer_missing",
        "user_deleted", "user_is_bot", "user_is_blocked", "payment_required",
    }
    PENDING = {"join_request_sent", "schedule_too_much", "channels_too_much"}
    TRANS = {"slowmode", "flood_wait", "peer_flood", "network_dc_error", "timeout", "random_id_duplicate",
             "from_peer_unresolvable", "peer_id_invalid", "not_in_group", "schedule_date_too_late"}
    if c in PERM:
        return "readonly" if c == "readonly_channel" else "permanent"
    if c in PENDING:
        return "pending"
    if c in TRANS:
        return "transient"
    if c == "chat_write_forbidden" and link_kind == "public":
        return "readonly"
    return "unknown"


async def handle_send_error(code: str, meta: dict, ctx: StrategyContext):
    try:
        seconds = int((meta or {}).get("seconds", 0) or 0)
    except Exception:
        seconds = 0

    link_kind = (meta or {}).get("link_kind") or ""
    action = StrategyAction(attr=await classify_attr(code, link_kind=link_kind), meta=dict(meta or {}))

    if code in {"flood_wait", "slowmode"} and seconds > 0:
        action.cooldown_chat = seconds
        if code == "flood_wait":
            action.cooldown_phone = seconds

    if code in {"username_not_occupied", "invite_invalid", "invite_expired", "channel_invalid", "chat_id_invalid"}:
        action.blacklist = True

    if code in {"channel_private", "chat_write_forbidden", "banned_in_channel"}:
        action.reassign = True

    did: Dict[str, Any] = {}
    handler = HANDLERS.get(code)
    if handler:
        try:
            await handler(ctx, meta or {}, did)
        except Exception:
            pass

    action.meta.update(did)
    attr = action.attr or "unknown"
    action.meta.setdefault("ok_like", attr in {"pending", "transient", "unknown"})

    return action.__dict__


__all__ = [
    "call",
    "TE",
    "detect_flood_seconds",
    "classify_telethon_error",
    "classify_error_decision",
    "classify_send_exception",
    "Reassigner",
    "StrategyContext",
    "handle_send_error",
]
