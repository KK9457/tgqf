# -*- coding: utf-8 -*-
# tg/group_utils.py 

from __future__ import annotations
from typing import Tuple, Optional, Any
import re, time
from unified.logger import log_debug, log_info, log_warning, log_error, log_exception
from unified.trace_context import inject_trace_context
from telethon import TelegramClient,utils
from telethon.errors import ( 
    InviteHashInvalidError, InviteHashExpiredError, UserAlreadyParticipantError,
    InviteRequestSentError, UsernameInvalidError, UsernameNotOccupiedError,
    FloodWaitError, FloodError, UserNotParticipantError, ChannelsTooMuchError,
    PeerIdInvalidError, ChannelPrivateError, ChatAdminRequiredError
)
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import GetParticipantRequest as ChannelsGetParticipant, JoinChannelRequest
from telethon.tl.types import Channel, Chat, User
from typess.link_types import ParsedLink
from typess.join_status import JoinResult, JoinCode
from tg.link_utils import _sanitize_raw_link
from tg.entity_utils import (
    get_input_entity_strict, resolve_marked_peer, ensure_entity_seen,
    get_input_peer_for_c, classify_channel_flags, get_full_channel_safe, channel_has_paid_constraints
)

__all__ = [
    "_to_username",
    "check_membership",
    "smart_join",
]

# ---------------- 小工具 ----------------

def _to_username(value: str) -> Optional[str]:
    """
    使用官方 utils.parse_username 做统一解析；遇到 join 邀请时返回 None。
    """
    v = _sanitize_raw_link(value)
    v = re.sub(r"^(https?://)?t\.me/", "", v, flags=re.I).lstrip("@")
    uname, is_invite = utils.parse_username(v)
    return None if is_invite else (uname or "").lower()

async def _get_entity_by_type(client, pl: ParsedLink):
    if pl.is_invite():
        raise ValueError(f"[group_utils] 不应对 INVITE 类型调用 get_entity: {pl}")
    try:
        if pl.is_public():
            uname = pl.username or _to_username(str(pl.value))
            if uname:
                return await client.get_input_entity(uname)
            log_error(f"❌无效目标: {pl.value}")
            return None
        if pl.is_public_msg():
            return await client.get_input_entity(pl.username)
        if pl.is_channel_msg():
            cid, _ = pl.value  # type: ignore
            return await get_input_peer_for_c(client, int(cid))
        if pl.is_userid():
            val = int(pl.value)
            peer_or_id = resolve_marked_peer(val) if str(val).startswith("-") else val
            return await get_input_entity_strict(client, peer_or_id)
    except UsernameNotOccupiedError:
        log_warning(f"❌ 无效用户名：{pl}")
        return None
    except Exception as e:
        log_warning(f"⚠️ 获取实体解析失败: {pl} -> {e}")
    return None




# ---------------- 功能函数 ----------------

async def check_membership(client, pl: ParsedLink) -> Tuple[bool, str]:
    try:
        if pl.is_invite():
            return False, "未加入（邀请链接）"
        entity_like = await _get_entity_by_type(client, pl)
        if entity_like is None:
            await ensure_entity_seen(client)
            entity_like = await _get_entity_by_type(client, pl)
            if entity_like is None:
                return False, "未知"

        full = await client.get_entity(entity_like)
        if isinstance(full, User):
            return False, "对象类型为用户"

        inp_channel = await client.get_input_entity(full)
        me_inp = await client.get_input_entity((await client.get_me()).id)

        if isinstance(full, Channel):
            try:
                await client(ChannelsGetParticipant(inp_channel, me_inp))
                return True, "✅ 已在超级群中"
            except UserNotParticipantError:
                return False, "未加入"
            except ChatAdminRequiredError:
                return False, "未加入"

        if isinstance(full, Chat):
            return True, "✅ 已在普通群中"

        return False, "未知类型"
    except Exception as e:
        return False, f"检查失败: {e}"
    
async def ensure_join(client: TelegramClient, pl: ParsedLink, peer_cache: Optional[Any] = None) -> JoinResult:
    inject_trace_context("ensure_join", phase="join")
    # 1) 预判是否已在群（不产生冷却）
    try:
        already, reason = await check_membership(client, pl)
        if already:
            return JoinResult(code=JoinCode.ALREADY, meta={"attempted_join": False, "reason": reason})
    except Exception as e:
        # 预检查失败不阻断，进入尝试阶段
        log_warning("【预检查】失败，跳过", extra={"err": str(e), "plink": pl.short()})

    # 2) 根据类型触发 join/import（会产生节奏）
    try:
        if pl.is_invite():
            ok, msg, payload = await _join_via_invite(client, pl)
            if ok:
                return JoinResult(code=JoinCode.OK, meta={"attempted_join": True, "peer": payload, "reason": msg})
            # 邀请失败分支（审批/无效等）
            lower = (msg or "").lower()
            if "等待管理员审批" in lower or "申请已发送" in lower:
                return JoinResult(code=JoinCode.NEED_APPROVE, meta={"attempted_join": True, "reason": msg})
            if "无效" in lower or "过期" in lower:
                return JoinResult(code=JoinCode.INVALID, meta={"attempted_join": True, "reason": msg})
            if "私有" in lower or "不可见" in lower:
                return JoinResult(code=JoinCode.FORBIDDEN, meta={"attempted_join": True, "reason": msg})
            if "flood" in lower or "限流" in lower:
                # 无明确秒数时，code 给 FLOOD，无 seconds
                return JoinResult(code=JoinCode.FLOOD, meta={"attempted_join": True, "reason": msg})
            return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": True, "reason": msg})

        if pl.is_public() or pl.is_public_msg():
            ok, msg, payload = await _join_public_group(client, pl)
            if ok:
                # 公开群：成功或“已加入”都会走 ok=True，这里统归 OK（ALREADY 已在预检阶段返回）
                return JoinResult(code=JoinCode.OK, meta={"attempted_join": True, "peer": payload, "reason": msg})
            lower = (msg or "").lower()
            if "审批" in lower:
                return JoinResult(code=JoinCode.NEED_APPROVE, meta={"attempted_join": True, "reason": msg})
            if "无效" in lower or "非法" in lower:
                return JoinResult(code=JoinCode.INVALID, meta={"attempted_join": True, "reason": msg})
            if "私有" in lower or "不可见" in lower:
                return JoinResult(code=JoinCode.FORBIDDEN, meta={"attempted_join": True, "reason": msg})
            if "限流" in lower:
                return JoinResult(code=JoinCode.FLOOD, meta={"attempted_join": True, "reason": msg})
            if "过多" in lower:
                return JoinResult(code=JoinCode.FORBIDDEN, meta={"attempted_join": True, "reason": msg})
            return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": True, "reason": msg})

        if pl.is_channel_msg():
            ok, msg, payload = await _access_channel_msg(client, pl)
            # 访问消息会话不触发“加入”，但按“尝试访问”视为 attempted_join=True 进行节奏控制更保守
            if ok:
                return JoinResult(code=JoinCode.OK, meta={"attempted_join": True, "peer": payload, "reason": msg})
            return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": True, "reason": msg})

        if pl.is_userid():
            ok, msg, payload = await _resolve_user_id(client, pl)
            # 解析ID不执行加入，本质不应产生节奏；这里 attempted_join=False
            if ok:
                return JoinResult(code=JoinCode.OK, meta={"attempted_join": False, "peer": payload, "reason": msg})
            return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": False, "reason": msg})

        return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": False, "reason": "unsupported link type"})

    except FloodWaitError as e:
        return JoinResult(code=JoinCode.FLOOD, meta={"attempted_join": True, "seconds": int(getattr(e, "seconds", 0) or 0), "reason": "flood"})
    except InviteRequestSentError:
        return JoinResult(code=JoinCode.NEED_APPROVE, meta={"attempted_join": True, "reason": "waiting approval"})
    except (InviteHashInvalidError, InviteHashExpiredError, UsernameInvalidError, UsernameNotOccupiedError, PeerIdInvalidError):
        return JoinResult(code=JoinCode.INVALID, meta={"attempted_join": True, "reason": "invalid"})
    except ChannelPrivateError:
        return JoinResult(code=JoinCode.FORBIDDEN, meta={"attempted_join": True, "reason": "private"})
    except ChannelsTooMuchError:
        return JoinResult(code=JoinCode.FORBIDDEN, meta={"attempted_join": True, "reason": "channels too much"})
    except Exception as e:
        log_exception("【预检查】 异常错误", e, extra={"plink": pl.short()})
        return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": True, "reason": str(e)})


async def smart_join(
    client,
    pl: ParsedLink,
    skip_check: bool = False
) -> Tuple[bool, str, Optional[Any]]:
    inject_trace_context("smart_join", phase="join")
    t0 = time.perf_counter()
    log_debug("【智能加群】检查是否已加入目标群组入口", extra={
        "type": pl.type.value, "short": pl.short(), "skip_check": skip_check
    })
    try:
        if hasattr(pl, "is_valid") and not pl.is_valid():
            log_warning("【智能加群】无效链接，跳过", extra={"plink": pl.to_link(), "type": pl.type.value})
            return False, "跳过：无效链接/用户名", {"code": "skip_invalid_link"}

        log_debug("【智能加群】检查是否已加入目标群组开始", extra={"plink": pl.to_link(), "plink_type": pl.type.value, "skip_check": skip_check})

        if not skip_check:
            try:
                joined, reason = await check_membership(client, pl)
                log_info("【智能加群】检查是否已加入目标群组完成",
                         extra={"joined": joined, "reason": reason, "elapsed_ms": int((time.perf_counter()-t0)*1000)})
                if joined:
                    return True, (reason or "already in"), None
            except Exception as e:
                log_warning("【智能加群】: 检查是否已加入目标群组失败，跳过", extra={"error": str(e)})

        branch_start = time.perf_counter()
        if pl.is_invite():
            ok, msg, payload = await _join_via_invite(client, pl)
            log_info("【智能加群】通过邀请链接", extra={"ok": ok, "reason": msg, "elapsed_ms": int((time.perf_counter()-branch_start)*1000)})
            return ok, msg, payload

        if pl.is_public() or pl.is_public_msg():
            if (pl.username or "") == "me":
                log_info("【智能加群】目标是自己，跳过", extra={"username": "me"})
                return False, "目标为 'me'（自身），不需要加入", None
            ok, msg, payload = await _join_public_group(client, pl)
            log_info("【智能加群】通过公开链接", extra={"ok": ok, "reason": msg, "elapsed_ms": int((time.perf_counter()-branch_start)*1000)})
            return ok, msg, payload

        if pl.is_channel_msg():
            ok, msg, payload = await _access_channel_msg(client, pl)
            log_info("【智能加群】通过消息链接", extra={"ok": ok, "reason": msg, "elapsed_ms": int((time.perf_counter()-branch_start)*1000)})
            return ok, msg, payload

        if pl.is_userid():
            ok, msg, payload = await _resolve_user_id(client, pl)
            log_info("【智能加群】通过用户ID", extra={"ok": ok, "reason": msg, "elapsed_ms": int((time.perf_counter()-branch_start)*1000)})
            return ok, msg, payload

        log_warning("【智能加群】❌不支持的类型", extra={"type": pl.type.value})
        return False, f"❌ 暂不支持该类型 {pl.type}", None

    except Exception as e:
        log_exception("【智能加群】❌异常错误", e, extra={
            "type": pl.type.value, "short": pl.short()
        })
        raise
    finally:
        log_info("【智能加群】结束", extra={
            "type": pl.type.value, "short": pl.short(),
            "elapsed_ms": int((time.perf_counter() - t0) * 1000)
        })

# ============== 公开群/频道加入与访问 ==============
async def _join_via_invite(client, pl: ParsedLink):
    try:
        code = str(pl.value).strip()
        if not code:
            return False, "❌ 无效邀请链接", None

        res_checked = None
        try:
            res_checked = await client(CheckChatInviteRequest(code))
        except InviteRequestSentError:
            return False, "⏳ 已申请进群，等待管理员批准", None
        except FloodWaitError as e:
            return False, f"🚫 Flood 限流，请等待 {e.seconds} 秒", None
        except FloodError as e:
            seconds = getattr(e, "seconds", None)
            hint = f"（{seconds}s）" if seconds else ""
            return False, f"🚫 Flood 限流，请稍后再试{hint}", None
        except (InviteHashInvalidError, InviteHashExpiredError) as e:
            return False, f"❌ 无效邀请链接: {e}", None
        except PeerIdInvalidError as e:
            return False, f"❌ 无效邀请链接: {e}", None
        except ChannelPrivateError as e:
            return False, f"🚫 私有目标为不可见：{e}", None

        try:
            res = await client(ImportChatInviteRequest(code))
            chats = getattr(res, "chats", [])
            for ch in chats:
                if isinstance(ch, (Channel, Chat)):
                    peer = utils.get_input_peer(ch)
                    return True, "✅ 成功通过邀请链接加入群组", peer
            return True, "✅ 成功通过邀请链接加入群组", None

        except UserAlreadyParticipantError:
            try:
                chat_obj = getattr(res_checked, "chat", None)
                peer = utils.get_input_peer(chat_obj) if chat_obj is not None else None
            except Exception:
                peer = None
            return True, "✅ 已在群中", peer

    except UserAlreadyParticipantError:
        return True, "✅ 已在群中", None
    except Exception as e:
        return False, f"❌ 邀请入群异常：{e}", None



# 公开群/频道加入
async def _join_public_group(client, pl: ParsedLink) -> Tuple[bool, str, Optional[Any]]:
    uname = pl.username or _to_username(str(pl.value))
    if not uname:
        return False, f"❌ 无效用户名：{pl.short()}", None
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", uname):
        return False, f"❌ 无效目标：{uname}", None
    try:
        full = await client.get_entity(uname)
    except UsernameNotOccupiedError as e:
        return False, f"❌ 无效用户名 @{uname}：{e}", None
    except UsernameInvalidError as e:
        return False, f"❌ 用户名格式无效 @{uname}：{e}", None
    except ChannelPrivateError as e:
        return False, f"🚫 私有目标为不可见：{e}", None

    if isinstance(full, User):
        return False, f"❌ @{uname} 是用户账号，不能作为群/频道加入", None

    try:
        inp = await client.get_input_entity(uname)
        await client(JoinChannelRequest(inp))

        try:
            ch = await client.get_entity(inp)
            msg = "✅ 成功加入公开群/频道"
            if isinstance(ch, Channel):
                flags = classify_channel_flags(ch)
                extras = []
                if flags.get("noforwards"): extras.append("禁止转发")
                if flags.get("slowmode_enabled"): extras.append("慢速模式")
                if flags.get("join_to_send"): extras.append("需加入才能发言")
                if flags.get("join_request"): extras.append("加入需审批")
                full_cf = await get_full_channel_safe(client, ch)
                if channel_has_paid_constraints(ch, full_cf):
                    extras.append("付费/Stars 限制")
                if extras:
                    msg = f"✅ 成功加入（{'，'.join(extras)}）"
            elif isinstance(ch, Chat):
                msg = "✅ 成功加入普通群"
            return True, msg, inp
        except Exception:
            return True, "✅ 成功加入公开群/频道", inp

    except UserAlreadyParticipantError:
        return True, "✅ 已加入", None
    except InviteRequestSentError as e:
        return False, f"⏳ 加入申请已发送，等待管理员审批：{e}", None
    except FloodWaitError as e:
        return False, f"🚫 限流，请等待 {e.seconds} 秒：{e}", None
    except ChannelsTooMuchError as e:
        return False, f"🚧 账号加入的频道/群组已达上限：{e}", None
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        return False, f"❌ 无效用户名 @{uname}：{e}", None
    except ChannelPrivateError as e:
        return False, f"🚫 私有频道/不可见：{e}", None
    except Exception as e:
        return False, f"❌ 加入失败：{type(e).__name__}: {e}", None


# 访问消息所属频道
async def _access_channel_msg(client, pl: ParsedLink) -> Tuple[bool, str, Optional[Any]]:
    try:
        cid, _ = pl.value  # type: ignore
        entity = await get_input_peer_for_c(client, int(cid))
        if entity is None:
            return False, "❌ 无法访问该 /c/<id> 会话（可能未加入或无权限）", None
        return True, "✅ 可访问消息所属会话（/c）", entity
    except Exception as e:
        log_error(f"CHANNEL_MSG 处理异常: {e}")
        return False, "❌ 解析消息会话失败", None

    # 解析数值 ID
async def _resolve_user_id(client, pl: ParsedLink) -> Tuple[bool, str, Optional[Any]]:
    try:
        entity = await client.get_input_entity(int(pl.value))
        return True, "ℹ️ 已解析 ID（用户/群/频道），不执行加入操作", entity
    except Exception as e:
        log_error(f"❌ 无法解析 ID: {e}")
        return False, f"❌ 无法解析 ID: {e}", None