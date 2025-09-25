# unified/report_utils.py

from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Any, Dict, List, Optional, Tuple

from telethon.tl import functions
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, ReportRequest
from telethon.tl.types import (
    InputReportReasonChildAbuse,
    InputReportReasonCopyright,
    InputReportReasonFake,
    InputReportReasonGeoIrrelevant,
    InputReportReasonIllegalDrugs,
    InputReportReasonOther,
    InputReportReasonPersonalDetails,
    InputReportReasonPornography,
    InputReportReasonSpam,
    InputReportReasonViolence,
)

from core.telethon_errors import TE, detect_flood_seconds
from typess.link_types import ParsedLink
from tg.entity_utils import get_input_entity_strict
from tg.link_utils import normalize_link
from unified.logger import log_debug, log_error, log_exception, log_info, log_warning
from unified.trace_context import get_log_context


async def _send_report_message_request(client, *, peer, mid: int, reason_obj, message: str):
    """
    优先尝试老签名：
        ReportRequest(peer=peer, id=[mid], reason=reason_obj, message=message)
    若抛 TypeError（你的新版签名）则回退：
        option = reason_obj._bytes() if hasattr(reason_obj, '_bytes') else b''
        ReportRequest(peer=peer, id=[mid], option=option, message=message)
    """
    from telethon.tl.functions.messages import ReportRequest  # 保持原 import 路径

    # 先试“老签名”：reason=
    try:
        return await client(ReportRequest(peer=peer, id=[mid], reason=reason_obj, message=message))
    except TypeError as e_reason_sig:
        # 再试“新签名”：option=
        try:
            option_bytes = b""
            if hasattr(reason_obj, "_bytes"):
                try:
                    option_bytes = reason_obj._bytes()
                except Exception:
                    option_bytes = b""
            return await client(ReportRequest(peer=peer, id=[mid], option=option_bytes, message=message))
        except TypeError as e_option_sig:
            # 同时把两种签名的错误都打到日志里，便于排查
            from unified.logger import log_warning
            from unified.trace_context import get_log_context
            log_warning(
                "messages.ReportRequest 两种签名均失败",
                extra={
                    "err_reason_sig": str(e_reason_sig),
                    "err_option_sig": str(e_option_sig),
                    **(get_log_context() or {}),
                },
            )
            raise
        
# 兼容旧字符串中的“xx seconds”提取（新逻辑优先用 detect_flood_seconds）
def detect_wait_seconds(obj: Any) -> int:
    try:
        if isinstance(obj, dict):
            msg = str(
                obj.get("error")
                or obj.get("message_html")
                or obj.get("exception")
                or ""
            )
        else:
            msg = str(obj or "")
        m = re.search(r"(\d+)\s*seconds?", msg.lower())
        return int(m.group(1)) if m else 0
    except Exception:
        return 0


def _build_telegram_scene_hint(code: str, mode: int | None) -> str:
    code = (code or "other").lower()
    base = "You are reporting content on Telegram. Keep it platform-specific."
    if code == "spam":
        detail = "Focus on unsolicited promotions, repeated invite links, mass forwards, or scam sales in Telegram channels/groups/messages."
    elif code == "violence":
        detail = "Mention incitement to violence or threats posted in a Telegram channel/group/message."
    elif code == "pornography":
        detail = "Mention explicit sexual content shared via public Telegram channels/groups or messages."
    elif code == "child_abuse":
        detail = "Emphasize involvement of minors and explicit content on Telegram. Urgent review."
    elif code == "copyright":
        detail = "Refer to unauthorized redistribution of copyrighted media/files in a Telegram channel or group."
    elif code == "fake":
        detail = "Refer to misinformation or impersonation accounts operating on Telegram."
    elif code == "illegal_drugs":
        detail = "Refer to promotion/sale of illegal drugs in Telegram channels/groups."
    elif code == "personal_details":
        detail = "Refer to doxxing or sharing private data (phone, address, IDs) on Telegram."
    else:
        detail = "Refer to a clear violation of Telegram rules happening on Telegram."
    mode_hint = {
        1: "Report targets a user/chat/channel.",
        2: "Report targets a specific message.",
        3: "Report targets a profile photo.",
    }.get(mode or 0, "")
    return f"{base} {detail} {mode_hint}".strip()


# ============== 映射表（与 UI 对齐） ==============
REPORT_TYPE_MAPPING: Dict[int, Dict[str, str]] = {
    1: {"desc": "垃圾信息", "code": "spam", "en": "Scam or spam"},
    2: {"desc": "暴力内容", "code": "violence", "en": "Violent content"},
    3: {"desc": "色情内容", "code": "pornography", "en": "Pornographic content"},
    4: {"desc": "儿童色情", "code": "child_abuse", "en": "Child abuse"},
    5: {"desc": "侵犯版权", "code": "copyright", "en": "Copyright violation"},
    6: {"desc": "其他违规", "code": "other", "en": "Other violations"},
    7: {"desc": "虚假信息", "code": "fake", "en": "Fake news"},
    8: {"desc": "非法药物", "code": "illegal_drugs", "en": "Illegal drugs"},
    9: {"desc": "泄露隐私", "code": "personal_details", "en": "Personal data leak"},
}

# === 兜底英文模板（按 code 分类）===
DEFAULT_REASON_BY_CODE: Dict[str, str] = {
    "spam": (
        "This account/content is repeatedly sending unsolicited messages and mass promotions, "
        "disrupting normal use and violating Telegram's anti-spam policy."
    ),
    "violence": (
        "This account/content contains violent or graphic material that may cause harm or fear. "
        "It violates Telegram's safety rules."
    ),
    "pornography": (
        "This account/content contains sexual or explicit material that is inappropriate for distribution "
        "and violates Telegram guidelines."
    ),
    "child_abuse": (
        "This content endangers minors and violates child safety. Urgent review requested."
    ),
    "copyright": (
        "This account/content likely infringes intellectual property rights by distributing protected "
        "materials without authorization. Please investigate."
    ),
    "fake": (
        "This account shares deceptive or impersonation content misleading users on Telegram."
    ),
    "illegal_drugs": (
        "This account/content seems to promote the sale/distribution of illegal drugs, which is prohibited on Telegram."
    ),
    "personal_details": (
        "This content exposes private personal data without consent, posing safety and privacy risks."
    ),
    "geo_irrelevant": (
        "This content is geo-irrelevant and manipulates local discovery features, violating Telegram rules."
    ),
    "other": (
        "This account/content violates Telegram community guidelines. Please review and take appropriate actions."
    ),
}
DEFAULT_GENERIC = DEFAULT_REASON_BY_CODE["other"]

# 兜底文本库
REPORT_REASONS_DICT: Dict[str, List[str]] = {
    "spam": [
        "This content is unsolicited spam disrupting users.",
        "Repetitive spam messages are harming the community.",
        "The account is used to spam unrelated content.",
    ],
    "pornography": [
        "This material is explicit and violates Telegram rules.",
        "This content contains harmful adult material.",
    ],
    "child_abuse": [
        "This content endangers minors and violates child safety.",
        "Severe violation involving minors; urgent review required.",
    ],
    "personal_details": [
        "This content exposes personal data without consent.",
        "Personal information is disclosed without authorization.",
    ],
    "other": [
        "This content violates Telegram community guidelines.",
        "Clear violation of platform policy; please review.",
    ],
}

# === 举报 code 统一别名 ===
ALIAS_CODE = {
    "scam": "spam",
    "porn": "pornography",
    "illegal_goods": "illegal_drugs",
    "impersonation": "fake",
    "geo": "geo_irrelevant",
}


def normalize_report_code(code: str) -> str:
    c = (code or "other").strip().lower()
    return ALIAS_CODE.get(c, c)


def get_report_reason_object(code: str):
    code = normalize_report_code(code)
    mapping = {
        "spam": InputReportReasonSpam,
        "violence": InputReportReasonViolence,
        "pornography": InputReportReasonPornography,
        "child_abuse": InputReportReasonChildAbuse,
        "copyright": InputReportReasonCopyright,
        "fake": InputReportReasonFake,
        "illegal_drugs": InputReportReasonIllegalDrugs,
        "personal_details": InputReportReasonPersonalDetails,
        "geo_irrelevant": InputReportReasonGeoIrrelevant,
        "other": InputReportReasonOther,
    }
    cls = mapping.get(str(code).lower(), InputReportReasonOther)
    reason = cls()
    log_info(
        "🛠 使用举报类型对象",
        extra={"code": code, "reason": str(reason), **(get_log_context() or {})},
    )
    return reason


def parse_telegram_link(
    link: str,
) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    兼容层（仍保留）：解析简单 t.me 链接
    返回：(username_or_chat_id, msg_id, invite_hash)
    """
    s = (link or "").strip()
    m = re.match(r"^https?://t\.me/(?P<username>[\w\d_]+)/(?P<msg_id>\d+)$", s, re.I)
    if m:
        return m.group("username"), int(m.group("msg_id")), None
    m2 = re.match(r"^https?://t\.me/c/(?P<chat_id>\d+)/(?P<msg_id>\d+)$", s, re.I)
    if m2:
        return m2.group("chat_id"), int(m2.group("msg_id")), None
    m3 = re.match(r"^https?://t\.me/(?:joinchat/|\+)(?P<hash>[-_A-Za-z0-9]+)$", s, re.I)
    if m3:
        return None, None, m3.group("hash")
    return None, None, None


# ============== 目标解析（新版返回 dict；兼容旧签名） ==============
async def resolve_target(client_or_target, maybe_target: Optional[str] = None):
    """
    新版：resolve_target(client, target) -> dict{entity, msg_id, photo_id, invite, link}
    兼容：resolve_target(target) -> (entity, msg_id)
    """
    # ---------- 兼容旧签名：resolve_target(target) ----------
    if maybe_target is None:
        legacy_target = str(client_or_target)
        try:
            username_or_chat, msg_id, invite_hash = parse_telegram_link(legacy_target)
            if username_or_chat or invite_hash:
                return None, msg_id
            return None, None
        except Exception as e:
            log_exception(
                "resolve_target(legacy) 解析失败",
                exc=e,
                extra={"target": legacy_target, **(get_log_context() or {})},
            )
            return None, None

    # ---------- 新签名：resolve_target(client, target) ----------
    client = client_or_target
    raw = (maybe_target or "").strip()

    # 先做强力清洗并解析
    pl: Optional[ParsedLink] = None
    try:
        pl = normalize_link(raw)
    except Exception:
        try:
            pl = ParsedLink.parse(raw)
        except Exception:
            pl = None

    # 已能构造 ParsedLink
    if pl:
        try:
            # 公开/频道消息：返回 entity + msg_id
            if pl.is_public_msg() or pl.is_channel_msg():
                uname_or_cid = pl.username or pl.chat_id
                mid = pl.msg_id
                if uname_or_cid is not None and mid:
                    peer_like = (
                        uname_or_cid
                        if isinstance(uname_or_cid, str)
                        else int(f"-100{uname_or_cid}")
                    )
                    entity = await client.get_entity(peer_like)
                    log_info(
                        "目标解析成功(link)",
                        extra={
                            "target": raw,
                            "who": uname_or_cid,
                            "msg_id": mid,
                            **(get_log_context() or {}),
                        },
                    )
                    return {
                        "entity": entity,
                        "msg_id": mid,
                        "photo_id": None,
                        "invite": None,
                        "link": pl.to_link(),
                    }
            # 公共会话/纯 ID
            if pl.is_public() or pl.is_userid() or pl.is_chat_id():
                peer_like = pl.username or pl.peer_id
                entity = await client.get_entity(peer_like)
                return {
                    "entity": entity,
                    "msg_id": None,
                    "photo_id": None,
                    "invite": None,
                    "link": pl.to_link(),
                }
            # 邀请链接
            if pl.is_invite():
                return {
                    "entity": None,
                    "msg_id": None,
                    "photo_id": None,
                    "invite": str(pl.value),
                    "link": pl.to_link(),
                }
        except (TE.UsernameNotOccupiedError, TE.ChannelPrivateError, TE.PeerIdInvalidError) as e:
            log_warning(
                "ParsedLink 解析失败",
                extra={
                    "target": raw,
                    "err": f"{type(e).__name__}: {e}",
                    **(get_log_context() or {}),
                },
            )
        except Exception as e:
            log_exception("ParsedLink 解析异常", exc=e, extra={"target": raw, **(get_log_context() or {})})

    # 兜底：直接试 get_entity + 老解析
    try:
        username_or_chat, msg_id, invite_hash = parse_telegram_link(raw)
    except Exception:
        username_or_chat, msg_id, invite_hash = None, None, None

    if username_or_chat:
        try:
            peer_like = (
                int(f"-100{username_or_chat}")
                if username_or_chat.isdigit()
                else username_or_chat
            )
            entity = await client.get_entity(peer_like)
            return {
                "entity": entity,
                "msg_id": msg_id,
                "photo_id": None,
                "invite": invite_hash,
                "link": raw,
            }
        except TE.UsernameNotOccupiedError:
            log_error(f"❌ 目标用户名 `{raw}` 不存在！", extra=(get_log_context() or {}))
        except TE.ChannelPrivateError:
            log_error(f"❌ 频道 `{raw}` 私有，无法访问！", extra=(get_log_context() or {}))
        except TE.PeerIdInvalidError:
            log_error(f"❌ 无效的目标 ID: `{raw}`", extra=(get_log_context() or {}))
        except Exception as e:
            log_exception("目标解析失败", exc=e, extra={"target": raw, **(get_log_context() or {})})

    # 最后强行解析为 entity
    try:
        entity = await client.get_entity(raw)
        return {"entity": entity, "msg_id": None, "photo_id": None, "invite": None, "link": raw}
    except TE.UsernameNotOccupiedError:
        log_error(f"❌ 目标用户名 `{raw}` 不存在！", extra=(get_log_context() or {}))
    except TE.ChannelPrivateError:
        log_error(f"❌ 频道 `{raw}` 私有，无法访问！", extra=(get_log_context() or {}))
    except TE.PeerIdInvalidError:
        log_error(f"❌ 无效的目标 ID: `{raw}`", extra=(get_log_context() or {}))
    except Exception as e:
        log_exception("目标解析失败", exc=e, extra={"target": raw, **(get_log_context() or {})})
    return {"entity": None, "msg_id": None, "photo_id": None, "invite": None, "link": raw}


async def try_join_if_needed(
    client, *, entity=None, invite_hash: Optional[str] = None
) -> bool:
    """
    优先通过邀请 hash 入群；否则尝试 JoinChannelRequest(entity)。
    返回 True 表示“看起来已加入或已是成员”，False 表示失败。
    """
    ctx = {"invite": invite_hash, "has_entity": bool(entity), **(get_log_context() or {})}

    # 1) 邀请 hash
    if invite_hash:
        try:
            await client(ImportChatInviteRequest(invite_hash))
            log_info("已通过邀请链接加入", extra=ctx)
            return True
        except TE.UserAlreadyParticipantError:
            log_info("已是成员（邀请链接）", extra=ctx)
            return True
        except TE.FloodWaitError as fw:
            wait = float(getattr(fw, "seconds", 0) or 0) + random.uniform(1.0, 3.0)
            log_warning("邀请链接入群触发 FloodWait，退避后继续", extra={**ctx, "wait": int(wait)})
            await asyncio.sleep(wait)
        except (TE.InviteHashExpiredError, TE.InviteHashInvalidError):
            log_warning("邀请链接失效/无效，转尝试公开加入", extra=ctx)
        except Exception as e:
            log_warning("邀请链接入群失败，转尝试公开加入", extra={**ctx, "err": f"{type(e).__name__}: {e}"})

    # 2) 公开 Join
    if entity is not None:
        try:
            await client(JoinChannelRequest(entity))
            log_info("已通过 JoinChannelRequest 加入", extra=ctx)
            return True
        except TE.UserAlreadyParticipantError:
            log_info("已是成员（公开加入）", extra=ctx)
            return True
        except TE.FloodWaitError as fw:
            wait = float(getattr(fw, "seconds", 0) or 0) + random.uniform(1.0, 3.0)
            log_warning("公开加入触发 FloodWait，退避后放弃", extra={**ctx, "wait": int(wait)})
            await asyncio.sleep(wait)
            return False
        except (TE.ChannelPrivateError, TE.ChannelsTooMuchError) as e:
            log_warning("公开加入被拒绝", extra={**ctx, "err": f"{type(e).__name__}: {e}"})
            return False
        except Exception as e:
            log_exception("公开加入异常", exc=e, extra=ctx)
            return False

    log_warning("无法入群/频道（无可用 invite 或 entity）", extra=ctx)
    return False


# ============== 理由生成（懒加载 + 降级） ==============
async def _random_reason_from_pool(code: str) -> str:
    key = str(code or "other").lower()
    pool = REPORT_REASONS_DICT.get(key, REPORT_REASONS_DICT["other"])
    return random.choice(pool)


async def generate_report_reason(
    primary_code: str,
    secondary_desc: str | None = None,
    *,
    mode: int | None = None,
    scene: str | None = None,
) -> str:
    primary_code = normalize_report_code(primary_code)
    base = await _random_reason_from_pool(primary_code or "other")

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return base

    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=api_key)
        primary_desc = next(
            (v.get("en", v["desc"]) for v in REPORT_TYPE_MAPPING.values() if v["code"] == primary_code),
            "Other violations",
        )
        tg_hint = _build_telegram_scene_hint(primary_code, mode)
        scene_part = f" Specific scene: {scene}. " if scene else ""
        prompt = (
            "Write ONE short moderation report reason (<=25 words). "
            "Language: English. Audience: Telegram moderation. "
            f"Category: '{primary_desc}'.{scene_part} {tg_hint} "
            "Avoid email/banking examples unless relevant to Telegram. "
            "No preambles, hashtags, or quotes. Output only the sentence."
        )

        async def _call():
            resp = await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=60,
            )
            return (resp.choices[0].message.content or "").strip()

        # ⏱️ 设一个保守超时，避免卡批次
        reason = await asyncio.wait_for(_call(), timeout=float(os.environ.get("OPENAI_TIMEOUT", "12")))
        words = re.findall(r"\S+", reason or "")
        if len(words) > 25:
            reason = " ".join(words[:25])
        return reason or base

    except Exception as e:
        log_warning("GPT举报理由生成失败，使用本地模板", extra={"err": str(e), **(get_log_context() or {})})
        return base


async def _build_unique_reasons(
    report_type_code: str,
    n: int,
    *,
    mode: int | None = None,
    scene: str | None = None,
    **_compat,
) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    for _ in range(n):
        tries = 0
        chosen = None
        while tries < 3:
            tries += 1
            try:
                r = await generate_report_reason(report_type_code, mode=mode, scene=scene)
            except Exception:
                r = None
            r = (r or "This content violates Telegram rules and risks user safety.").strip()
            if r.lower() not in seen:
                chosen = r
                break
        if not chosen:
            chosen = f"{r} Please review."
        seen.add(chosen.lower())
        out.append(chosen)

    return out[:n]


# ============== 批量举报核心 ==============
async def do_report_multi_accounts(
    user_id: int,
    mode: int,
    report_type,  # 兼容：可传 dict 或 str(code)
    target: Any,  # 可为 entity（已解析）或 @/链接
    reason: str,
    client_manager,
    msg_id: int | None = None,
    photo_id: int | None = None,
    progress_cb=None,
    *,
    reasons: List[str] | None = None,  # 每账号不同理由
    reason_per_account: bool = False,  # 是否按账号分配 reasons
    limit_phones: List[str] | None = None,  # 仅限部分手机号
) -> dict:
    """
    返回 {"ok": int, "fail": int, "results": list[dict]}
    """
    report_code = (
        report_type.get("code")
        if isinstance(report_type, dict)
        else str(report_type or "other")
    )
    report_code = normalize_report_code(report_code)

    # CHANGE: 只有在当前用户 clients 为空时才 load_all
    clients_all = client_manager.get_clients_for_user(user_id) or {}
    if not clients_all:
        try:
            await client_manager.load_all_for_user(user_id)
        except Exception as e:
            log_warning("load_all_for_user 异常（首次为空时触发）", extra={"user_id": user_id, "err": str(e), **(get_log_context() or {})})
        clients_all = client_manager.get_clients_for_user(user_id) or {}

    # 可选：仅限部分手机号（本地裁剪，不再每次向 manager 取）
    clients = {
        p: c for p, c in clients_all.items()
        if not limit_phones or p in set(limit_phones)
    }

    ok, fail = 0, 0
    idx = 0
    results = []

    phone_list = list(clients.keys())

    for phone in phone_list:
        idx += 1

        # CHANGE: 直接用本地 clients 取，避免游标变化
        client = clients.get(phone)
        if client is None:
            log_warning("找不到对应 client，跳过", extra={"user_id": user_id, "phone": phone, **(get_log_context() or {})})
            fail += 1
            results.append({"phone": phone, "status": "fail", "reason_used": None, "why": "client-missing"})
            continue

        # 每账号不同理由
        reason_for_this = reason
        if reason_per_account and reasons:
            try:
                reason_for_this = reasons[(idx - 1) % len(reasons)]
            except Exception:
                pass

        log_debug("client-instance", extra={"phone": phone, "client_id": id(client), **(get_log_context() or {})})
        log_info(
            "单账号举报启动",
            extra={
                "user_id": user_id,
                "phone": phone,
                "mode": mode,
                "report_code": report_code,
                "target": str(target)[:120],
                "reason": reason_for_this,
                "idx": idx,
                **(get_log_context() or {}),
            },
        )

        try:
            if not client.is_connected():
                await client.connect()

            # ---- 解析 entity（如果上层没解析） ----
            entity = target
            real_msg_id = msg_id

            # 如果上层给的是字符串，或 entity 缺 id，则用该 client 再解析
            if isinstance(entity, str) or not (hasattr(entity, "id") and getattr(entity, "id")):
                parsed = await resolve_target(client, str(target))
                if isinstance(parsed, tuple):
                    entity, real_msg_id = parsed
                else:
                    entity = parsed.get("entity")
                    real_msg_id = parsed.get("msg_id") or msg_id

            # ✅ 统一转换为 InputPeer（严格模式）
            peer = None
            try:
                if entity is not None:
                    peer = await get_input_entity_strict(client, entity)
                elif isinstance(target, str) and target.strip():
                    ent2 = await client.get_entity(target.strip())
                    peer = await get_input_entity_strict(client, ent2)
            except Exception:
                try:
                    ent3 = await client.get_entity(str(target))
                    peer = await get_input_entity_strict(client, ent3)
                except Exception:
                    peer = None

            if mode == 1:
                # 举报用户/群/频道
                if not peer:
                    log_warning(
                        "举报 entity/peer 解析失败（可能为跨会话对象或私有频道不可达）",
                        extra={"user_id": user_id, "phone": phone, "target": str(target)[:120], "branch": "mode_1", **(get_log_context() or {})},
                    )
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "peer-resolve-failed"})
                    continue

                await asyncio.sleep(random.uniform(2, 5))
                reason_obj = get_report_reason_object(report_code)

                try:
                    result = await client(
                        functions.account.ReportPeerRequest(peer=peer, reason=reason_obj, message=reason_for_this)
                    )
                except TE.FloodWaitError as e:
                    sec = detect_flood_seconds(e) or getattr(e, "seconds", 0) or 0
                    log_warning("FloodWait遇到休眠", extra={"user_id": user_id, "phone": phone, "wait_seconds": sec, **(get_log_context() or {})})
                    await asyncio.sleep(float(sec) + random.uniform(10, 20))
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": f"floodwait-{sec}s"})
                    if progress_cb:
                        try:
                            await progress_cb(idx, len(clients), ok, fail)
                        except Exception:
                            pass
                    await asyncio.sleep(random.uniform(3, 8))
                    continue

            elif mode == 2:
                # 举报消息
                mid = real_msg_id or msg_id
                if not peer or not mid:
                    log_warning(
                        "举报消息缺少 peer 或 msg_id",
                        extra={"user_id": user_id, "phone": phone, "peer": str(peer), "msg_id": mid, **(get_log_context() or {})},
                    )
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "missing-peer-or-msgid"})
                    continue

                await asyncio.sleep(random.uniform(2, 5))
                reason_obj = get_report_reason_object(report_code)

                try:
                    result = await _send_report_message_request(
    client, peer=peer, mid=mid, reason_obj=reason_obj, message=reason_for_this
)
                except Exception:
                    log_warning(
                        "messages.ReportRequest异常，尝试入群/频道后重试",
                        extra={"user_id": user_id, "phone": phone, "peer": str(peer), "msg_id": mid, **(get_log_context() or {})},
                    )
                    invite_hash = None
                    parsed2 = None
                    try:
                        parsed2 = await resolve_target(client, str(target))
                        if isinstance(parsed2, dict):
                            invite_hash = parsed2.get("invite")
                            if not peer:
                                ent2 = parsed2.get("entity")
                                if ent2 is not None:
                                    peer = await get_input_entity_strict(client, ent2)
                    except Exception:
                        invite_hash = None

                    joined = await try_join_if_needed(client, entity=entity, invite_hash=invite_hash)
                    if joined:
                        await asyncio.sleep(random.uniform(1.0, 2.5))
                        try:
                            if not peer and isinstance(parsed2, dict):
                                ent3 = parsed2.get("entity")
                                if ent3 is not None:
                                    peer = await get_input_entity_strict(client, ent3)
                            result = await _send_report_message_request(
    client, peer=peer, mid=mid, reason_obj=reason_obj, message=reason_for_this
)
                        except Exception:
                            try:
                                await asyncio.sleep(random.uniform(2, 5))
                                result = await client(
                                    functions.account.ReportPeerRequest(peer=peer, reason=reason_obj, message=reason_for_this)
                                )
                            except Exception as e3:
                                log_warning(
                                    "降级 ReportPeerRequest 仍失败",
                                    extra={"user_id": user_id, "phone": phone, "err": str(e3), **(get_log_context() or {})},
                                )
                                fail += 1
                                results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "fallback-peer-report-failed"})
                                if progress_cb:
                                    try:
                                        await progress_cb(idx, len(clients), ok, fail)
                                    except Exception:
                                        pass
                                await asyncio.sleep(random.uniform(3, 8))
                                continue
                    else:
                        try:
                            await asyncio.sleep(random.uniform(2, 5))
                            result = await client(
                                functions.account.ReportPeerRequest(peer=peer, reason=reason_obj, message=reason_for_this)
                            )
                        except Exception as e2:
                            log_warning(
                                "入群失败且降级 ReportPeerRequest 仍失败",
                                extra={"user_id": user_id, "phone": phone, "err": str(e2), **(get_log_context() or {})},
                            )
                            fail += 1
                            results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "fallback-peer-report-failed"})
                            if progress_cb:
                                try:
                                    await progress_cb(idx, len(clients), ok, fail)
                                except Exception:
                                    pass
                            await asyncio.sleep(random.uniform(3, 8))
                            continue

            elif mode == 3:
                # 举报头像
                if not entity:
                    log_warning(
                        "举报头像 entity 为空",
                        extra={"user_id": user_id, "phone": phone, "target": str(target)[:120], **(get_log_context() or {})},
                    )
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "entity-parse-failed"})
                    continue

                # CHANGE: 头像举报也用 peer（更稳）
                peer_for_photo = None
                try:
                    peer_for_photo = await get_input_entity_strict(client, entity)
                except Exception:
                    try:
                        ent2 = await client.get_entity(entity)
                        peer_for_photo = await get_input_entity_strict(client, ent2)
                    except Exception:
                        peer_for_photo = None

                try:
                    photos = await client.get_profile_photos(entity)
                except Exception as e:
                    log_warning("获取头像失败", extra={"user_id": user_id, "phone": phone, "err": str(e), **(get_log_context() or {})})
                    photos = None

                if not photos or not getattr(photos, "total", 0):
                    log_warning("被举报对象没有头像", extra={"user_id": user_id, "phone": phone, "target": str(entity), **(get_log_context() or {})})
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "no-photos"})
                    continue

                p0 = photos[0]
                from telethon.tl import types as tl_types
                try:
                    input_photo = tl_types.InputPhoto(
                        id=p0.id, access_hash=p0.access_hash, file_reference=p0.file_reference or b""
                    )
                except Exception as e:
                    log_warning("构造 InputPhoto 失败", extra={"user_id": user_id, "phone": phone, "err": str(e), **(get_log_context() or {})})
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "inputphoto-build-failed"})
                    continue

                await asyncio.sleep(random.uniform(2, 5))
                reason_obj = get_report_reason_object(report_code)

                try:
                    result = await client(
                        functions.account.ReportProfilePhotoRequest(
                            peer=peer_for_photo or entity,  # 优先 InputPeer
                            photo_id=input_photo,
                            reason=reason_obj,
                            message=reason_for_this,
                        )
                    )
                except Exception as e:
                    log_exception(
                        "ReportProfilePhotoRequest异常",
                        exc=e,
                        extra={
                            "user_id": user_id,
                            "phone": phone,
                            "entity": str(entity),
                            "photo_id": getattr(p0, "id", None),
                            "reason_obj": str(reason_obj),
                            "reason": reason_for_this,
                            **(get_log_context() or {}),
                        },
                    )
                    fail += 1
                    results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "ReportProfilePhotoRequest-exception"})
                    if progress_cb:
                        try:
                            await progress_cb(idx, len(clients), ok, fail)
                        except Exception:
                            pass
                    await asyncio.sleep(random.uniform(3, 8))
                    continue

            else:
                log_warning("未知 mode，自动失败", extra={"user_id": user_id, "phone": phone, "mode": mode, **(get_log_context() or {})})
                fail += 1
                results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "unknown-mode"})
                continue

            # 结果判断
            if bool(result):
                ok += 1
                results.append({"phone": phone, "status": "ok", "reason_used": reason_for_this})
            else:
                log_warning("举报接口返回无效", extra={"user_id": user_id, "phone": phone, "branch": f"mode_{mode}", "result": str(result), **(get_log_context() or {})})
                fail += 1
                results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "empty-result"})

        except TE.FloodWaitError as e:
            sec = detect_flood_seconds(e) or getattr(e, "seconds", 0) or 0
            log_warning("FloodWait遇到休眠", extra={"user_id": user_id, "phone": phone, "wait_seconds": sec, **(get_log_context() or {})})
            await asyncio.sleep(float(sec) + random.uniform(10, 20))
            fail += 1
            results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": f"floodwait-{sec}s"})
        except Exception as e:
            log_exception("账号举报顶级异常", exc=e, extra={"user_id": user_id, "phone": phone, "mode": mode, "target": str(target)[:120], **(get_log_context() or {})})
            fail += 1
            results.append({"phone": phone, "status": "fail", "reason_used": reason_for_this, "why": "top-exception"})

        if progress_cb:
            try:
                await progress_cb(idx, len(clients), ok, fail)
            except Exception:
                pass

        await asyncio.sleep(random.uniform(3, 8))

    log_info("本轮举报全部结束", extra={"user_id": user_id, "ok": ok, "fail": fail, **(get_log_context() or {})})
    return {"ok": ok, "fail": fail, "results": results}