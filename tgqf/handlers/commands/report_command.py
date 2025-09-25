# handlers/commands/report_command.py
from __future__ import annotations

import asyncio
import inspect
import os
import json
from typing import Any, Dict, List, Optional, Tuple

from telethon import Button, events

from core.decorators import super_command, command_safe, ensure_context
from core.registry_guard import safe_add_event_handler
from router.command_router import main_router
from unified.config import get_user_dir
from unified.context import (
    get_client_manager,
    get_reporter,
    get_scheduler,
    get_redis,
)
from unified.logger import log_exception, log_info, log_warning
from unified.report_utils import (
    REPORT_TYPE_MAPPING,
    _build_unique_reasons,
    do_report_multi_accounts,
    normalize_report_code,
    resolve_target,
)
from unified.trace_context import get_log_context, inject_trace_context
from tg.link_utils import normalize_link, check_single_link_availability


# ================== 常量/Pattern ================== #
_REPORT_STATE_TTL = 3600  # 1h

PATTERN_REPORT_MODE = rb"^report_mode_\d+$"
PATTERN_REPORT_TYPE = rb"^report_type_[a-z_]+$"
CB_REPORT_ENTRY     = b"report:entry"
CB_REPORT_CANCEL    = b"report_cancel"

def _bind_reporter_best_effort(reporter, event, uid):
    """
    自适应调用 reporter.bind(...)：
    - 优先传 event
    - 其次尝试 user_id / uid / chat_id
    - 全部不匹配则尝试纯位置参数或无参
    """
    try:
        sig = inspect.signature(reporter.bind)
        params = sig.parameters

        kwargs = {}
        if "event" in params:
            kwargs["event"] = event
        # user id 相关的不同命名
        if "user_id" in params:
            kwargs["user_id"] = uid
        elif "uid" in params:
            kwargs["uid"] = uid
        elif "chat_id" in params and getattr(event, "chat_id", None) is not None:
            kwargs["chat_id"] = event.chat_id

        if kwargs:
            reporter.bind(**kwargs)
        else:
            # 优先尝试带 event 的位置参数
            try:
                reporter.bind(event)
            except TypeError:
                reporter.bind()
        log_info("Reporter 绑定成功", extra={"user_id": uid, "bind_kwargs": list(kwargs.keys())})
    except Exception as e:
        log_warning("Reporter 绑定失败，已忽略", extra={"user_id": uid, "err": str(e)})


# ================== Redis 兼容小工具（支持 sync/async 客户端） ================== #
async def _redis_get(redis, key: str) -> Optional[str]:
    if not redis:
        return None
    try:
        res = redis.get(key)
        if inspect.isawaitable(res):
            res = await res
        if res is None:
            return None
        if isinstance(res, (bytes, bytearray)):
            return res.decode("utf-8", errors="ignore")
        return str(res)
    except Exception:
        return None

async def _redis_setex(redis, key: str, seconds: int, value: str) -> None:
    if not redis:
        return
    try:
        res = redis.setex(key, seconds, value)
        if inspect.isawaitable(res):
            await res
    except Exception:
        pass

async def _redis_del(redis, key: str) -> None:
    if not redis:
        return
    try:
        res = redis.delete(key)
        if inspect.isawaitable(res):
            await res
    except Exception:
        pass

# ================== 状态（Redis 为主，内存兜底） ================== #
def _state_key(uid: int) -> str:
    return f"report:state:{int(uid)}"

_REPORT_MEM: Dict[int, Dict[str, Any]] = {}

async def load_state(uid: int) -> Dict[str, Any]:
    redis = None
    try:
        redis = get_redis()
    except Exception:
        redis = None
    if redis:
        raw = await _redis_get(redis, _state_key(uid))
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                return {}
        return {}
    return _REPORT_MEM.setdefault(uid, {})

async def save_state(uid: int, st: Dict[str, Any]) -> None:
    redis = None
    try:
        redis = get_redis()
    except Exception:
        redis = None
    if redis:
        await _redis_setex(
            redis,
            _state_key(uid),
            _REPORT_STATE_TTL,
            json.dumps(st, ensure_ascii=False, default=str)  # ← 这里
        )
        return
    _REPORT_MEM[uid] = st

async def clear_state(uid: int) -> None:
    redis = None
    try:
        redis = get_redis()
    except Exception:
        redis = None
    if redis:
        await _redis_del(redis, _state_key(uid))
    _REPORT_MEM.pop(uid, None)


# ================== 入口命令 ==================
@super_command(trace_action="举报入口", admin_only=True, session_lock=True, session_lock_timeout=120, ensure_ctx=True)
async def report_entry(event):
    uid = event.sender_id
    inject_trace_context("report_entry", user_id=uid, phase="handler", signal="entry")
    await clear_state(uid)
    st = {"step": 1, "user_id": uid}
    await save_state(uid, st)
    log_info("进入举报流程", extra={"user_id": uid, **(get_log_context() or {})})

    buttons = [
        [Button.inline("1️⃣ 举报用户/群组/频道", data=b"report_mode_1")],
        [Button.inline("2️⃣ 举报频道/群组消息", data=b"report_mode_2")],
        [Button.inline("3️⃣ 举报头像", data=b"report_mode_3")],
        [Button.inline("❌ 取消举报", data=CB_REPORT_CANCEL)],
    ]
    markup = event.client.build_reply_markup(buttons, inline_only=True)
    await event.respond("请选择举报类型 👇", buttons=markup)



# ================== 选择大类（回调） ==================
@super_command(admin_only=True)
@ensure_context
async def on_report_mode_cb(event):
    uid = event.sender_id
    inject_trace_context("on_report_mode_cb", user_id=uid, phase="handler", signal="callback")
    st = await load_state(uid)
    try:
        mode = int((event.data or b"").decode().split("_")[-1])
    except Exception:
        await event.answer("❌ 非法模式指令", alert=True); return

    st["mode"] = mode
    st["step"] = 2
    await save_state(uid, st)
    log_info("举报主模式选择", extra={"user_id": uid, "mode": mode, **(get_log_context() or {})})

    buttons = [[Button.inline(f"{k}. {v['desc']}", data=f"report_type_{v['code']}".encode())] for k, v in REPORT_TYPE_MAPPING.items()]
    buttons.append([Button.inline("❌ 取消举报", data=CB_REPORT_CANCEL)])
    try:
        await event.edit("选择举报类型：", buttons=event.client.build_reply_markup(buttons, inline_only=True))
    except Exception:
        await event.respond("选择举报类型：", buttons=event.client.build_reply_markup(buttons, inline_only=True))


# ================== 选择理由类型（回调） ==================
@super_command(admin_only=True)
@ensure_context
async def on_report_type_cb(event):
    uid = event.sender_id
    inject_trace_context("on_report_type_cb", user_id=uid, phase="handler", signal="callback")
    st = await load_state(uid)
    code = (event.data or b"").decode(errors="ignore").split("_", 2)[-1]
    rt = next((v for v in REPORT_TYPE_MAPPING.values() if v.get("code") == code), None)
    if not rt:
        await event.answer("❌ 无效的举报理由类型", alert=True); return

    st["report_type"] = rt
    st["step"] = 3
    await save_state(uid, st)
    log_info("举报理由选择", extra={"user_id": uid, "report_type": code, **(get_log_context() or {})})

    try:
        await event.edit("请输入被举报对象（@用户名、t.me链接或消息链接）：")
    except Exception:
        await event.respond("请输入被举报对象（@用户名、t.me链接或消息链接）：")

# ================== 输入对象（消息） ==================
@super_command(admin_only=True)
@ensure_context
async def on_report_text_input(event):
    uid = event.sender_id
    inject_trace_context("on_report_text_input", user_id=uid, phase="handler", signal="message")
    st = await load_state(uid)
    if st.get("step") != 3:
        return  # 非举报输入阶段，忽略

    st["raw_target"] = (event.text or "").strip()
    st["step"] = 4
    await save_state(uid, st)
    log_info("输入举报对象", extra={"user_id": uid, "target": st.get("raw_target"), **(get_log_context() or {})})

    await event.respond("正在解析目标并准备批量举报，请稍候...")
    await do_report(event)

# ================== 取消（回调） ==================
@super_command(admin_only=True)
@ensure_context
async def on_report_cancel_cb(event):
    uid = event.sender_id
    inject_trace_context("on_report_cancel_cb", user_id=uid, phase="handler", signal="callback")
    await clear_state(uid)
    log_info("举报流程用户取消", extra={"user_id": uid, **(get_log_context() or {})})
    try:
        await event.answer("❌ 已取消举报", alert=True)
    except Exception:
        pass
    await event.respond("举报操作已取消")

# ================== 批量举报主流程 ==================
async def _discover_sessions_by_walk(uid: int) -> List[Tuple[str, str]]:
    """递归扫描用户目录，查找 .session"""
    user_dir = get_user_dir(uid)
    found: List[Tuple[str, str]] = []
    for root, _, files in os.walk(user_dir):
        for fname in files:
            if fname.endswith(".session"):
                phone = fname[:-8]
                found.append((phone, os.path.join(root, fname)))
    return found


async def _ensure_clients_for_user(uid: int, client_manager):
    try:
        await client_manager.release_all_clients(uid)
    except Exception:
        pass
    try:
        if hasattr(client_manager, "cleanup_all_journals"):
            client_manager.cleanup_all_journals(uid)
    except Exception:
        pass

    phones: Optional[List[Tuple[str, str]]] = None
    try:
        phones = client_manager.list_user_phones(uid)
        log_info("举报流程-扫描到用户手机列表", extra={"user_id": uid, "phones": phones, **(get_log_context() or {})})
    except Exception as e:
        log_warning("list_user_phones 异常", extra={"user_id": uid, "err": str(e), **(get_log_context() or {})})

    if not phones:
        log_warning("list_user_phones 返回空，尝试 load_all_for_user", extra={"user_id": uid, **(get_log_context() or {})})
        try:
            await client_manager.load_all_for_user(uid)
        except Exception as e:
            log_warning("load_all_for_user 异常", extra={"user_id": uid, "err": str(e), **(get_log_context() or {})})

        clients = client_manager.get_clients_for_user(uid) or {}
        if clients:
            log_info("举报流程-采用 load_all_for_user 的结果",
                     extra={"user_id": uid, "count": len(clients), "phones": list(clients.keys()), **(get_log_context() or {})})
            return clients

        try:
            phones = await _discover_sessions_by_walk(uid)
            log_info("举报流程-递归扫描会话文件", extra={"user_id": uid, "found": phones, **(get_log_context() or {})})
        except Exception as e:
            log_warning("递归扫描目录异常", extra={"user_id": uid, "err": str(e), **(get_log_context() or {})})

    for phone, session_path in phones or []:
        try:
            ok = await client_manager.try_create_and_register(uid, phone, session_path=session_path)
            if not ok:
                log_warning("注册账号失败（举报流程）", extra={"user_id": uid, "phone": phone, **(get_log_context() or {})})
        except Exception as e:
            log_warning("注册账号异常（举报流程）",
                        extra={"user_id": uid, "phone": phone, "err": str(e), **(get_log_context() or {})})

    clients = client_manager.get_clients_for_user(uid) or {}
    log_info("举报流程-账号注册完成",
             extra={"user_id": uid, "count": len(clients), "phones": list(clients.keys()), **(get_log_context() or {})})
    return clients

async def do_report(event):
    uid = event.sender_id
    inject_trace_context("do_report", user_id=uid, phase="handler", signal="start")
    st = await load_state(uid)

    client_manager = get_client_manager()
    reporter = get_reporter()
    if reporter:
        _bind_reporter_best_effort(reporter, event, uid)
    log_info("开始进入批量举报流程", extra={"user_id": uid, "state": st, **(get_log_context() or {})})

    mode = st.get("mode")
    report_type = st.get("report_type")
    target = st.get("target")
    reason = st.get("reason")  # 不再依赖；下面会生成 N 条
    msg_id = st.get("msg_id")
    photo_id = st.get("photo_id")

    # 链接可用性预检（best-effort）
    try:
        raw = st.get("raw_target")
        if raw:
            pl = normalize_link(raw)
            try:
                redis = get_redis()
            except Exception:
                redis = None
            ok, why = await check_single_link_availability(event.client, pl, redis=redis, user_id=uid, skip_invite_check=False)
            if not ok:
                await event.respond(f"⚠️ 该链接看起来不可达或无效：{why}")
    except Exception:
        pass

    # 解析目标（上层如未解析成功，这里兜底）
    if not target:
        parsed: Optional[Dict[str, Any]] = None
        try:
            parsed = await resolve_target(event.client, st.get("raw_target", ""))
            if isinstance(parsed, tuple):
                entity, mid = parsed
                parsed = {"entity": entity, "msg_id": mid}
        except Exception as e:
            log_warning("解析被举报对象失败（do_report兜底）", extra={"user_id": uid, "err": str(e), **(get_log_context() or {})})

        if isinstance(parsed, dict):
            safe_target = parsed.get("link") or st.get("raw_target")  # ← 只存可序列化的
            st["target"] = safe_target
            st["msg_id"] = parsed.get("msg_id")
            st["photo_id"] = parsed.get("photo_id")
            await save_state(uid, st)
            log_info("目标解析成功", extra={"user_id": uid, "target": str(st["target"]), "msg_id": st.get("msg_id"), **(get_log_context() or {})})
        else:
            st["target"] = st.get("raw_target")
            await save_state(uid, st)


    # 1) 确保可用 clients
    clients = await _ensure_clients_for_user(uid, client_manager)
    total = len(clients)

    # 2) 可选 validator
    try:
        scheduler = get_scheduler()
    except Exception:
        scheduler = None

    if scheduler and getattr(scheduler, "validator", None):
        try:
            valid_clients, invalid_clients = await scheduler.validator.validate_clients(uid, clients)
            clients = valid_clients or {}
            total = len(clients)
            log_info("账号验证完成（举报流程）",
                     extra={"user_id": uid, "valid": list(clients.keys()),
                            "invalid": (list(invalid_clients) if isinstance(invalid_clients, (list, tuple, set)) else invalid_clients),
                            **(get_log_context() or {})})
        except Exception as e:
            log_warning("账号验证阶段异常（举报流程），降级使用原始 clients", extra={"err": str(e), **(get_log_context() or {})})

    if total == 0:
        await event.respond("⚠️ 没有可用账号，无法发起举报。请先上传/登录账号。")
        await clear_state(uid)
        return

    msg = await event.respond(f"{total} 个活跃账号，正在自动生成举报理由并开始批量举报...")

    # 3) 归一化 report_type
    report_type_code = normalize_report_code(report_type.get("code") if isinstance(report_type, dict) else str(report_type))
    # 4) 生成 N 条不同理由
    try:
        reasons = await _build_unique_reasons(
            report_type_code, total, mode=mode, scene=None,
            target=str(st.get("target")) if st.get("target") else None,
            msg_id=st.get("msg_id"), photo_id=st.get("photo_id"),
            max_len=400, llm_call=None
        )
    except Exception as e:
        log_warning("批量理由生成异常，降级为类型模板", extra={"err": str(e), **(get_log_context() or {})})
        from unified.report_utils import DEFAULT_REASON_BY_CODE, normalize_report_code as _norm
        code_norm = _norm(report_type_code)
        base = reason or DEFAULT_REASON_BY_CODE.get(code_norm, DEFAULT_REASON_BY_CODE["other"])
        reasons = [base] * total

    # 4.1) 检测底层是否支持 reasons/reason_per_account
    supports_reasons = False
    try:
        sig = inspect.signature(do_report_multi_accounts)
        supports_reasons = ("reasons" in sig.parameters) and ("reason_per_account" in sig.parameters)
    except Exception:
        supports_reasons = False

    # 5) 进度回调（1.5s 节流）
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    _last_edit_ts = 0.0

    async def progress(idx, total, ok, fail):
        nonlocal _last_edit_ts
        now = loop.time()
        if idx == total or (now - _last_edit_ts) >= 1.5:
            _last_edit_ts = now
            try:
                await msg.edit(f"进度：{idx}/{total}\n✅ 成功:{ok}\n❌ 失败:{fail}")
            except Exception:
                pass

    # 6) 调用底层批量举报
    try:
        if supports_reasons:
            result = await do_report_multi_accounts(
                user_id=uid, mode=mode, report_type=report_type_code,
                target=st.get("target"), reason=reasons[0],
                client_manager=get_client_manager(),
                msg_id=st.get("msg_id"), photo_id=st.get("photo_id"),
                progress_cb=progress, reasons=reasons, reason_per_account=True,
            )
        else:
            ok = fail = 0
            idx = 0
            for phone, _client in list(clients.items()):
                idx += 1
                r = reasons[(idx - 1) % len(reasons)]
                try:
                    one = await do_report_multi_accounts(
                        user_id=uid, mode=mode, report_type=report_type_code,
                        target=st.get("target"), reason=r,
                        client_manager=get_client_manager(),
                        msg_id=st.get("msg_id"), photo_id=st.get("photo_id"),
                        progress_cb=progress, limit_phones=[phone],
                    )
                    ok += int(one.get("ok", 0))
                    fail += int(one.get("fail", 0))
                except Exception as e:
                    log_warning("单账号举报异常（降级路径）", extra={"phone": phone, "err": str(e), **(get_log_context() or {})})
                    fail += 1
            result = {"ok": ok, "fail": fail, "results": []}
    except Exception as e:
        log_exception("批量举报异常", exc=e)
        try:
            await msg.edit(f"❌ 批量举报异常：{e}")
        except Exception:
            await event.respond(f"❌ 批量举报异常：{e}")
        await clear_state(uid)
        return

    # 7) 报告
    try:
        rep = get_reporter()
        if rep:
            for item in result.get("results", []):
                await rep.report_report_result(
                    user_id=uid,
                    phone=item.get("phone", ""),
                    target=str(st.get("raw_target") or st.get("target")),
                    status=item.get("status", ""),
                    reason_used=item.get("reason_used", ""),
                    why=item.get("why"),
                )
    except Exception:
        pass

    ok = int(result.get("ok", 0))
    fail = int(result.get("fail", 0))
    try:
        await msg.edit(f"全部举报完毕。\n✅ 成功: {ok}\n❌ 失败: {fail}")
    except Exception:
        await event.respond(f"全部举报完毕。\n✅ 成功: {ok}\n❌ 失败: {fail}")

    try:
        rep2 = get_reporter()
        if rep2:
            await rep2.report_report_summary(uid, ok, fail, result.get("results", []))
    except Exception:
        pass

    await clear_state(uid)
    log_info("举报流程结束", extra={"user_id": uid, "ok": ok, "fail": fail, **(get_log_context() or {})})


# ================== 对外注册 ==================
def register_callbacks(bot):
    safe_add_event_handler(bot, on_report_mode_cb, events.CallbackQuery(pattern=PATTERN_REPORT_MODE), tag="report:mode")
    safe_add_event_handler(bot, on_report_type_cb, events.CallbackQuery(pattern=PATTERN_REPORT_TYPE), tag="report:type")
    safe_add_event_handler(bot, on_report_cancel_cb, events.CallbackQuery(data=CB_REPORT_CANCEL), tag="report:cancel")
    # 支持从按钮进入举报入口（不用 /jb）
    safe_add_event_handler(bot, report_entry, events.CallbackQuery(data=CB_REPORT_ENTRY), tag="report:entry")
    # 文本输入阶段（所有新消息进来，内部自己判 step==3）
    safe_add_event_handler(bot, on_report_text_input, events.NewMessage(), tag="report:input")

def register_commands():
    # /jb 入口命令（受 super_command admin_only 控制）
    main_router.command(["/jb"], trace_name="举报入口", admin_only=True)(report_entry)
    return True
