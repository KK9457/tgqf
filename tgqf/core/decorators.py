# core/decorators.py
# -*- coding: utf-8 -*-
# core/decorators.py
from __future__ import annotations

import functools
from functools import wraps
from typing import Any, Awaitable, Callable, Optional


def super_command(
    *,
    trace_action: str = "未命名命令",
    phase: str = "handler",
    admin_only: bool = False,
    white_only: bool = False,
    session_lock: bool = False,
    session_lock_func: Callable[..., str] = None,
    session_lock_timeout: float | None = None,
    ensure_ctx: bool = True,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Optional[Any]]]]:
    """
    统一指令装饰器：
    - with_trace：注入追踪
    - command_safe：权限/白名单/异常保护
    - ensure_context：确保运行所需上下文已就绪
    - with_session_lock：可选会话级分布式锁
    """
    # 延迟导入，避免循环依赖
    from unified.lock_utils import default_session_lock_path, with_session_lock
    from unified.trace_context import with_trace

    if session_lock_func is None:
        session_lock_func = default_session_lock_path

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Optional[Any]]]:
        f: Callable[..., Awaitable[Any]] = func
        f = with_trace(trace_action, phase=phase)(f)
        f = command_safe(admin_only=admin_only, white_only=white_only)(f)
        if ensure_ctx:
            f = ensure_context(f)
        if session_lock:
            f = (
                with_session_lock(session_lock_func, timeout=session_lock_timeout)(f)
                if session_lock_timeout is not None
                else with_session_lock(session_lock_func)(f)
            )

        @functools.wraps(f)
        async def wrapper(*args, **kwargs) -> Optional[Any]:
            from unified.logger import log_exception
            from unified.trace_context import get_log_context
            from core.defaults.bot_utils import BotUtils  # 走统一的 BotUtils（已集成 parse_mode 策略）

            try:
                return await f(*args, **kwargs)
            except Exception as e:
                log_exception(
                    f"super_command({trace_action}) 异常",
                    exc=e,
                    extra=get_log_context(),
                )
                event = args[0] if args else kwargs.get("event")
                if event:
                    try:
                        await BotUtils.safe_respond(
                            event,
                            event.client,
                            "❌ 指令执行异常，请稍后再试。",
                        )
                    except Exception:
                        pass
                return None

        return wrapper

    return decorator


def extract_command_from_event(event: Any) -> str:
    try:
        if hasattr(event, "raw_text") and event.raw_text:
            s = str(event.raw_text or "")
            return (s.split() or ["unknown"])[0] or "unknown"
        if hasattr(event, "data") and event.data is not None:
            if isinstance(event.data, (bytes, bytearray)):
                return event.data.decode(errors="ignore")[:24]
            return str(event.data)[:24]
    except Exception:
        pass
    return "unknown"


def command_safe(
    admin_only: bool = False, white_only: bool = False
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[None]]]:
    """
    权限/白名单/异常保护：
    - admin_only：仅管理员可用；非管理员静默 ACK（若为回调）
    - white_only：白名单可用；管理员直通；非白名单静默 ACK（若为回调）
    - 捕获异常并通过 BotUtils 安全回信
    """
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[None]]:
        @wraps(func)
        async def wrapper(event: Any, *args, **kwargs) -> None:
            from core.defaults.bot_utils import BotUtils
            from unified.config import get_admin_ids
            from unified.context import get_fsm
            from unified.logger import log_debug, log_exception, log_info
            from unified.trace_context import get_log_context, inject_trace_context

            raw_uid = getattr(event, "sender_id", None)
            try:
                user_id = int(raw_uid) if raw_uid is not None else None
            except Exception:
                user_id = None

            chat_id = getattr(event, "chat_id", None)
            try:
                _ids = get_admin_ids() or set()
                admin_ids = {int(x) for x in _ids}
            except Exception:
                admin_ids = set()

            command_name = getattr(func, "__name__", "unknown")
            command_val = extract_command_from_event(event)

            inject_trace_context(
                func_name=command_name,
                user_id=user_id,
                chat_id=chat_id,
                command=command_val,
                phase="decorator",
                signal="entry",
                module=func.__module__,
                trace_name=f"执行命令 {command_name}",
            )

            try:
                if admin_only:
                    is_admin = (user_id in admin_ids) if user_id is not None else False
                    log_debug("command_safe: 进入 admin_only 分支", extra={
                        "user_id": user_id,
                        "is_admin": is_admin,
                        "admin_ids_count": len(admin_ids),
                    })
                    if not is_admin:
                        # 未授权：静默 ACK（若为回调），不提示，避免菊花
                        try:
                            if hasattr(event, "answer"):
                                await event.answer()
                        except Exception:
                            pass
                        return

                if white_only:
                    is_admin = (user_id in admin_ids) if user_id is not None else False
                    log_debug("command_safe: 进入 white_only 分支", extra={
                        "user_id": user_id,
                        "is_admin": is_admin,
                        "admin_ids": list(admin_ids),
                        "command": command_val,
                    })

                    if is_admin:
                        log_debug("command_safe: 管理员白名单直通（white_only）", extra={"user_id": user_id, "command": command_val})
                        await func(event, *args, **kwargs)
                        return

                    fsm = get_fsm()
                    if fsm is None:
                        # 上下文缺失：静默 ACK（若为回调），不提示
                        try:
                            if hasattr(event, "answer"):
                                await event.answer()
                        except Exception:
                            pass
                        return

                    is_white = False
                    try:
                        if user_id is not None:
                            is_white = await fsm.is_in_whitelist(0, user_id)
                            if (not is_white) and admin_ids:
                                owner_id = next(iter(admin_ids))
                                is_white = await fsm.is_in_whitelist(owner_id, user_id)
                    except Exception as e:
                        log_exception("command_safe: is_in_whitelist 异常", exc=e, extra={"user_id": user_id})

                    log_debug("command_safe: 白名单判定结果", extra={"user_id": user_id, "is_white": is_white})

                    if not is_white:
                        # 非白名单：静默 ACK（若为回调），不提示
                        try:
                            if hasattr(event, "answer"):
                                await event.answer()
                        except Exception:
                            pass
                        return

                log_debug(f"📩 执行命令：{command_name} by {user_id}", extra=get_log_context())
                await func(event, *args, **kwargs)

            except Exception as e:
                log_exception(f"❌ 命令处理异常：{command_name}", exc=e, extra=get_log_context())
                try:
                    await BotUtils.safe_respond(
                        event,
                        event.client,
                        "⚠️ 指令执行失败，请稍后重试",
                    )
                except Exception:
                    pass

        return wrapper

    return decorator


def ensure_context(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """
    关键依赖注入校验：FSM / client_manager / task_control / Bot / scheduler 等。
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        from unified.context import (
            get_bot,
            get_client_manager,
            get_fsm,
            get_scheduler,
            get_task_control,
        )
        from unified.logger import log_exception
        from unified.trace_context import get_log_context

        try:
            assert get_fsm() is not None, "❌ FSM 未初始化"
            assert get_client_manager() is not None, "❌ client_manager 未初始化"
            assert get_task_control() is not None, "❌ task_control 未初始化"
            assert get_bot() is not None, "❌ Bot 未注入"
            assert get_scheduler() is not None, "❌ scheduler 尚未绑定"
        except AssertionError as e:
            log_exception("❌ Context 初始化不完整", exc=e, extra=get_log_context())
            raise
        return await func(*args, **kwargs)

    return wrapper
