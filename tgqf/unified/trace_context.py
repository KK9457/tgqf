# unified/trace_context.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import uuid
from contextvars import ContextVar, copy_context
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, Optional


# ========================= 上下文容器（标准字段；已统一命名） =========================
# 变更：
# - ❌ task_id 移除，统一用 trace_id
# - ✅ group 为标准；写入 link 将被归一到 group
# - ✅ me_phone 取代 phone
# - ✅ bot_name 取代 session_name
_log_ctx: Dict[str, ContextVar] = {
    "user_id": ContextVar("user_id", default=None),
    "trace_id": ContextVar("trace_id", default=None),
    "group": ContextVar("group", default=None),
    "me_phone": ContextVar("me_phone", default=None),
    "me_id": ContextVar("me_id", default=None),
    "me_username": ContextVar("me_username", default=None),
    "bot_name": ContextVar("bot_name", default=None),
    "task_id": ContextVar("task_id", default=None),
    "round": ContextVar("round", default=None),
    "index": ContextVar("index", default=None),
    "func": ContextVar("func", default=None),
    "mod_name": ContextVar("mod_name", default=None),
    "function": ContextVar("function", default=None),
    "phase": ContextVar("phase", default=None),
    "signal": ContextVar("signal", default=None),
    "error": ContextVar("error", default=None),
    "exception": ContextVar("exception", default=None),
}

# === 兼容老变量（保留以兼容既有调用点） ===
trace_id_var = contextvars.ContextVar("trace_id", default=None)
user_id_var = contextvars.ContextVar("user_id", default=None)


# ========================= 别名归一化（仅写入时生效） =========================
# 说明：仅在 set_log_context / use_log_context / inject_trace_context 写入阶段做映射；
# get_log_context 仅返回“新标准键”。
_ALIAS_IN_MAP: Dict[str, str] = {
    # 老→新

    "link": "group",
    "phone": "me_phone",
    "session_name": "bot_name",
}

def _normalize_incoming_ctx(data: Dict[str, Any]) -> Dict[str, Any]:
    """将传入上下文字段按新标准归一化；冲突时新键优先。"""
    if not data:
        return {}
    out: Dict[str, Any] = {}
    # 1) 先放新键
    for k, v in data.items():
        if k in _log_ctx:   # 标准新键
            out[k] = v
    # 2) 再处理旧键→新键（已存在新键则保留新键，不覆盖）
    for old, new in _ALIAS_IN_MAP.items():
        if old in data and new not in out:
            out[new] = data[old]
    return out

# ========================= Trace 生成 =========================
def generate_trace_id(short: int | None = 8) -> str:
    """
    生成 Trace ID。
    - short: 指定短码长度（4~32），默认 8；传 None/False 则返回 32位hex 全量。
    """
    h = uuid.uuid4().hex  # 32 位十六进制
    if not short:
        return h
    n = max(4, min(int(short), 32))
    return h[:n]

def generate_trace_uuid() -> str:
    """返回 32 位 hex 全量 trace id。"""
    return uuid.uuid4().hex


# ========================= 基础 set/get =========================
def set_trace_context(user_id: int, trace_id: Optional[str] = None):
    """旧接口：设置 user/trace（短码），供历史调用链使用。"""
    user_id_var.set(user_id)
    trace_id_var.set(trace_id or generate_trace_id())

def get_trace_id() -> str:
    return (
        trace_id_var.get()
        or (_log_ctx["trace_id"].get() if "trace_id" in _log_ctx else None)
        or "unknown"
    )

def get_user_id() -> Optional[int]:
    """
    从日志上下文中获取 user_id（若存在且为正整数）
    """
    try:
        ctx = get_log_context() or {}
        uid = ctx.get("user_id")
        if isinstance(uid, int) and uid > 0:
            return uid
        if isinstance(uid, str) and uid.isdigit():
            return int(uid)
    except Exception:
        return None
    return None

def set_log_context(ctx: Dict[str, Any]) -> None:
    """
    批量写入上下文（支持旧键别名）；
    同时镜像到老变量，保证兼容。
    """
    norm = _normalize_incoming_ctx(ctx or {})
    # 若传入仅有旧的 task_id，则既写 task_id 又回填 trace_id（避免丢失）
    try:
        if ("task_id" in ctx) and ("trace_id" not in norm) and ctx.get("task_id"):
            norm["trace_id"] = str(ctx.get("task_id"))
    except Exception:
        pass
    for key, var in _log_ctx.items():
        if key in norm:
            try:
                var.set(norm[key])
            except Exception:
                pass
    # 镜像老变量
    try:
        if norm.get("trace_id"):
            trace_id_var.set(norm["trace_id"])
    except Exception:
        pass
    try:
        if "user_id" in norm and norm["user_id"] is not None:
            user_id_var.set(int(norm["user_id"]))
    except Exception:
        pass

def get_log_context() -> Dict[str, Any]:
    """
    返回非 None 的上下文字段（仅新标准键）。
    兼容：不会再回填 task_id/link/phone/session_name 等旧键。
    """
    ctx = {k: v.get() for k, v in _log_ctx.items() if v.get() is not None}
    tid = trace_id_var.get()
    if tid and "trace_id" not in ctx:
        ctx["trace_id"] = tid
    uid = user_id_var.get()
    if uid is not None and "user_id" not in ctx:
        ctx["user_id"] = uid
    return ctx or {}

# ========================= 注入/装饰器 =========================
def inject_trace_context(func_name: str, **extras: Any) -> None:
    """
    将 func/module/function/phase 等注入当前上下文。
    - 轻量避免外层 frame 强引用导致的 GC 难题。
    - 写入 extras 时应用别名归一化。
    """
    module_name: Optional[str] = None
    function_name: Optional[str] = None
    frame = inspect.currentframe()
    try:
        caller = frame.f_back if frame else None
        if caller:
            module_name = caller.f_globals.get("__name__")
            function_name = caller.f_code.co_name
    except Exception:
        module_name = module_name or "__unknown__"
        function_name = function_name or func_name
    finally:
        # 释放引用，避免循环引用导致的内存泄漏
        try:
            del frame, caller
        except Exception:
            pass

    current_tid = trace_id_var.get() or _log_ctx["trace_id"].get()
    base_ctx = {
        "trace_id": current_tid or generate_trace_id(),
        "func": func_name,
        "mod_name": module_name,
        "function": function_name or func_name,
    }
    if extras:
        base_ctx.update(_normalize_incoming_ctx(extras))
    set_log_context(base_ctx)

def with_trace(action_name: str = "", phase: str = ""):
    """装饰器：同步/异步通吃，自动注入 trace_id/func/module/function/phase（别名可传入）。"""
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                inject_trace_context(func_name=action_name or func.__name__, phase=phase)
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                inject_trace_context(func_name=action_name or func.__name__, phase=phase)
                return func(*args, **kwargs)
            return sync_wrapper
    return decorator

# ========================= 上下文复制运行/建Task =========================
def ctx_run(func: Callable[..., Any], *args, **kwargs):
    """
    在复制的 ContextVars 中运行任意可调用（同步或协程函数）。
    若是协程函数，返回协程对象（由上层 await）。
    """
    ctx = copy_context()
    return ctx.run(func, *args, **kwargs)

def ctx_run_sync(func: Callable[..., Any], *args, **kwargs) -> Any:
    """与 ctx_run 语义相同，但强调目标是同步函数；保持可读性。"""
    ctx = copy_context()
    return ctx.run(func, *args, **kwargs)

def ctx_create_task(
    coro_func: Callable[..., Awaitable] | Any,
    *args,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs,
) -> asyncio.Task:
    if inspect.iscoroutine(coro_func):
        raise TypeError(
            "ctx_create_task expected a callable, got a coroutine object. "
            "Call as ctx_create_task(fn, *args) instead of ctx_create_task(fn(*args))."
        )
    ctx = copy_context()
    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
    result = ctx.run(coro_func, *args, **kwargs)
    if not asyncio.iscoroutine(result):
        raise TypeError(
            f"ctx_create_task expected {coro_func!r} to return a coroutine, got {type(result).__name__}."
        )
    return loop.create_task(result)


# ========================= 临时覆盖上下文（自动恢复） =========================
@contextlib.contextmanager
def use_log_context(**pairs: Any):
    """
    with use_log_context(user_id=..., trace_id=..., me_phone=..., group=..., bot_name=...):
        ...
    写入时会对旧键别名做归一化（task_id→trace_id / link→group / phone→me_phone / session_name→bot_name）。
    退出时自动恢复原值。
    """
    norm = _normalize_incoming_ctx(pairs or {})
    tokens: Dict[str, contextvars.Token] = {}
    try:
        for k, v in norm.items():
            var = _log_ctx.get(k)
            if var is not None:
                tokens[k] = var.set(v)
        # 兼容老变量镜像（仅 trace_id / user_id）
        if "trace_id" in norm:
            tokens["__trace"] = trace_id_var.set(norm["trace_id"])
        if "user_id" in norm:
            tokens["__user"] = user_id_var.set(norm["user_id"])
        yield
    finally:
        for k, token in tokens.items():
            try:
                if k in ("__trace", "__user"):
                    token.var.reset(token)
                else:
                    _log_ctx[k].reset(token)  # type: ignore
            except Exception:
                pass

# ========================= 语义化便捷封装 =========================
@contextlib.contextmanager
def with_trace_ctx(
    user_id: Optional[int] = None,
    task_id: Optional[str] = None,
    phone: Optional[str] = None,
    group: Optional[str] = None,
    *,
    round: Optional[int] = None,
    index: Optional[int] = None,
    trace_id: Optional[str] = None,
    bot_name: Optional[str] = None,
):
    """
    统一日志上下文（推荐入口）：
    - 字段稳定：trace_id / task_id / user_id / me_phone / group / round / index
    - 兼容：phone → me_phone；未给 trace_id 则自动生成
    用法：
        with with_trace_ctx(uid, tid, phone, group, round=1, index=3):
            ...
    """
    ctx = {
        "user_id": user_id,
        "task_id": task_id,
        "trace_id": trace_id or generate_trace_id(),
        "me_phone": phone,
        "group": group,
        "round": round,
        "index": index,
        "bot_name": bot_name,
    }
    # 清理 None
    ctx = {k: v for k, v in ctx.items() if v is not None}
    with use_log_context(**ctx):
        yield