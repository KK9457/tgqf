# unified/context.py
# -*- coding: utf-8 -*-
"""
统一上下文管理器（避免循环依赖，集中注入/获取）

改进要点：
- 线程安全：所有 set/get/reset 操作加锁，防止并发竞争
- 键名校验：限制在已知键集合内，避免拼写错误静默失败
- 统一必需项获取：新增 ensure_* 系列（缺失即抛错），get_* 仍保留向后兼容（可能返回 None）
- 错误类型统一：ContextMissingError，错误信息更清晰
- 快照/探测：新增 has_context() 与 context_snapshot() 便于调试与健康检查
- 不引入项目级依赖（仅使用标准库），避免循环导入

对外 API 兼容：
- 原有 set_*/get_* 函数全部保留；仅内部实现优化
- get_scheduler/get_redis 仍在缺失时抛错（与旧行为一致）
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Dict
from threading import RLock

if TYPE_CHECKING:
    from scheduler.base_scheduler import BaseScheduler

__all__ = [
    # 原有 API（兼容）
    "set_fsm", "get_fsm",
    "set_white_user", "get_white_user",
    "set_task_control", "get_task_control",
    "set_health_checker", "get_health_checker",
    "set_client_manager", "get_client_manager",
    "set_bot", "get_bot",
    "set_scheduler", "get_scheduler",
    "set_reporter", "get_reporter",
    "set_reporter_factory", "get_reporter_factory",
    "set_context", "get_context",
    "reset_context",
    "set_redis", "get_redis",

    # 新增能力（可选使用）
    "ContextMissingError",
    "has_context",
    "unset_context",
    "context_snapshot",
    "ensure_fsm",
    "ensure_white_user",
    "ensure_task_control",
    "ensure_health_checker",
    "ensure_client_manager",
    "ensure_bot",
    "ensure_scheduler",
    "ensure_reporter",
    "ensure_reporter_factory",
    "ensure_redis",
    "set_context_bulk",
    "assert_has",
]

# -------------------- 锁与存储 --------------------
_LOCK = RLock()

# 允许的上下文键（避免拼写错误）
_KNOWN_KEYS = {
    "fsm",
    "white_user",
    "task_control",
    "health_checker",
    "client_manager",
    "bot",
    "redis",
    "reporter",
    "reporter_factory",
}

# 统一存储（值类型多样，不做强约束）
_CONTEXT: Dict[str, Any] = {k: None for k in _KNOWN_KEYS}

# Scheduler 单独持有（历史原因，保持兼容）
_scheduler: Optional["BaseScheduler"] = None


# -------------------- 异常 --------------------
class ContextMissingError(RuntimeError):
    """在需要的上下文未注入时抛出。"""
    pass


# -------------------- 工具函数（内部） --------------------
def _validate_key(key: str) -> None:
    if key not in _KNOWN_KEYS:
        raise KeyError(f"Unknown context key: {key!r}. Allowed: {sorted(_KNOWN_KEYS)}")

def _set(key: str, value: Any) -> None:
    _validate_key(key)
    with _LOCK:
        _CONTEXT[key] = value

def _get(key: str) -> Any:
    _validate_key(key)
    with _LOCK:
        return _CONTEXT.get(key)

def _require(key: str, err: str) -> Any:
    val = _get(key)
    if val is None:
        raise ContextMissingError(err)
    return val


# -------------------- Scheduler 专用（兼容原行为） --------------------
def set_scheduler(scheduler: "BaseScheduler") -> None:
    global _scheduler
    with _LOCK:
        _scheduler = scheduler

def get_scheduler() -> "BaseScheduler":
    with _LOCK:
        if _scheduler is None:
            raise ContextMissingError("❌ Scheduler 尚未初始化")
        return _scheduler

# ensure 版本
def ensure_scheduler() -> "BaseScheduler":
    return get_scheduler()


# -------------------- 通用 set/get --------------------
def set_context(key: str, value: Any) -> None:
    _set(key, value)

def get_context(key: str) -> Any:
    return _get(key)

def has_context(key: str) -> bool:
    _validate_key(key)
    with _LOCK:
        return _CONTEXT.get(key) is not None

def unset_context(key: str) -> None:
    _validate_key(key)
    with _LOCK:
        _CONTEXT[key] = None

def context_snapshot() -> Dict[str, Any]:
    with _LOCK:
        snap = dict(_CONTEXT)
        snap["_scheduler_set"] = _scheduler is not None
        return snap

def reset_context() -> None:
    with _LOCK:
        for key in _CONTEXT:
            _CONTEXT[key] = None
        global _scheduler
        _scheduler = None


# -------------------- 领域别名（保留旧 API） --------------------
def set_fsm(i: Any) -> None: _set("fsm", i)
def get_fsm() -> Any: return _get("fsm")
def ensure_fsm() -> Any: return _require("fsm", "❌ FSM 尚未注入")

def set_white_user(i: Any) -> None: _set("white_user", i)
def get_white_user() -> Any: return _get("white_user")
def ensure_white_user() -> Any: return _require("white_user", "❌ white_user 尚未注入")

def set_task_control(i: Any) -> None: _set("task_control", i)
def get_task_control() -> Any: return _get("task_control")
def ensure_task_control() -> Any: return _require("task_control", "❌ task_control 尚未注入")

def set_health_checker(i: Any) -> None: _set("health_checker", i)
def get_health_checker() -> Any: return _get("health_checker")
def ensure_health_checker() -> Any: return _require("health_checker", "❌ health_checker 尚未注入")

def set_client_manager(i: Any) -> None: _set("client_manager", i)
def get_client_manager() -> Any: return _get("client_manager")
def ensure_client_manager() -> Any: return _require("client_manager", "❌ client_manager 尚未注入")

def set_bot(i: Any) -> None: _set("bot", i)
def get_bot() -> Any: return _get("bot")
def ensure_bot() -> Any: return _require("bot", "❌ bot 尚未注入")

def set_reporter(i: Any) -> None: _set("reporter", i)
def get_reporter() -> Any: return _get("reporter")
def ensure_reporter() -> Any: return _require("reporter", "❌ reporter 尚未注入")

def set_reporter_factory(i: Any) -> None: _set("reporter_factory", i)
def get_reporter_factory() -> Any: return _get("reporter_factory")
def ensure_reporter_factory() -> Any: return _require("reporter_factory", "❌ reporter_factory 尚未注入")

def set_redis(i: Any) -> None: _set("redis", i)
def get_redis() -> Any: return _require("redis", "❌ Redis 实例尚未注入")
def ensure_redis() -> Any: return get_redis()

# -------------------- 便捷 API --------------------
def set_context_bulk(values: Dict[str, Any]) -> None:
    """批量注入（受 _KNOWN_KEYS 校验）。"""
    if not values:
        return
    for k, v in values.items():
        _set(k, v)

def assert_has(*keys: str) -> None:
    """一次性断言多项上下文已注入，缺哪个报哪个。"""
    missing = [k for k in keys if not has_context(k)]
    if missing:
        raise ContextMissingError(f"❌ 缺少上下文: {', '.join(missing)}")
