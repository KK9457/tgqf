# -*- coding: utf-8 -*-
# scheduler/client_validator.py
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional, TypeAlias, List

# 统一走核心错误桥接，避免直接依赖 telethon.errors 细节（降版本兼容风险）
from core.telethon_errors import TE
from typess.health_types import HealthState
from unified.config import FLOOD_WAIT_KEY_PREFIX, FLOOD_RESTORATION_INTERVAL
from unified.logger import log_debug, log_exception, log_info, log_warning
from unified.trace_context import get_log_context, set_log_context
from unified.context import get_client_manager
from core.event_bus import bus

RedisT: TypeAlias = Any


# -------------------- 工具函数 --------------------
async def _maybe_await(fn: Optional[Callable[..., Any]], *args, **kwargs) -> None:
    """兼容 sync/async 回调；静默吞错，避免影响主流程。"""
    if not callable(fn):
        return
    try:
        res = fn(*args, **kwargs)
        if asyncio.iscoroutine(res):
            await res
    except Exception:
        # 回调异常不传播
        pass


def _decode_redis(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8", errors="ignore")
        except Exception:
            return None
    return str(val)


def _is_unauthorized_exc(e: Exception) -> bool:
    s = (str(e) or "").lower()
    if "unauthorized" in s or "未授权" in s:
        return True
    try:
        return isinstance(e, (TE.UnauthorizedError, getattr(TE, "AuthKeyUnregisteredError", tuple())))
    except Exception:
        return False


def _collect_flood_errors():
    """
    动态收集 Telethon 的 Flood* 异常类，构造纯“异常类”的元组，避免 getattr 返回 tuple() 造成 TypeError。
    """
    base = []
    try:
        from telethon import errors as te_errors
        if hasattr(te_errors, "FloodWaitError") and isinstance(te_errors.FloodWaitError, type):
            base.append(te_errors.FloodWaitError)
    except Exception:
        pass
    try:
        for name in ("FloodWaitError", "FloodPremiumWaitError", "FloodTestPhoneWaitError"):
            cls = getattr(TE, name, None)
            if isinstance(cls, type):
                base.append(cls)
    except Exception:
        pass
    # 去重并仅保留类型
    return tuple({c for c in base if isinstance(c, type)})


FLOOD_ERRORS = _collect_flood_errors()


# -------------------- 校验器主体 --------------------
class ClientValidator:
    """
    账号可用性校验器：
    - 并发校验连接/授权（concurrency_limit 与全局一致）
    - 冷却跳过（FloodWait）并自动标记/恢复
    - 健康联动（health_checker 可选）
    - 回调 on_validate 支持 sync/async，签名 on_validate(phone: str, ok: bool) -> None
    """

    def __init__(
        self,
        redis: Optional[RedisT],
        fsm: Any = None,
        health_checker: Any = None,
        notifier: Any = None,
    ):
        self.redis = redis
        self.fsm = fsm
        self.health_checker = health_checker
        self.notifier = notifier

    # ---------- 冷却存取 ----------
    def _cooldown_key(self, phone: str) -> str:
        return f"{FLOOD_WAIT_KEY_PREFIX}{phone}"

    async def _broadcast_floodwait(self, phone: str, seconds: int) -> None:
        """统一 FLOOD_WAIT 事件派发与（必要时的）外部通知。"""
        try:
            manager = get_client_manager()
            user_ids = list((getattr(manager, "get_clients_by_phone")(phone) or {}).keys())
        except Exception:
            user_ids = []
        try:
            await bus.dispatch(
                "FLOOD_WAIT",
                {
                    "type": "FLOOD_WAIT",
                    "phone": phone,
                    "seconds": int(seconds),
                    "user_ids": user_ids,
                },
            )
        except Exception:
            pass

    async def _broadcast_banned(self, phone: str) -> None:
        """统一 BANNED 事件派发。"""
        try:
            manager = get_client_manager()
            user_ids = list((getattr(manager, "get_clients_by_phone")(phone) or {}).keys())
        except Exception:
            user_ids = []
        try:
            await bus.dispatch(
                "BANNED", {"type": "BANNED", "phone": phone, "user_ids": user_ids}
            )
        except Exception:
            pass

    async def _mark_flood_wait(self, phone: str, seconds: int) -> None:
        seconds = max(1, int(seconds or 0))
        # === CHANGED: 冷却真源 → HealthChecker/FSM ===
        try:
            manager = get_client_manager()
            hc = getattr(manager, "health_checker", None)
        except Exception:
            hc = None

        if hc:
            try:
                hc._set_state(phone, HealthState.FLOOD_WAIT)
                await hc._try_set_cooldown(phone, seconds)
                await hc._broadcast_floodwait(phone, seconds)
                log_warning("⏱️ 标记 Flood 冷却（via HealthChecker）", extra={"phone": phone, "seconds": seconds, **(get_log_context() or {})})
                return
            except Exception:
                pass

        # Fallback：保持原有行为（Redis 轨迹 + 事件）
        try:
            if self.redis:
                expire_at = int(time.time()) + seconds
                await self.redis.set(self._cooldown_key(phone), expire_at, ex=seconds)
        except Exception:
            pass
        log_warning("⏱️ 标记 Flood 冷却（fallback Redis）", extra={"phone": phone, "seconds": seconds, **(get_log_context() or {})})
        await self._broadcast_floodwait(phone, seconds)

    async def _fsm_cooldown_remaining(self, phone: str) -> Optional[int]:
        """优先从 FSM 读取冷却剩余秒数，接口兼容若干实现。"""
        try:
            manager = get_client_manager()
            fsm = getattr(manager, "fsm", None)
            if not fsm:
                return None
            # 兼容实现 A：get_cooldown_remaining(phone)
            if hasattr(fsm, "get_cooldown_remaining"):
                left = await fsm.get_cooldown_remaining(phone)
                return int(left or 0)
            # 兼容实现 B：get_cooldown(phone) -> {"remain": int} / timestamp
            if hasattr(fsm, "get_cooldown"):
                cd = await fsm.get_cooldown(phone)
                if isinstance(cd, dict) and "remain" in cd:
                    return int(cd["remain"] or 0)
                if isinstance(cd, (int, float)):
                    # 可能返回过期时间戳
                    return max(0, int(cd - time.time()))
        except Exception:
            return None
        return None

    async def _is_under_cooldown(self, phone: str) -> bool:
        # === CHANGED: 优先 FSM ===
        left = await self._fsm_cooldown_remaining(phone)
        if left is not None:
            return left > 0
        # Fallback Redis
        if not self.redis:
            return False
        try:
            expire_ts = await self.redis.get(self._cooldown_key(phone))
            s = _decode_redis(expire_ts)
            return bool(s and int(s) > int(time.time()))
        except Exception:
            return False

    async def get_cooldown_remaining(self, phone: str) -> int:
        # === CHANGED: 优先 FSM ===
        left = await self._fsm_cooldown_remaining(phone)
        if left is not None:
            return max(0, int(left))
        # Fallback Redis
        if not self.redis:
            return 0
        try:
            expire_ts = await self.redis.get(self._cooldown_key(phone))
            s = _decode_redis(expire_ts)
            left = (int(s) - int(time.time())) if s else 0
            return max(0, left)
        except Exception:
            return 0

    async def auto_restore_flood_clients(self, user_id: int, client_manager) -> None:
        """
        尝试对已过冷却期的账号进行重连与健康检查，并广播恢复事件。
        """
        # === CHANGED: 若有 HealthChecker → 直接委托，避免重复实现 ===
        try:
            hc = getattr(get_client_manager(), "health_checker", None)
            if hc and hasattr(hc, "auto_reconnect_failed"):
                await hc.auto_reconnect_failed()
                return
        except Exception:
            pass

        # Fallback: 原逻辑
        clients = client_manager.get_clients_for_user(user_id) or {}
        for phone in list(clients.keys()):
            if not await self._is_under_cooldown(phone):
                try:
                    await client_manager.try_create_and_register(user_id, phone)
                    new_client = client_manager.get_client(user_id, phone)
                    log_info("🔁 尝试重连 Flood 账号", extra={"phone": phone, **(get_log_context() or {})})
                    if new_client and self.health_checker:
                        try:
                            await self.health_checker.perform_health_check(phone, new_client)
                        except Exception:
                            pass
                    try:
                        manager = get_client_manager()
                        user_ids = list((getattr(manager, "get_clients_by_phone")(phone) or {}).keys())
                    except Exception:
                        user_ids = [user_id]
                    try:
                        await bus.dispatch("RECOVERED", {"type": "RECOVERED", "phone": phone, "user_ids": user_ids})
                    except Exception:
                        pass
                except Exception as e:
                    log_warning("🔁 自动重连失败", extra={"phone": phone, "error": str(e), **(get_log_context() or {})})

async def flood_restoration_loop(user_ids: List[int], validator: ClientValidator, client_manager) -> None:
    """
    简单轮询恢复：每 60s 检查一轮。
    - 支持取消：外部取消任务时可及时退出
    """
    try:
        while True:
            for uid in user_ids or []:
                try:
                    await validator.auto_restore_flood_clients(uid, client_manager)
                except Exception as e:
                    log_warning("flood_restoration_loop 异常", extra={"user_id": uid, "error": str(e)})
            await asyncio.sleep(FLOOD_RESTORATION_INTERVAL)
    except asyncio.CancelledError:
        return
