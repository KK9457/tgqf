# -*- coding: utf-8 -*-
# scheduler/ratelimit.py
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, Any, Optional
from contextlib import asynccontextmanager

from unified.logger import log_debug, log_info, log_warning
from unified.trace_context import get_log_context
from core.event_bus import bus

__all__ = [
    "SpacingLimiter",
    "CooldownMap",
    "FloodController",
]

# ---- time helpers ----
def _now_monotonic() -> float:
    return time.monotonic()

def _now_unix() -> float:
    return time.time()


# =========================
# SpacingLimiter
# =========================
@dataclass
class SpacingLimiter:
    """
    确保两次 wait() 之间至少等待 [lo, hi] 的随机秒数（含轻微抖动）。
    - 协程安全：内部自锁，避免多并发竞争更新 _last。
    - 首次调用：若 _last=0，则不会强制延迟（与现有逻辑兼容）。
    """
    window: Tuple[float, float]  # (lo, hi) in seconds
    _last: float = field(default=0.0)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def _norm_window(self, w: Tuple[float, float]) -> Tuple[float, float]:
        lo, hi = float(w[0]), float(w[1])
        lo = max(0.0, lo)
        hi = max(lo, hi if hi > 0 else lo)
        if hi == 0.0:
            hi = 0.001
        return lo, hi

    async def wait(self) -> float:
        """等待直到满足当前窗口的随机间隔，返回实际 sleep 秒数。"""
        async with self._lock:
            lo, hi = self._norm_window(self.window)
            target = random.uniform(lo, hi)
            elapsed = _now_monotonic() - self._last
            sleep_for = target - elapsed
            if sleep_for > 0:
                try:
                    await asyncio.sleep(sleep_for)
                except asyncio.CancelledError:
                    # 保持 _last 不更新，外部可重试
                    raise
            # 标记“上次完成时刻”
            self._last = _now_monotonic()
            return max(0.0, sleep_for)

    def set_window(self, window: Tuple[float, float]) -> None:
        """动态调整窗口（用于惩罚/恢复）。"""
        self.window = self._norm_window(window)

    def tick_now(self) -> None:
        """标记“刚完成一次发送”，用于跃迁到下次间隔开始。"""
        self._last = _now_monotonic()


# =========================
# CooldownMap
# =========================
@dataclass
class CooldownMap:
    """
    记录 key 的冷却截止时间（unix 秒）。
    - remaining(key): 剩余秒数（<=0 表示不过冷却）
    - wait_if_needed(key): 等待剩余秒数，并加轻微抖动
    - set_cooldown(key, seconds): 从现在起设置 seconds 的冷却
    """
    _map: Dict[str, float] = field(default_factory=dict)

    def remaining(self, key: Optional[str]) -> float:
        if not key:
            return 0.0
        left = self._map.get(key, 0.0) - _now_unix()
        return left if left > 0 else 0.0

    async def wait_if_needed(self, key: Optional[str]) -> float:
        left = self.remaining(key)
        if left > 0:
            jitter = random.uniform(0.2, 0.8)
            try:
                await asyncio.sleep(left + jitter)
            except asyncio.CancelledError:
                raise
            return left + jitter
        return 0.0

    def set_cooldown(self, key: Optional[str], seconds: float) -> None:
        if not key or seconds <= 0:
            return
        self._map[key] = _now_unix() + float(seconds)

    def set_until(self, key: Optional[str], until_unix_ts: float) -> None:
        if not key:
            return
        self._map[key] = float(until_unix_ts)

    def clear(self, key: Optional[str]) -> None:
        if key and key in self._map:
            self._map.pop(key, None)

    def prune_expired(self) -> None:
        now = _now_unix()
        dead = [k for k, ts in self._map.items() if ts <= now]
        for k in dead:
            self._map.pop(k, None)


# =========================
# 内部状态：Phone / Chat
# =========================
@dataclass
class _PhoneState:
    amp: float = 1.0                # 放大因子（>1 越慢；≈1 正常）
    last_at: float = 0.0            # 上次发送时间点（秒）
    cooldown_until: float = 0.0     # 因 FloodWait/数量上限触发的整机冷却
    base_delay: float = 0.5         # 单条基准延迟（秒）
    max_delay: float = 8.0          # 单条最大延迟（秒）
    success_smooth: float = 0.0     # 指数平滑成功数（本轮）
    fail_smooth: float = 0.0        # 指数平滑失败数（本轮）


@dataclass
class _ChatState:
    slowmode_until: float = 0.0
    cooldown_until: float = 0.0     # peer_flood/写禁等触发的 chat 冷却
    last_at: float = 0.0


# =========================
# FloodController
# =========================
class FloodController:
    """
    全局 + 每账号 + 每 chat 的节流与冷却。
    - global_spacing: 全局请求间隔（如 1.2~2.7s，仅做薄节流）
    - per_phone_spacing: 每账号请求间隔（如 3.0~8.0s，主节流）
    - slowmode: 每 chat 的慢速模式节流（由 SlowModeWaitError 动态设置）
    - cooldown_phone/chat: Flood/PeerFlood 后的长短期冷却
    - 惩罚期：接收到 FLOOD_WAIT 事件后，临时放大该账号的 per_phone 间隔
    """

    def __init__(
        self,
        global_spacing: Tuple[float, float] = (1.2, 2.7),
        per_phone_spacing: Tuple[float, float] = (3.0, 8.0),
        *,
        bound_user_ids: Optional[set[int]] = None,
        base_delay: float = 0.5,
        max_delay: float = 8.0,
        adj_success: float = 0.02,   # 成功使 amp 向 1.0 收敛
        adj_fail: float = 0.10,      # 失败放大
        round_recovery: float = 0.15,# 轮次收敛强度（越小回归越慢）
    ) -> None:
        self._per_phone_window: Tuple[float, float] = tuple(per_phone_spacing)
        self._bound_user_ids = set(bound_user_ids or [])

        self.global_spacing = SpacingLimiter(global_spacing)
        self.per_phone_spacing_map: Dict[str, SpacingLimiter] = {}

        self.cooldown_phone = CooldownMap()
        self.cooldown_chat = CooldownMap()
        self.slowmode = CooldownMap()  # 用“冷却”模型实现每 chat 的下一次允许时间

        self._phone_semaphores: Dict[str, asyncio.Semaphore] = {}  # 单账号串行

        self._phones: Dict[str, _PhoneState] = {}
        self._chats: Dict[str, _ChatState] = {}
        self._base_delay = max(0.0, float(base_delay))
        self._max_delay = max(0.1, float(max_delay))
        self._adj_success = max(0.0, float(adj_success))
        self._adj_fail = max(0.0, float(adj_fail))
        self._round_recovery = max(0.0, float(round_recovery))
        # —— 惩罚期管理（phone -> 到期时间 / 倍率）—— #
        self._penalty_until: Dict[str, float] = {}
        self._penalty_factor: Dict[str, float] = {}
        self._lock = asyncio.Lock()

        # 订阅总线：FLOOD_WAIT → 放大该账号节流；RECOVERED → 恢复默认
        self._register_bus_handlers()

    # ----------------- 公共：发送前等待（amp/慢速/冷却版） -----------------
    async def throttle(self, phone: str, chat_key: str) -> None:
        """发送前统一等待；考虑 phone 全局冷却、chat 冷却/慢速、amp 动态放大"""
        now = time.time()
        async with self._lock:
            ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
            cs = self._chats.setdefault(chat_key, _ChatState())

            # 1) 账号级冷却（FloodWait / ChannelsTooMuch）
            if ps.cooldown_until > now:
                wait = ps.cooldown_until - now
                log_warning("phone_cooldown_sleep", extra={"phone": phone, "wait_s": round(wait, 2), **(get_log_context() or {})})
                await self._sleep_unlock(wait)

            # 2) chat 级冷却/慢速
            now = time.time()
            if cs.cooldown_until > now:
                wait = cs.cooldown_until - now
                log_warning("chat_cooldown_sleep", extra={"chat_key": chat_key, "wait_s": round(wait, 2), **(get_log_context() or {})})
                await self._sleep_unlock(wait)

            now = time.time()
            if cs.slowmode_until > now:
                wait = cs.slowmode_until - now
                log_warning("chat_slowmode_sleep", extra={"chat_key": chat_key, "wait_s": round(wait, 2), **(get_log_context() or {})})
                await self._sleep_unlock(wait)

            # 3) 速率限制（以 phone 为主的发条节流；chat 也参考）
            slot_delay = min(ps.max_delay, max(ps.base_delay, ps.base_delay * ps.amp))
            wait_phone = max(0.0, (ps.last_at + slot_delay) - time.time())
            wait_chat = max(0.0, (cs.last_at + slot_delay * 0.5) - time.time())
            wait = max(wait_phone, wait_chat)

        if wait > 0:
            await asyncio.sleep(wait)

        async with self._lock:
            now = time.time()
            ps.last_at = now
            cs.last_at = now

    # 兼容旧名
    acquire = throttle

    # ----------------- 事件总线注册 -----------------
    def _register_bus_handlers(self) -> None:
        async def _on_flood_wait(evt: Dict[str, Any]):
            try:
                if not isinstance(evt, dict) or (evt.get("type") or "").upper() != "FLOOD_WAIT":
                    return
                uids = evt.get("user_ids")
                if self._bound_user_ids and isinstance(uids, (list, set, tuple)) and not (self._bound_user_ids & set(int(x) for x in uids)):
                    return
                phone = str(evt.get("phone") or "")
                seconds = int(evt.get("seconds") or 0)
                if not phone or seconds <= 0:
                    return
                # seconds 越大，倍率越高，但限制在 [1.5, 3.0]
                factor = max(1.5, min(3.0, 1.0 + (seconds / 60.0) ** 0.6))
                duration = max(90, min(1800, seconds * 2))
                self._apply_phone_penalty(phone, factor, duration)
            except Exception:
                pass

        async def _on_recovered(evt: Dict[str, Any]):
            try:
                if not isinstance(evt, dict) or (evt.get("type") or "").upper() != "RECOVERED":
                    return
                uids = evt.get("user_ids")
                if self._bound_user_ids and isinstance(uids, (list, set, tuple)) and not (self._bound_user_ids & set(int(x) for x in uids)):
                    return
                phone = str(evt.get("phone") or "")
                if not phone:
                    return
                self._reset_phone_penalty(phone)
            except Exception:
                pass

        try:
            bus.register("FLOOD_WAIT", _on_flood_wait)
            bus.register("RECOVERED", _on_recovered)
        except Exception:
            pass

    # ---- helpers ----
    def _sem_for_phone(self, phone: str) -> asyncio.Semaphore:
        sem = self._phone_semaphores.get(phone)
        if not sem:
            sem = self._phone_semaphores[phone] = asyncio.Semaphore(1)
        return sem

    def _spacing_for_phone(self, phone: str) -> SpacingLimiter:
        sl = self.per_phone_spacing_map.get(phone)
        if not sl:
            sl = self.per_phone_spacing_map.setdefault(phone, SpacingLimiter(self._per_phone_window))
        return sl

    # ---- 惩罚/恢复 ----
    def _apply_phone_penalty(self, phone: str, factor: float, duration_sec: int) -> None:
        now = _now_unix()
        self._penalty_until[phone] = now + max(1, int(duration_sec))
        self._penalty_factor[phone] = max(1.0, float(factor))
        # 立刻放大窗口（影响后续 wait）
        base_lo, base_hi = self._per_phone_window
        self._spacing_for_phone(phone).set_window((base_lo * factor, base_hi * factor))

    def _reset_phone_penalty(self, phone: str) -> None:
        self._spacing_for_phone(phone).set_window(self._per_phone_window)
        self._penalty_until.pop(phone, None)
        self._penalty_factor.pop(phone, None)

    def _decay_phone_penalty_if_expired(self, phone: str) -> None:
        exp = self._penalty_until.get(phone, 0.0)
        if exp and _now_unix() > exp:
            self._reset_phone_penalty(phone)

    # ----------------- 事件：成功/失败/轮次收敛 -----------------
    def on_success(self, phone: str, chat_key: str) -> None:
        ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
        before = ps.amp
        ps.amp = max(1.0, before - (before - 1.0) * self._adj_success)  # 指数式收敛
        ps.success_smooth = ps.success_smooth * 0.8 + 1.0
        log_debug("flood_on_success", extra={"phone": phone, "amp": round(ps.amp, 3), **(get_log_context() or {})})

    def on_fail(self, phone: str, chat_key: str) -> None:
        ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
        before = ps.amp
        ps.amp = min(6.0, before + self._adj_fail + (before - 1.0) * 0.05)
        ps.fail_smooth = ps.fail_smooth * 0.8 + 1.0
        log_debug("flood_on_fail", extra={"phone": phone, "amp": round(ps.amp, 3), **(get_log_context() or {})})

    def on_round_complete(self, phone: str, success_delta: int, total: int) -> None:
        ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
        rate = float(success_delta) / max(1, int(total or 0))
        k = self._round_recovery * (0.5 + rate)  # 低到高：0.075 ~ 0.3（默认）
        before = ps.amp
        ps.amp = max(1.0, before - (before - 1.0) * k)
        ps.success_smooth *= 0.5
        ps.fail_smooth *= 0.5
        log_info("flood_on_round_complete", extra={"phone": phone, "rate": round(rate, 3), "amp": round(ps.amp, 3), **(get_log_context() or {})})

    # ---- 发送前/后（提供“强串行 + 全局/账号节流”风格，供需要者使用） ----
    async def before_send(self, phone: str, chat_key: str) -> None:
        """
        串行化指定 phone 的发送，并按顺序执行：
        1) 惩罚期衰减检查
        2) 账号/会话冷却 & 慢速模式等待
        3) 全局 & 账号级节流
        """
        sem = self._sem_for_phone(phone)
        await sem.acquire()
        try:
            # 1) 惩罚有效期衰减
            self._decay_phone_penalty_if_expired(phone)
            # 2) 冷却与慢速
            await self.cooldown_phone.wait_if_needed(phone)
            if chat_key:
                await self.cooldown_chat.wait_if_needed(chat_key)
                await self.slowmode.wait_if_needed(chat_key)
            # 3) 节流（全局 + 账号）
            await self.global_spacing.wait()
            await self._spacing_for_phone(phone).wait()
        except asyncio.CancelledError:
            try:
                sem.release()
            except Exception:
                pass
            raise
        except Exception:
            try:
                sem.release()
            except Exception:
                pass
            raise

    def after_send(self, phone: str) -> None:
        """释放账号级串行信号量。"""
        self._sem_for_phone(phone).release()

    @asynccontextmanager
    async def sending(self, phone: str, chat_key: str):
        await self.before_send(phone, chat_key)
        try:
            yield
        finally:
            self.after_send(phone)

    # ---- 事件钩子（供 Runner/上层调用）----
    def on_floodwait(self, phone: str, seconds: int) -> None:
        ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
        ps.cooldown_until = max(ps.cooldown_until, time.time() + max(0, int(seconds)))
        ps.amp = min(8.0, ps.amp + 0.3)
        log_warning("floodwait_registered", extra={"phone": phone, "seconds": seconds, "amp": round(ps.amp, 3), **(get_log_context() or {})})

    def on_peer_flood(self, phone: str, chat_key: str, *, hours: int) -> None:
        cs = self._chats.setdefault(chat_key, _ChatState())
        cs.cooldown_until = max(cs.cooldown_until, time.time() + max(0, hours * 3600))
        ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
        ps.amp = min(6.0, ps.amp + 0.2)

    def on_slowmode(self, chat_key: str, seconds: int) -> None:
        cs = self._chats.setdefault(chat_key, _ChatState())
        cs.slowmode_until = max(cs.slowmode_until, time.time() + max(0, int(seconds)))
        log_warning("slowmode_registered", extra={"chat_key": chat_key, "seconds": seconds, **(get_log_context() or {})})

    def on_channels_too_much(self, phone: str, *, hours: int) -> None:
        ps = self._phones.setdefault(phone, _PhoneState(base_delay=self._base_delay, max_delay=self._max_delay))
        ps.cooldown_until = max(ps.cooldown_until, time.time() + max(0, hours * 3600))
        ps.amp = min(8.0, ps.amp + 0.5)
        log_warning("channels_too_much_registered", extra={"phone": phone, "hours": hours, "amp": round(ps.amp, 3), **(get_log_context() or {})})

    # ---- 观测/调试辅助 ----
    def get_effective_phone_window(self, phone: str) -> Tuple[float, float]:
        """获取当前 phone 的实际节流窗口（考虑惩罚后的即时状态）。"""
        sl = self._spacing_for_phone(phone)
        return sl.window

    def get_penalty_info(self, phone: str) -> Optional[Tuple[float, float]]:
        """返回 (penalty_until_ts, factor)；若无惩罚返回 None。"""
        if phone in self._penalty_until:
            return self._penalty_until.get(phone, 0.0), self._penalty_factor.get(phone, 1.0)
        return None

    # —— 维护：周期清理过期 cooldown —— #
    def prune(self) -> None:
        try:
            self.cooldown_phone.prune_expired()
            self.cooldown_chat.prune_expired()
            self.slowmode.prune_expired()
        except Exception:
            pass

    # ----------------- 内部 -----------------
    async def _sleep_unlock(self, seconds: float) -> None:
        # 锁外 sleep，避免阻塞其他 phone/chat 的推进
        await asyncio.sleep(max(0.0, seconds))
