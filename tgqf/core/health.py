# -*- coding: utf-8 -*-
# core/health.py
from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from telethon import TelegramClient, errors as te_errors

from typess.health_types import HealthState
from typess.message_enums import TaskStatus
from unified.logger import log_debug, log_info, log_warning
from unified.trace_context import (
    ctx_create_task,
    generate_trace_id,
    get_log_context,
    set_log_context,
)
from core.telethon_errors import (
     TE,
     detect_flood_seconds,
     classify_telethon_error,     # 返回 (HealthState, meta)
 )

from core.event_bus import bus
from core.task_status_reporter import TaskStatusReporter

HealthCallbackType = Callable[[str, HealthState], Any]

# —— 可调超时与并发 —— #
CHECK_TIMEOUT_AUTH = 6
CHECK_TIMEOUT_GETME = 8
HEALTHCHECK_CONCURRENCY = 8


def _collect_flood_errors():
    """
    动态收集 Flood 系列异常类，返回纯“异常类”的元组，避免 except 中混入非类型对象。
    """
    base = []
    try:
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
    return tuple({c for c in base if isinstance(c, type)})


FLOOD_ERRORS = _collect_flood_errors()


# ---------- 小工具 ----------
async def _maybe_await(fn: Optional[Callable[..., Any]], *args, **kwargs) -> None:
    if not callable(fn):
        return
    try:
        res = fn(*args, **kwargs)
        if asyncio.iscoroutine(res):
            await res
    except Exception:
        # 回调失败不可影响主流程
        pass

class HealthChecker:
    """
    统一账号健康检查与联动：
    - 定期检查连接/授权，写入 HealthState
    - 遇到 FLOOD_WAIT / 非健康（BANNED/AUTH_EXPIRED/NETWORK_ERROR）自动暂停任务；恢复 OK 自动恢复任务并可拉起调度
    - 支持 notifier / 自定义回调（均支持 sync/async）
    - 与调度/验证模块的字段和方法保持一致（get_task_status、pause_task、resume_task 等）
    """

    def __init__(self, manager):
        """
        manager 建议暴露：
          - get_health_state_legacy(phone) -> HealthState | None
          - update_status(phone, HealthState) -> None
          - redis_index.get_user_id(phone) -> int | None
          - fsm: get_task_status/pause_task/resume_task/clear_cooldown/set_cooldown
          - scheduler: start_user_task(user_id)
          - get_all_clients / ensure_connected
        """
        self.manager = manager
        self.state_map: Dict[str, HealthState] = getattr(manager, "_health_states", {})
        self.state_callbacks: Dict[str, HealthCallbackType] = {}
        self._recover_callbacks: List[Callable[[str, Optional[int]], Any]] = []

        self.notifier: Optional[Any] = None
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None

        try:
            set_log_context({"function": "HealthChecker.__init__", "phase": "init"})
        except Exception:
            pass
        log_debug("✅ HealthChecker 初始化完成", extra=get_log_context())

    # ---------- Notifier / Callbacks ----------
    def set_notifier(self, notifier) -> None:
        self.notifier = notifier

    def register_callback(self, phone: str, callback: HealthCallbackType) -> None:
        self.state_callbacks[phone] = callback

    def register_on_recover_callback(
        self, cb: Callable[[str, Optional[int]], Any]
    ) -> None:
        """注册恢复健康时的回调：参数为 (phone, user_id)。"""
        self._recover_callbacks.append(cb)

    # ---------- State helpers ----------
    def get_state(self, phone: str) -> Optional[HealthState]:
        try:
            if hasattr(self.manager, "get_health_state_legacy"):
                return self.manager.get_health_state_legacy(phone)
            return self.manager.get_health_state(phone)  # type: ignore
        except Exception:
            return None

    def _safe_state(self, new_state: Union[HealthState, str]) -> HealthState:
        try:
            return HealthState.parse(new_state)
        except Exception:
            return HealthState.UNKNOWN

    def _set_state(
        self,
        phone: str,
        new_state: Union[HealthState, str],
        *,
        notify: bool = True,
        origin: Optional[str] = None,
    ) -> None:
        ns = self._safe_state(new_state)
        set_log_context({
            "function": "HealthChecker._set_state",
            "phone": phone,
            "phase": "health_state_change",
            "trace_id": generate_trace_id(),
            "notify": notify,
            "origin": origin or "",
        })
        ctx_create_task(self._apply_state_change, phone, ns, notify, origin)

    # 公共冷却接口（供外部统一调用）
    async def set_cooldown(self, phone: str, seconds: int) -> None:
        await self._try_set_cooldown(phone, int(seconds or 0))

    async def _broadcast_floodwait(self, phone: str, seconds: int) -> None:
        """统一 FLOOD_WAIT 事件派发与 UI 通知。"""
        try:
            user_ids = []
            if hasattr(self.manager, "get_clients_by_phone"):
                user_ids = list((self.manager.get_clients_by_phone(phone) or {}).keys())
        except Exception:
            user_ids = []
        await bus.dispatch(
            "FLOOD_WAIT",
            {
                "type": "FLOOD_WAIT",
                "phone": phone,
                "seconds": int(seconds or 0),
                "user_ids": user_ids,
            },
        )
        if user_ids:
            rep = TaskStatusReporter(
                redis_client=getattr(getattr(self.manager, "fsm", None), "redis", None)
            )
            for uid in user_ids:
                rep.set_user(uid)
                await rep.notify_flood_wait(uid, phone, int(seconds or 0))

    async def _broadcast_banned(self, phone: str) -> None:
        """统一 BANNED 事件派发与 UI 通知。"""
        try:
            user_ids = []
            if hasattr(self.manager, "get_clients_by_phone"):
                user_ids = list((self.manager.get_clients_by_phone(phone) or {}).keys())
        except Exception:
            user_ids = []
        await bus.dispatch(
            "BANNED", {"type": "BANNED", "phone": phone, "user_ids": user_ids}
        )
        if user_ids:
            rep = TaskStatusReporter(
                redis_client=getattr(getattr(self.manager, "fsm", None), "redis", None)
            )
            for uid in user_ids:
                rep.set_user(uid)
                await rep.notify_banned(uid, phone)

    async def _apply_state_change(
        self,
        phone: str,
        new_state: HealthState,
        notify: bool = True,
        origin: Optional[str] = None,
    ) -> None:
        ctx = get_log_context() or {}
        trace_id = ctx.get("trace_id") or generate_trace_id()
        set_log_context({
            "function": "HealthChecker._apply_state_change",
            "phone": phone,
            "trace_id": trace_id,
            "notify": notify,
            "origin": origin or "",
        })

        # 读取旧状态
        try:
            if hasattr(self.manager, "get_health_state_legacy"):
                old_state = self.manager.get_health_state_legacy(phone)
            else:
                old_state = self.manager.get_health_state(phone)  # type: ignore
        except Exception:
            old_state = None

        # 恢复回调（仅在旧状态不健康 -> OK 时触发）
        unhealthy_set = {
            HealthState.BANNED,
            HealthState.AUTH_EXPIRED,
            HealthState.NETWORK_ERROR,
            HealthState.FLOOD_WAIT,
        }
        if old_state in unhealthy_set and new_state == HealthState.OK:
            user_id = None
            try:
                redis_index = getattr(self.manager, "redis_index", None)
                if redis_index and hasattr(redis_index, "get_user_id"):
                    user_id = await redis_index.get_user_id(phone)
            except Exception:
                user_id = None
            for cb in self._recover_callbacks:
                await _maybe_await(cb, phone, user_id)

        # 写入状态（以 manager 为真源）
        try:
            if hasattr(self.manager, "update_status"):
                self.manager.update_status(phone, new_state)
        except Exception:
            pass

        # 日志
        try:
            emoji = new_state.emoji() if hasattr(new_state, "emoji") else ""
            zh = new_state.zh_name() if hasattr(new_state, "zh_name") else str(new_state)
            log_info(f"📶 状态更新: {phone} {old_state} → {new_state} {emoji} {zh}", extra=get_log_context())
        except Exception:
            log_info(f"📶 状态更新: {phone} {old_state} → {new_state}", extra=get_log_context())

        # 通知器：受 notify 控制
        if notify and self.notifier and hasattr(self.notifier, "notify"):
            try:
                await _maybe_await(self.notifier.notify, phone, new_state)
            except Exception as e:
                log_warning(f"⚠️ 通知器执行异常: {e}", extra=get_log_context())

        # 联动任务/恢复通知
        try:
            await self._handle_state_change(phone, old_state, new_state, notify=notify, origin=origin)
        except Exception as e:
            log_warning(f"⚠️ 状态处理异常: {e}", extra=get_log_context())

        # 单号回调
        cb = self.state_callbacks.get(phone)
        if cb:
            await _maybe_await(cb, phone, new_state)

    async def _handle_state_change(
        self,
        phone: str,
        old: Optional[HealthState],
        new: HealthState,
        *,
        notify: bool = True,
        origin: Optional[str] = None,
    ) -> None:
        set_log_context({
            "function": "HealthChecker._handle_state_change",
            "phone": phone,
            "phase": "handle_state_change",
            "notify": notify,
            "origin": origin or "",
        })
        redis_index = getattr(self.manager, "redis_index", None)
        fsm = getattr(self.manager, "fsm", None)
        scheduler = getattr(self.manager, "scheduler", None)

        if not (redis_index and fsm):
            log_warning("⚠️ manager 未正确注入 redis_index 或 fsm，无法自动暂停/恢复任务", extra=get_log_context())
            return

        # 获取 user_ids
        user_ids: List[int] = []
        try:
            if hasattr(self.manager, "get_clients_by_phone"):
                bucket = self.manager.get_clients_by_phone(phone) or {}
                user_ids = list(bucket.keys())
        except Exception:
            user_ids = []
        if not user_ids:
            try:
                uid = await redis_index.get_user_id(phone)
                if uid:
                    user_ids = [int(uid)]
            except Exception:
                pass
        if not user_ids:
            log_warning("❌ 找不到用户 ID（多租户查无此号）", extra=get_log_context())
            return

        unhealthy_set = {
            HealthState.BANNED,
            HealthState.AUTH_EXPIRED,
            HealthState.NETWORK_ERROR,
            HealthState.FLOOD_WAIT,
        }

        # 异常 → 暂停
        if new in unhealthy_set:
            for user_id in user_ids:
                try:
                    await fsm.pause_task(user_id)
                except Exception:
                    pass
                log_debug("⏸️ 健康异常，已请求暂停任务", extra={"phone": phone, "user_id": user_id, **(get_log_context() or {})})
            return

        # 恢复为 OK
        if old in unhealthy_set and new == HealthState.OK:
            # 系统内联动事件（总线）
            try:
                user_ids = list((getattr(self.manager, "get_clients_by_phone")(phone) or {}).keys())
            except Exception:
                user_ids = []
            await bus.dispatch("RECOVERED", {"type": "RECOVERED", "phone": phone, "user_ids": user_ids})

            # 聊天“已恢复群发！”：仅在 notify=True 时发
            if notify and user_ids:
                rep = TaskStatusReporter(redis_client=getattr(getattr(self.manager, "fsm", None), "redis", None))
                for uid in user_ids:
                    rep.set_user(uid)
                    await rep.notify_recovered(uid, phone)

            # 若任务处于暂停则恢复，并尝试拉起调度（不受 notify 影响）
            for user_id in user_ids:
                try:
                    cur = await fsm.get_task_status(user_id)
                except Exception:
                    cur = None
                if cur == TaskStatus.PAUSED:
                    try:
                        await fsm.resume_task(user_id)
                        log_debug("✅ 恢复健康，已恢复任务状态", extra={"phone": phone, "user_id": user_id, **(get_log_context() or {})})
                    except Exception:
                        pass
                    if scheduler and hasattr(scheduler, "start_user_task"):
                        try:
                            set_log_context({"function": "HealthChecker._handle_state_change", "user_id": user_id, "trace_id": generate_trace_id(), "phase": "auto_start_task"})
                            ctx_create_task(scheduler.start_user_task, user_id)
                            log_info("🚀 自动拉起调度任务", extra={"user_id": user_id, **(get_log_context() or {})})
                        except Exception as e:
                            log_warning(f"❌ 启动任务失败: {e}", extra=get_log_context())

    # ---------- 冷却清理（与外部兼容） ----------
    async def clear_cooldown_for_user(self, user_id: int, phone: str) -> None:
        await self.clear_cooldown(phone)

    async def clear_all_cooldowns_for_user(self, user_id: int) -> None:
        try:
            clients = (
                getattr(self.manager, "get_clients_for_user", lambda _uid: {})(user_id)
                or {}
            )
            phones = list(clients.keys())
            if not phones and hasattr(self.manager, "list_user_phones"):
                pairs = self.manager.list_user_phones(user_id) or []
                phones = [p for p, _ in pairs]
            for p in phones:
                try:
                    await self.clear_cooldown(p)
                except Exception:
                    continue
        except Exception as e:
            log_warning(
                f"⚠️ clear_all_cooldowns_for_user 执行异常: {e}", extra=get_log_context()
            )

    async def clear_cooldown(self, phone: str) -> None:
        """透传给 FSM（若实现）。"""
        fsm = getattr(self.manager, "fsm", None)
        if fsm and hasattr(fsm, "clear_cooldown"):
            try:
                await fsm.clear_cooldown(phone)
            except Exception:
                pass

    async def _try_set_cooldown(self, phone: str, seconds: int) -> None:
        """
        兼容不同 FSM 签名：先尝试 set_cooldown(phone, seconds)
        不兼容则按 (user_id, phone, seconds) 对所有相关用户逐个设置
        """
        fsm = getattr(self.manager, "fsm", None)
        if not (fsm and hasattr(fsm, "set_cooldown")):
            return
        try:
            await fsm.set_cooldown(phone, seconds)
            return
        except TypeError:
            pass
        # 多租户逐个尝试
        try:
            user_ids = []
            if hasattr(self.manager, "get_clients_by_phone"):
                user_ids = list(
                    (self.manager.get_clients_by_phone(phone) or {}).keys()
                )
            for uid in user_ids:
                try:
                    await fsm.set_cooldown(uid, phone, seconds)
                except Exception:
                    continue
        except Exception:
            pass

    # ---------- 核心健康检查 ----------
    async def perform_health_check(self, phone: str, client: TelegramClient) -> None:
        """
        健康探针策略：
        1) 若未连接则尝试连接；
        2) 授权探针 is_user_authorized（短时异常不直接判死）；
        3) 基础 RPC：get_me（遇到 FLOOD_WAIT → 设置冷却并广播；其它异常用 classify_telethon_error 分类）。
        """
        set_log_context(
            {
                "function": "HealthChecker.perform_health_check",
                "phone": phone,
                "trace_id": generate_trace_id(),
                **(get_log_context() or {}),
            }
        )
        try:
            if not client.is_connected():
                try:
                    await client.connect()
                except Exception as ce:
                    from core.telethon_errors import classify_telethon_error, unwrap_error
                    st, _meta = classify_telethon_error(unwrap_error(ce))
                    if st == HealthState.AUTH_EXPIRED:
                        self._set_state(phone, HealthState.AUTH_EXPIRED)
                        return
                    if st == HealthState.BANNED:
                        self._set_state(phone, HealthState.BANNED)
                        await self._broadcast_banned(phone)
                        return
                    self._set_state(phone, st or HealthState.NETWORK_ERROR)
                    return
                    setattr(client, "__hc_connect_streak", 0)
                except Exception:
                    streak = int(getattr(client, "__hc_connect_streak", 0) or 0) + 1
                    setattr(client, "__hc_connect_streak", streak)
                    if streak >= 2:
                        self._set_state(phone, HealthState.NETWORK_ERROR)
                        return
                    # 第一次失败先短路返回，等待下一轮
                    self._set_state(phone, HealthState.NETWORK_ERROR)
                    return

            # 授权
            try:
                authed = await asyncio.wait_for(
                    client.is_user_authorized(), timeout=CHECK_TIMEOUT_AUTH
                )
            except Exception:
                # 授权探针偶发异常时，不立刻认定为未授权；交由后续 get_me 判定
                authed = None
            if authed is False:
                self._set_state(phone, HealthState.AUTH_EXPIRED)
                return

            # 基础 RPC：get_me
            gm_streak = int(getattr(client, "__hc_getme_to_streak", 0) or 0)
            try:
                await asyncio.wait_for(client.get_me(), timeout=CHECK_TIMEOUT_GETME)
                setattr(client, "__hc_getme_to_streak", 0)
            except FLOOD_ERRORS as e:
                secs = detect_flood_seconds(e)
                self._set_state(phone, HealthState.FLOOD_WAIT)
                await self._try_set_cooldown(phone, int(secs or 900))
                await self._broadcast_floodwait(phone, int(secs or 0))
                return
            except asyncio.TimeoutError:
                gm_streak += 1
                setattr(client, "__hc_getme_to_streak", gm_streak)
                if gm_streak >= 2:
                    self._set_state(phone, HealthState.NETWORK_ERROR)
                    return
                self._set_state(phone, HealthState.NETWORK_ERROR)
                return
            except Exception as e:
                # 统一分类 → HealthState
                state, meta = classify_telethon_error(e)

                if state == HealthState.FLOOD_WAIT:
                    secs = int((meta or {}).get("seconds") or detect_flood_seconds(e) or 900)
                    self._set_state(phone, HealthState.FLOOD_WAIT)
                    await self._try_set_cooldown(phone, secs)
                    await self._broadcast_floodwait(phone, secs)
                    return

                if state == HealthState.BANNED:
                    self._set_state(phone, HealthState.BANNED)
                    await self._broadcast_banned(phone)
                    return

                if state == HealthState.AUTH_EXPIRED:
                    self._set_state(phone, HealthState.AUTH_EXPIRED)
                    return

                # 其他异常归并 NETWORK_ERROR，便于可视化
                self._set_state(phone, state)
                return

            # 一切正常
            self._set_state(phone, HealthState.OK)

        except Exception as e:
            log_warning(f"perform_health_check 异常: {e}", extra=get_log_context())
            self._set_state(phone, HealthState.NETWORK_ERROR)

    async def check_all_clients(
        self, *, concurrency_limit: int = HEALTHCHECK_CONCURRENCY
    ) -> None:
        """并发巡检所有客户端。（统一改用 get_all_clients）"""
        try:
            clients = {}
            if hasattr(self.manager, "get_all_clients"):
                clients = self.manager.get_all_clients() or {}
        except Exception:
            clients = {}

        set_log_context(
            {
                "function": "HealthChecker.check_all_clients",
                "phase": "health_check_all",
                "trace_id": generate_trace_id(),
                "clients": len(clients or {}),
            }
        )
        sem = asyncio.Semaphore(max(1, int(concurrency_limit or 5)))

        async def _one(phone: str, client: TelegramClient):
            async with sem:
                set_log_context(
                    {"function": "HealthChecker.check_all_clients/_one", "phone": phone}
                )
                await self.perform_health_check(phone, client)

        tasks = [ctx_create_task(_one, p, c) for p, c in (clients or {}).items()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def auto_reconnect_failed(
        self, *, concurrency_limit: int = HEALTHCHECK_CONCURRENCY
    ) -> None:
        """对 NETWORK_ERROR / FLOOD_WAIT 的账号尝试重连 + 二次健康检查。（统一改用 get_all_clients）"""
        try:
            clients = {}
            if hasattr(self.manager, "get_all_clients"):
                clients = self.manager.get_all_clients() or {}
        except Exception:
            clients = {}

        set_log_context(
            {
                "function": "HealthChecker.auto_reconnect_failed",
                "phase": "auto_reconnect_failed",
                "trace_id": generate_trace_id(),
                "clients": len(clients or {}),
            }
        )
        sem = asyncio.Semaphore(max(1, int(concurrency_limit or 5)))

        async def _one(phone: str, client: TelegramClient):
            state = self.get_state(phone)
            if state not in {HealthState.NETWORK_ERROR, HealthState.FLOOD_WAIT}:
                return
            async with sem:
                try:
                    await client.connect()
                    # 若 manager 提供 ensure_connected 则优先使用（更稳健）
                    ok = False
                    try:
                        if hasattr(self.manager, "ensure_connected"):
                            ok = await getattr(self.manager, "ensure_connected")(client)
                        else:
                            # 兜底直接 get_me
                            await asyncio.wait_for(client.get_me(), timeout=CHECK_TIMEOUT_GETME)
                            ok = True
                    except Exception:
                        ok = False

                    if not ok:
                        # 短退避重试一次
                        await asyncio.sleep(1.0)
                        try:
                            if hasattr(self.manager, "ensure_connected"):
                                ok = await getattr(self.manager, "ensure_connected")(client)
                            else:
                                await asyncio.wait_for(client.get_me(), timeout=CHECK_TIMEOUT_GETME)
                                ok = True
                        except Exception:
                            ok = False

                    if not ok:
                        # 更精准区分未授权 vs 网络/连接异常
                        try:
                            authed = await client.is_user_authorized()
                        except Exception:
                            authed = None
                        if authed is False:
                            self._set_state(phone, HealthState.AUTH_EXPIRED)
                        else:
                            self._set_state(phone, HealthState.NETWORK_ERROR)
                        raise RuntimeError("ensure_connected failed after reconnect")

                    log_info(
                        "🔁 自动重连成功",
                        extra={"phone": phone, **(get_log_context() or {})},
                    )
                    await self.perform_health_check(phone, client)
                except Exception as e:
                    log_warning(
                        "❌ 自动重连失败",
                        extra={
                            "phone": phone,
                            "error": str(e),
                            **(get_log_context() or {}),
                        },
                    )

        tasks = [ctx_create_task(_one, p, c) for p, c in (clients or {}).items()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ---------- 循环管理 ----------
    async def _health_loop(self, interval_sec: int) -> None:
        set_log_context({"function": "HealthChecker._health_loop", "phase": "health_loop"})
        log_debug(f"⏱️ 启动健康检查，每 {interval_sec}s", extra=get_log_context())
        try:
            while self._running:
                await self.check_all_clients()
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log_warning(f"⚠️ 健康检查循环异常: {e}", extra=get_log_context())

    async def _reconnect_loop(self, interval_sec: int) -> None:
        set_log_context(
            {"function": "HealthChecker._reconnect_loop", "phase": "reconnect_loop"}
        )
        log_debug("🔄 启动自动恢复冷却账号巡检", extra=get_log_context())
        try:
            while self._running:
                await self.auto_reconnect_failed()
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log_warning(f"⚠️ 自动恢复循环异常: {e}", extra=get_log_context())

    async def start(
        self, *, health_interval_sec: int = 60, reconnect_interval_sec: int = 60
    ) -> None:
        """启动健康检查与自动恢复循环。多次调用仅第一次生效。"""
        if self._running:
            return
        self._running = True
        set_log_context(
            {
                "function": "HealthChecker.start",
                "phase": "health_start",
                "trace_id": generate_trace_id(),
            }
        )
        self._loop_task = ctx_create_task(self._health_loop, health_interval_sec)
        self._reconnect_task = ctx_create_task(
            self._reconnect_loop, reconnect_interval_sec
        )

    async def stop(self) -> None:
        """停止所有循环。"""
        if not self._running:
            return
        self._running = False
        for t in (self._loop_task, self._reconnect_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        self._loop_task = None
        self._reconnect_task = None
