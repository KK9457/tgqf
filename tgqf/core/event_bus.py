# core/event_bus.py 
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, DefaultDict, Dict, Iterable, List, Optional
from collections import defaultdict

# 事件名：字符串即可，例如：
# "FLOOD_WAIT", "BANNED", "AUTH_EXPIRED", "RECOVERED",
# "JOIN_APPROVED", "KICKED", "WRITE_FORBIDDEN", "RESTRICTED"
EventHandler = Callable[[Dict[str, Any]], Awaitable[None] | None]


class EventBus:
    """
    轻量事件总线（支持：
      - 同步/异步注册
      - once 一次性订阅
      - 粘性事件（sticky replay）
      - 通配符 "*" 监听
      - 线程/协程安全
    ）
    """

    def __init__(self) -> None:
        self._handlers: DefaultDict[str, List[EventHandler]] = defaultdict(list)
        self._once_handlers: DefaultDict[str, List[EventHandler]] = defaultdict(list)
        self._last_payloads: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._max_last = 200  # 粘性事件条目上限，防内存膨胀

    # ---------- 同步注册（推荐，避免竞态） ----------
    def register(self, event_name: str, handler: EventHandler) -> None:
        """同步注册，立即生效（幂等：同一 handler 不重复添加）。"""
        if handler not in self._handlers[event_name]:
            self._handlers[event_name].append(handler)

    def register_many(self, event_names: Iterable[str], handler: EventHandler) -> None:
        for name in event_names:
            if handler not in self._handlers[name]:
                self._handlers[name].append(handler)

    # ---------- 异步注册（向后兼容） ----------
    async def subscribe(self, event_name: str, handler: EventHandler) -> None:
        async with self._lock:
            self._handlers[event_name].append(handler)

    async def subscribe_many(self, event_names: Iterable[str], handler: EventHandler) -> None:
        async with self._lock:
            for name in event_names:
                self._handlers[name].append(handler)

    async def subscribe_sticky(
        self,
        event_name: str,
        handler: EventHandler,
        *,
        replay_immediately: bool = True,
    ) -> None:
        """
        订阅并可选立即回放最近一次该事件的 payload（若存在）。
        """
        async with self._lock:
            self._handlers[event_name].append(handler)
            payload = self._last_payloads.get(event_name)
        if replay_immediately and payload is not None:
            asyncio.create_task(self._safe_call(handler, payload))

    # ---------- 一次性订阅 ----------
    async def once(self, event_name: str, handler: EventHandler) -> None:
        async with self._lock:
            self._once_handlers[event_name].append(handler)

    # ---------- 取消订阅 ----------
    async def unsubscribe(self, event_name: str, handler: EventHandler) -> None:
        """取消订阅；静默失败。"""
        async with self._lock:
            try:
                if event_name in self._handlers and handler in self._handlers[event_name]:
                    self._handlers[event_name].remove(handler)
            except Exception:
                pass
            try:
                if event_name in self._once_handlers and handler in self._once_handlers[event_name]:
                    self._once_handlers[event_name].remove(handler)
            except Exception:
                pass

    # ---------- 粘性事件工具 ----------
    def clear_last(self, event_name: str) -> None:
        self._last_payloads.pop(event_name, None)

    def get_last(self, event_name: str) -> Optional[Dict[str, Any]]:
        return self._last_payloads.get(event_name)

    # ---------- 触发 ----------
    async def _safe_call(self, handler: EventHandler, payload: Dict[str, Any]) -> None:
        try:
            res = handler(payload)
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            # 事件处理失败不影响主流程
            pass

    async def dispatch(self, event_name: str, payload: Dict[str, Any]) -> None:
        """
        触发事件（记录粘性 payload；普通/通配/一次性订阅者并发执行）。
        """
        async with self._lock:
            self._last_payloads[event_name] = dict(payload or {})
            # 防止粘性事件无限增长
            if len(self._last_payloads) > self._max_last:
                try:
                    k = next(iter(self._last_payloads.keys()))
                    self._last_payloads.pop(k, None)
                except Exception:
                    pass

            handlers = list(self._handlers.get(event_name, []))
            wildcard = list(self._handlers.get("*", []))
            once_handlers = list(self._once_handlers.get(event_name, []))
            self._once_handlers[event_name].clear()

        all_handlers = handlers + wildcard + once_handlers
        if not all_handlers:
            return

        await asyncio.gather(
            *(self._safe_call(h, payload) for h in all_handlers),
            return_exceptions=True,  # 单个 handler 错误不阻断
        )

    # 可选别名
    async def emit(self, event_name: str, payload: Dict[str, Any]) -> None:
        await self.dispatch(event_name, payload)


# 单例总线
bus = EventBus()
