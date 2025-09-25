# router/command_router.py
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Set, Tuple, Union

from telethon import TelegramClient, events

from core.decorators import command_safe
from core.registry_guard import safe_add_event_handler
from unified.logger import log_exception, log_info, log_warning
from unified.trace_context import generate_trace_id, get_log_context, set_log_context


class CommandRouter:
    """
    统一命令/事件路由：
    - command(pattern|[pattern...], trace_name="", admin_only=False, white_only=False)
    - handler(event_class, *args, **kwargs)
    - register_to(client)  (幂等)
    """

    def __init__(self) -> None:
        self._routes: Dict[str, Callable[..., Any]] = {}
        self._handlers: List[Tuple[Callable[..., Any], Any, tuple, dict]] = []
        self._registered_clients: Set[int] = set()

    def command(
        self,
        pattern: Union[str, List[str]],
        *,
        trace_name: str = "",
        admin_only: bool = False,
        white_only: bool = False,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        patterns = [pattern] if isinstance(pattern, str) else (pattern or [])

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            for pat in patterns:
                if pat in self._routes:
                    log_warning(f"⚠️ 命令 `{pat}` 已存在，跳过注册 {func.__name__}")
                    continue

                @command_safe(admin_only=admin_only, white_only=white_only)
                async def wrapper(event, _pat=pat, _func=func, _trace_name=trace_name):
                    trace_id = generate_trace_id()
                    user_id = getattr(event, "sender_id", None)
                    set_log_context(
                        {
                            "user_id": user_id,
                            "chat_id": getattr(event, "chat_id", None),
                            "command": _pat,
                            "trace_id": trace_id,
                            "trace_name": _trace_name or _func.__name__,
                            "module": _func.__module__,
                            "signal": "entry",
                            "phase": "handler",
                        }
                    )
                    log_info(f"📩 命令触发：{_pat}", extra=get_log_context())
                    try:
                        await _func(event)
                    except Exception as e:
                        log_exception("❌ 命令处理异常", exc=e, extra=get_log_context())
                        return

                self._routes[pat] = wrapper
            return func

        return decorator

    def handler(
        self, event_class, *args, **kwargs
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._handlers.append((func, event_class, args, kwargs))
            return func

        return decorator

    def register_to(self, client: TelegramClient) -> None:
        cid = id(client)
        if cid in self._registered_clients:
            log_warning("⚠️ CommandRouter 已为该 client 注册过，跳过本次（幂等保护）")
            return

        for cmd, handler in self._routes.items():
            pattern = f"^{re.escape(cmd)}(?:@\\w+)?(?:\\s+.*)?$"
            safe_add_event_handler(
                client, handler, events.NewMessage(pattern=pattern), tag=f"cmd:{cmd}"
            )

        for func, event_class, args, kwargs in self._handlers:
            safe_add_event_handler(
                client,
                func,
                event_class(*args, **kwargs),
                tag=f"evt:{func.__module__}.{func.__name__}",
            )

        self._registered_clients.add(cid)


# 默认全局路由实例
main_router = CommandRouter()
