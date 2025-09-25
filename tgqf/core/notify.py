# -*- coding: utf-8 -*-
# core/notify.py

from __future__ import annotations
from typing import TYPE_CHECKING, Any, List, Optional

from core.task_status_reporter import TaskStatusReporter
from unified.ui_kit import UiKit
from unified.trace_context import set_log_context

# 懒加载上下文，避免循环引用
try:
    from unified.context import get_bot, get_redis
except Exception:
    def get_redis():
        return None  # type: ignore

    def get_bot():
        return None  # type: ignore

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from telethon import TelegramClient
    from telethon.events import NewMessage


class StdReporter:
    """
    统一“用户通知类”：
    - 所有通知统一走 TaskStatusReporter.notify_simple(...) 或对应语义方法
    - 样式统一：UiKit（HTML 卡片，默认 parse_mode=html）
    - 不附带 trace_id/user_id/me_phone/bot_name 等上下文尾注
    """
    def __init__(self, reporter: TaskStatusReporter, user_id: Optional[int]):
        self._r = reporter
        self._uid = user_id or getattr(reporter, "_default_user_id", None)

    # —— 语义方法 —— #
    async def info(self, title: str, lines: Optional[List[str]] = None, **send_kwargs) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="info", **send_kwargs)

    async def ok(self, title: str, lines: Optional[List[str]] = None, **send_kwargs) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="ok", **send_kwargs)

    async def warn(self, title: str, lines: Optional[List[str]] = None, **send_kwargs) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="warn", **send_kwargs)

    async def progress(self, title: str, lines: Optional[List[str]] = None, **send_kwargs) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="progress", **send_kwargs)

    async def error(self, title: str, err: Any | None = None, **send_kwargs) -> None:
        why = str(err) if err is not None else "-"
        await self._r.notify_simple(self._uid, title, [UiKit.bullet(why)], level="error", **send_kwargs)

    # —— 常用场景别名 —— #
    async def started(self, task_id: str = "-", **send_kwargs) -> None:
        await self._r.notify_simple(self._uid, "任务启动", [UiKit.kv("Task ID", task_id)], level="start", **send_kwargs)

    async def stopped(self, task_id: str = "-", **send_kwargs) -> None:
        await self._r.notify_task_stopped(self._uid, task_id, **send_kwargs)

    async def round_partial_ok(self, **send_kwargs) -> None:
        await self._r.notify_round_mix_result(self._uid, **send_kwargs)

    # —— 透传底层 —— #
    @property
    def raw(self) -> TaskStatusReporter:
        return self._r


class ReporterFactory:
    """
    入口工厂：
    - create(): 原始 TaskStatusReporter（注入 redis/bot/user_id）
    - create_std(): 返回 StdReporter（语义化通知）
    """
    def __init__(self, redis: Optional[Redis] = None, bot: Optional[TelegramClient] = None):
        self._redis = redis
        self._bot = bot

    def _resolve_redis(self, override: Optional[Redis]) -> Optional[Redis]:
        if override is not None:
            return override
        if self._redis is not None:
            return self._redis
        try:
            return get_redis()
        except Exception:
            return None

    def _resolve_bot(self, override: Optional[TelegramClient]) -> Optional[TelegramClient]:
        if override is not None:
            return override
        if self._bot is not None:
            return self._bot
        try:
            return get_bot()
        except Exception:
            return None



    def create(
        self,
        event: Optional[NewMessage.Event] = None,
        user_id: Optional[int] = None,
        *,
        redis: Optional[Redis] = None,
        bot: Optional[TelegramClient] = None,
    ) -> TaskStatusReporter:
        rds = self._resolve_redis(redis)
        b = self._resolve_bot(bot)
        uid = self._resolve_user_id(user_id, event)
        reporter = TaskStatusReporter(event=event, redis_client=rds, bot=b)
        if uid is not None:
            try:
                reporter.set_user(uid)
            except Exception:
                pass
            try:
                set_log_context({"user_id": uid})
            except Exception:
                pass
        return reporter

    def create_std(
        self,
        event: Optional[NewMessage.Event] = None,
        user_id: Optional[int] = None,
        *,
        redis: Optional[Redis] = None,
        bot: Optional[TelegramClient] = None,
    ) -> StdReporter:
        r = self.create(event, user_id, redis=redis, bot=bot)
        return StdReporter(r, self._resolve_user_id(user_id, event))
