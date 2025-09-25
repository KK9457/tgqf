# core/defaults/reporter_factory.py
from __future__ import annotations

"""
ReporterFactory（新版）
- 统一创建 TaskStatusReporter
- 自动注入 user_id（多租户隔离关键）
- 统一接入 bot / redis（从 unified.context 懒加载）
- 提供标准化通知封装 StdReporter（卡片模板一致化）
"""

from typing import Any, List, Optional
from redis.asyncio import Redis
from telethon import TelegramClient
from telethon.events import NewMessage

from core.task_status_reporter import TaskStatusReporter
from unified.ui_kit import UiKit
from unified.trace_context import set_log_context

# 懒加载上下文，避免循环引用
try:
    from unified.context import get_bot, get_redis
except Exception:  # 早期初始化场景允许缺省
    def get_redis():
        return None  # type: ignore

    def get_bot():
        return None  # type: ignore


class StdReporter:
    """
    标准化通知封装：统一卡片风格 + 语义型方法
    使用示例：
        std = ReporterFactory().create_std(event, user_id)
        await std.info("标题", ["行1", "行2"])
        await std.error("标题", "错误详情")
        await std.started(task_id)
        await std.stopped(task_id)

    """

    def __init__(self, reporter: TaskStatusReporter, user_id: Optional[int]):
        self._r = reporter
        self._uid = user_id or getattr(reporter, "_default_user_id", None)

    # ---- 语义级别封装（统一走 UiKit + notify_simple） ----
    async def info(self, title: str, lines: Optional[List[str]] = None) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="info")

    async def ok(self, title: str, lines: Optional[List[str]] = None) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="ok")

    async def warn(self, title: str, lines: Optional[List[str]] = None) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="warn")

    async def progress(self, title: str, lines: Optional[List[str]] = None) -> None:
        await self._r.notify_simple(self._uid, title, lines or [], level="progress")

    async def error(self, title: str, err: Any | None = None) -> None:
        why = str(err) if err is not None else "-"
        await self._r.notify_simple(self._uid, title, [UiKit.bullet(why)], level="error")

    # ---- 常用场景别名 ----
    async def started(self, task_id: str = "-") -> None:
        await self._r.notify_simple(self._uid, "任务启动", [UiKit.kv("Task ID", task_id)], level="start")

    async def stopped(self, task_id: str = "-") -> None:
        await self._r.notify_task_stopped(self._uid, task_id)

    async def round_partial_ok(self) -> None:
        await self._r.notify_round_mix_result(self._uid)

    # 透传底层能力（必要时可直接访问）
    @property
    def raw(self) -> TaskStatusReporter:
        return self._r


class ReporterFactory:
    """
    新版 ReporterFactory：
    - create(): 返回原始 TaskStatusReporter（保持向后兼容）
    - create_std(): 返回带标准化模板封装的 StdReporter

    user_id 注入策略（优先级）：
        1. 显式传入的 user_id
        2. event.sender_id（如果存在）
        3. （无）
    """

    def __init__(self, redis: Optional["Redis"] = None, bot: Optional["TelegramClient"] = None):
        self._redis = redis
        self._bot = bot

    # ---- 内部工具 ----
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



    # ---- 对外 API ----
    def create(
        self,
        event: Optional[NewMessage.Event] = None,
        user_id: Optional[int] = None,
        *,
        redis: Optional[Redis] = None,
        bot: Optional[TelegramClient] = None,
    ) -> TaskStatusReporter:
        """
        创建绑定好上下文的 TaskStatusReporter
        - 自动注入 redis / bot
        - 通过 set_user 绑定 user_id（如果解析到）
        """
        resolved_redis = self._resolve_redis(redis)
        resolved_bot = self._resolve_bot(bot)
        resolved_user = self._resolve_user_id(user_id, event)

        reporter = TaskStatusReporter(
            event=event,
            redis_client=resolved_redis,
            bot=resolved_bot,
        )
        # 注入 user（如果可用），并把 user_id 写进 log context 以便 trace 使用
        if resolved_user is not None:
            try:
                reporter.set_user(resolved_user)
            except Exception:
                pass
            try:
                set_log_context({"user_id": resolved_user})
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
        """
        返回带标准模板封装的 StdReporter
        """
        r = self.create(event, user_id, redis=redis, bot=bot)
        return StdReporter(r, self._resolve_user_id(user_id, event))

    def _resolve_user_id(self, explicit_uid: Optional[int], event: Optional[NewMessage.Event]) -> Optional[int]:
        if explicit_uid is not None:
            return int(explicit_uid)
        if event is not None and getattr(event, "sender_id", None) is not None:
            try:
                return int(event.sender_id)  # type: ignore[attr-defined]
            except Exception:
                return None
        return None