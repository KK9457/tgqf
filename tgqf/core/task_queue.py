# core/task_queue.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Optional, Tuple

from telethon import TelegramClient

from core.task_status_reporter import TaskStatusReporter
from typess.message_enums import SendTask
from unified.logger import log_debug, log_error, log_warning


class TaskPriority(IntEnum):
    HIGH = 0
    NORMAL = 10
    LOW = 20


@dataclass(order=True)
class TaskItem:
    # 用于优先级队列排序：优先级越小越先出队；同优先级按入队时间
    sort_index: Tuple[int, float] = field(init=False, repr=False)

    # 业务字段
    priority: TaskPriority
    enqueued_at: float
    user_id: int                       # 业务归属（多用户隔离主键）
    task: SendTask
    clients: Dict[str, TelegramClient]
    event: Optional[Any] = None
    reporter: Optional[TaskStatusReporter] = None

    # 新增：消息接收者（外发通知时若不为 None 则优先使用）
    to_user_id: Optional[int] = None

    # 追踪字段
    item_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        self.sort_index = (int(self.priority), self.enqueued_at)


class TaskQueue:
    """
    轻量优先级任务队列（基于 asyncio.PriorityQueue）：
    - enqueue：入队（支持优先级/回执 item_id）
    - pop：阻塞出队
    - try_pop_nowait：非阻塞出队
    - cancel_by_user：按 user_id 取消队列中未执行任务
    - join/task_done：与消费者配合完成队列 drain
    """
    def __init__(self) -> None:
        self._q: "asyncio.PriorityQueue[TaskItem]" = asyncio.PriorityQueue()

    # 非阻塞出队（为空返回 None）
    def try_pop_nowait(self) -> Optional[TaskItem]:
        try:
            item = self._q.get_nowait()  # 注意：get_nowait() 是同步方法
            log_debug(
                "📤 出队任务(try_nowait)",
                extra={
                    "user_id": item.user_id,
                    "to_user_id": item.to_user_id or item.user_id,
                    "task_id": getattr(item.task, "task_id", None),
                    "item_id": item.item_id,
                },
            )
            return item
        except asyncio.QueueEmpty:
            return None

    async def enqueue(
        self,
        user_id: int,
        task: SendTask,
        clients: Dict[str, TelegramClient],
        event: Optional[Any],
        *,
        reporter: Optional[TaskStatusReporter] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        to_user_id: Optional[int] = None,
    ) -> str:
        """
        入队统一入口：
        - user_id: 业务归属用户（隔离/统计）
        - to_user_id: 消息接收者；默认 None（下游使用时 fallback 到 user_id）
        """
        item = TaskItem(
            priority=priority,
            enqueued_at=time.monotonic(),
            user_id=user_id,
            task=task,
            clients=clients,
            event=event,
            reporter=reporter,
            to_user_id=to_user_id,
        )

        # 兜底赋 task_id（如未生成）
        if not getattr(task, "task_id", None):
            try:
                setattr(task, "task_id", str(uuid.uuid4()))
            except Exception:
                pass

        await self._q.put(item)
        log_debug(
            "📥 入队成功",
            extra={
                "user_id": user_id,
                "to_user_id": to_user_id or user_id,
                "task_id": getattr(task, "task_id", None),
                "priority": int(priority),
                "item_id": item.item_id,
            },
        )
        return item.item_id

    async def pop(self) -> Optional[TaskItem]:
        try:
            item = await self._q.get()
            log_debug(
                "📤 出队任务",
                extra={
                    "user_id": item.user_id,
                    "to_user_id": item.to_user_id or item.user_id,
                    "task_id": getattr(item.task, "task_id", None),
                    "item_id": item.item_id,
                },
            )
            return item
        except Exception as e:
            log_warning("⚠️ 出队失败（可能为空或并发调整）", extra={"err": str(e)})
            return None

    async def cancel_by_user(self, user_id: int) -> int:
        cnt = 0
        tmp = []
        try:
            while not self._q.empty():
                item = self._q.get_nowait()
                if item.user_id != user_id:
                    tmp.append(item)
                else:
                    cnt += 1
            for it in tmp:
                await self._q.put(it)
            log_debug("🗑️ 取消队列任务", extra={"user_id": user_id, "count": cnt})
        except Exception as e:
            log_error("❌ 取消队列任务失败", extra={"user_id": user_id, "err": str(e)})
        return cnt

    def empty(self) -> bool:
        return self._q.empty()

    def task_done(self) -> None:
        try:
            self._q.task_done()
        except ValueError:
            # 忽略偶发的未匹配 task_done 调用
            pass

    async def join(self) -> None:
        await self._q.join()
