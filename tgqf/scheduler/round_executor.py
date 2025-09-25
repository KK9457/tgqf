# scheduler/round_executor.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import List, Dict, Any, Callable, Optional

from telethon import TelegramClient
from telethon.tl.types import MessageEntityCustomEmoji
from typess.ttl_types import TTLConfig, next_join_cooldown     # ✅ 冷却序列
from typess.join_status import JoinCode,JoinResult                   # ✅ 识别 FLOOD/ALREADY 等
from core.task_status_reporter import TaskStatusReporter, notify_group_join_summary

from typess.link_types import ParsedLink
from typess.message_enums import MessageContent
from unified.trace_context import with_trace_ctx, set_log_context, get_log_context, generate_trace_id
from unified.logger import log_info, log_warning, log_error, log_exception
from unified.config import RUNNER_TIMEOUT, MAX_CONCURRENCY
from core.task_status_reporter import TaskStatusReporter
from scheduler.result_tracker import ResultTracker

__all__ = ["RoundExecutor","AccountJoinRunner"]


def _extract_custom_emoji_ids(entities) -> List[int]:
    ids: List[int] = []
    for e in (entities or []):
        if isinstance(e, MessageEntityCustomEmoji):
            try:
                v = int(getattr(e, "document_id", 0) or 0)
                if v:
                    ids.append(v)
            except Exception:
                continue
    # 保序去重
    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class RoundExecutor:
    """单轮执行器：并发执行群发（账号并行度统一由 MAX_CONCURRENCY 管理）"""

    def __init__(
        self,
        *,
        clients: Dict[str, TelegramClient],
        groups_by_phone: Dict[str, List[ParsedLink]],
        message: MessageContent,
        user_id: int,
        task_id: str,
        runner_factory: Callable[..., Any],
        reporter: Optional[TaskStatusReporter] = None,
        event: Optional[Any] = None,
        concurrency_limit: int = MAX_CONCURRENCY,   # ✅ 默认并发度=常量
        to_user_id: Optional[int] = None,
        trace_id: Optional[str] = None,
        round_index: int = 1,
    ) -> None:
        self.event = event
        self.clients = clients or {}
        self.groups_by_phone = groups_by_phone or {}
        self.message = message
        self.user_id = int(user_id)
        self.to_user_id = int(to_user_id or user_id)
        self.task_id = str(task_id)
        self.runner_factory = runner_factory
        self.reporter = reporter
        self.concurrency_limit = max(1, int(concurrency_limit or MAX_CONCURRENCY))
        self.sem = asyncio.Semaphore(self.concurrency_limit)
        self.trace_id = str(trace_id or generate_trace_id())
        self.round_index = round_index
        self.tracker = ResultTracker(user_id=self.user_id, reporter=self.reporter)
        emoji_ids = _extract_custom_emoji_ids(getattr(message, "entities", None))
        self._emoji_meta = {"emoji_ids": emoji_ids, "emoji_count": len(emoji_ids)}

        # ✅ 同一轮共享缓存（供各 Runner 共享，如链接检查/只读性探测/可写性探测等）
        self.round_cache: dict = {}

    async def run(self) -> List[Dict[str, Any]]:
        # 统一 trace 上下文（控制面）
        set_log_context({
            "trace_id": self.trace_id,
            "task_id": self.task_id,
            "user_id": self.user_id,
            "round": self.round_index,        # 统一字段
            "round_index": self.round_index,  # 兼容别名
            "phones_total": len(self.clients),
            "groups_total": sum(len(v or []) for v in self.groups_by_phone.values()),
        })
        runners: List[Any] = []
        for phone, client in self.clients.items():
            groups = self.groups_by_phone.get(phone, [])
            if not groups:
                continue
            runner = await self.runner_factory(
                phone=phone,
                client=client,
                groups=groups,
                message=self.message,
                user_id=self.user_id,
                task_id=self.task_id,
                reporter=self.reporter,
                event=self.event,
                to_user_id=self.to_user_id,
                trace_id=self.trace_id,
                # ✅ 按要求注入同一轮共享 round_cache
                link_check_cache=self.round_cache,
            )
            if runner is not None:
                runners.append(runner)

        if not runners:
            log_warning("所有账号均不可用", extra={"user_id": self.user_id, "task_id": self.task_id})
            return []

        results = await self._run_concurrently(runners)

        # 每轮结束时汇总通知（统一 reporter 汇总）
        if self.reporter:
            try:
                await self.reporter.notify_round_summary(
                    self.user_id,
                    self.round_index,
                    results,
                    to_user_id=self.to_user_id,
                )
            except Exception as e:
                log_exception("notify_round_summary 失败", exc=e, extra=get_log_context())

        return results

# =========================================================
# ✅ 账号内串行 Join 执行器：仅当“真正 join/import”才应用 JOIN_COOLDOWN_SEQUENCE
# =========================================================
class AccountJoinRunner:
    """
    账号维度的入群串行执行器：
    - ALREADY：不等待，但写入统计，纳入汇总通知
    - 真正 join/import：按策略秒数优先，否则按 JOIN_COOLDOWN_SEQUENCE 冷却
    """
    def __init__(self, *, account_id: str, client: TelegramClient, ttl: TTLConfig,
                 join_func: Callable[[TelegramClient, ParsedLink], "JoinResult"],
                 tracker: Optional[ResultTracker] = None,
                 reporter: Optional[TaskStatusReporter] = None):
        self.account_id = str(account_id)
        self.client = client
        self.ttl = ttl
        self.join_func = join_func
        self.tracker = tracker or ResultTracker(user_id=0, reporter=reporter)  # user_id 可按需外部填充
        self.reporter = reporter
        self._join_attempts = 0  # 仅统计“真正触发 join/import”的次数

    async def run_queue(self, targets: List[ParsedLink], *, user_id: int, to_user_id: Optional[int] = None) -> None:
        set_log_context({"stage": "join_queue", "account_id": self.account_id, "user_id": user_id})
        for t in targets or []:
            res = await self.join_func(self.client, t)  # -> JoinResult
            # 记录统计（要求 ResultTracker 能区分 ok/already/need_approve/forbidden/invalid/flood 等）
            try:
                self.tracker.add_join_result(self.account_id, t, res)  # 需在 ResultTracker 实现该接口
            except Exception:
                pass

            # 冷却判断
            sleep_sec = 0
            attempted = bool(getattr(res, "meta", {}).get("attempted_join"))  # 是否“真正 join”
            if attempted:
                # 策略携带秒数（FLOOD/SLOWMODE）优先
                policy_sec = int((res.meta or {}).get("seconds") or 0)
                if policy_sec > 0:
                    sleep_sec = policy_sec
                else:
                    self._join_attempts += 1
                    sleep_sec = next_join_cooldown(self.ttl, self._join_attempts)

            # ALREADY 不等待；其他 attempted 才等待
            if sleep_sec > 0:
                try:
                    await asyncio.sleep(sleep_sec)
                except Exception:
                    pass

        # 队列结束后发送汇总（需在 reporter 或独立通知里实现 already 统计）
        try:
            stats = self.tracker.snapshot(account_id=self.account_id)  # 需在 ResultTracker 实现
            await notify_group_join_summary(self.account_id, stats)
        except Exception as e:
            log_exception("notify_group_join_summary failed", exc=e, extra=get_log_context())

    async def _run_concurrently(self, runners: List[Any]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        async def run_runner(runner: Any) -> None:
            phone = getattr(runner, "phone", "unknown")
            async with self.sem:
                try:
                    # 控制面仅负责调度；数据面由 ResultTracker 接收 streaming 条目
                    with with_trace_ctx(self.user_id, self.task_id, phone, None, round=self.round_index, trace_id=self.trace_id):
                        result = await runner.run()
                    if not isinstance(result, dict):
                        result = {"status": "unknown", "raw": result}
                    result.update({
                        "trace_id": self.trace_id,
                        "task_id": self.task_id,
                        "user_id": self.user_id,
                        "phone": phone,
                        "round_index": self.round_index,
                    })
                    results.append(result)
                    # tracker.track 会转发到 reporter（若 reporter 可用）
                    self.tracker.track(result)

                except Exception as e:
                    log_exception("Runner 执行异常", exc=e, extra={
                        "phone": phone, "task_id": self.task_id, "user_id": self.user_id, "trace_id": self.trace_id
                    })
                    fail_result = {
                        "status": "failed",
                        "error": str(e),
                        "phone": phone,
                        "user_id": self.user_id,
                        "task_id": self.task_id,
                        "trace_id": self.trace_id,
                        "round_index": self.round_index,
                    }
                    results.append(fail_result)
                    self.tracker.track(fail_result)

        await asyncio.gather(*(run_runner(r) for r in runners))
        return results
