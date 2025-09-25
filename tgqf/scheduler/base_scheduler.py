# -*- coding: utf-8 -*-
# scheduler/base_scheduler.py
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Callable, Dict, Optional
import random
from redis.asyncio import Redis
from telethon import TelegramClient
from telethon.events import NewMessage

from core.task_status_reporter import TaskStatusReporter, K_PENDING_REASSIGN
from typess.message_enums import SendTask, TaskStatus
from unified.logger import log_info, log_warning, log_exception, log_debug
from unified.trace_context import set_log_context, set_trace_context, get_trace_id
from core.redis_fsm import RedisCommandFSM
from core.defaults.client_manager import SessionClientManager
from core.defaults.group_assigner import BalancedGroupAssigner
from core.defaults.message_sender import MessageSender
from unified.config import (
    MAX_CONCURRENCY,
    TASK_ROUND_INTERVAL_DEFAULT,
    REASSIGN_LOOP_INTERVAL,
    REASSIGN_MAX_TRY_PER_ROUND,
    REASSIGN_JITTER_MIN,
    REASSIGN_JITTER_MAX,
    ENTITY_WARM_DIALOGS_LIMIT,
)
from scheduler.task_executor import TaskExecutor
from scheduler.result_tracker import ResultTracker
from core.task_queue import TaskQueue, TaskPriority
from typess.link_types import ParsedLink
from scheduler.ratelimit import FloodController
# 预热实体
from tg.entity_utils import ensure_entity_seen
from unified.selection import unified_prepare_and_select_clients
__all__ = ["BaseScheduler"]


def _int_concurrency(v: int | Any) -> int:
    try:
        return max(1, int(getattr(v, "value", v)))
    except Exception:
        return 1


class BaseScheduler:
    def __init__(
        self,
        fsm: RedisCommandFSM,
        client_manager: SessionClientManager,
        reporter_factory,
        assigner_factory: Optional[Callable[[int], BalancedGroupAssigner]] = None,
        sender_factory: Callable[[int, TelegramClient], MessageSender] = (
            lambda user_id, client: MessageSender(client=client, phone=getattr(client, "phone", ""), user_id=user_id)
        ),
        max_concurrency: int = MAX_CONCURRENCY,
        validator=None,
        task_queue: Optional[TaskQueue] = None,
    ) -> None:
        self.fsm = fsm
        self.client_manager = client_manager
        self.reporter_factory = reporter_factory
        # ✅ 默认工厂会把 fsm 注入给分配器，策略里需要用到
        self.assigner_factory = assigner_factory or (lambda uid, _self=self: BalancedGroupAssigner(fsm=_self.fsm))
        self.sender_factory = sender_factory
        self.max_concurrency = _int_concurrency(max_concurrency)

        self._active_tasks: Dict[int, asyncio.Task] = {}
        self.executors: Dict[int, TaskExecutor] = {}
        self._reporters: Dict[int, TaskStatusReporter] = {}
        self._reassign_loops: Dict[int, asyncio.Task] = {}
        self.validator = validator
        self.task_queue = task_queue or TaskQueue()
        # 每个 user 复用一个 FloodController，供 runner 与重分配 loop 共用
        self._floods: Dict[int, FloodController] = {}

        log_debug("BaseScheduler 初始化", extra={"max_concurrency": self.max_concurrency})
        try:
            if hasattr(self.client_manager, "register_scheduler"):
                self.client_manager.register_scheduler(self)
        except Exception:
            pass

    def _get_flood(self, user_id: int) -> FloodController:
        fc = self._floods.get(user_id)
        if fc is None:
            fc = FloodController(bound_user_ids={int(user_id)})
            self._floods[user_id] = fc
            log_debug("flood_controller_initialized", extra={"user_id": user_id})
        return fc

    async def start(
        self,
        user_id: int,
        task: SendTask,
        clients: Dict[str, TelegramClient],
        event: Optional[NewMessage.Event] = None,
        reporter: Optional[TaskStatusReporter] = None,
        *,
        to_user_id: Optional[int] = None,
    ) -> None:
        recv_uid = int(to_user_id or user_id)

        old = self._active_tasks.pop(user_id, None)
        if old and not old.done():
            old.cancel()
            try:
                await asyncio.wait_for(old, timeout=5)
                log_info("⛔ 已取消旧任务", extra={"user_id": user_id})
            except asyncio.CancelledError:
                log_info("⛔ 旧任务主动取消", extra={"user_id": user_id})
            except asyncio.TimeoutError:
                log_warning("取消旧任务超时（强制释放引用）", extra={"user_id": user_id})
            except Exception as e:
                log_exception("取消旧任务时异常（忽略）", exc=e, extra={"user_id": user_id})

        trace_id = str(getattr(task, "task_id", "") or uuid.uuid4())
        task.task_id = trace_id
        set_trace_context(user_id=user_id, trace_id=trace_id)
        set_log_context({"user_id": user_id, "task_id": trace_id, "cmd": "/ok"})
        log_info("🧭 调度启动入口", extra={"trace_id": trace_id, "user_id": user_id})

        try:
            reporter = reporter or self.reporter_factory.create(event, user_id=user_id)
        except Exception as e:
            log_exception("Reporter 创建失败（继续运行）", exc=e, extra={"user_id": user_id})
            reporter = TaskStatusReporter(redis_client=self.fsm.redis)

        self._reporters[user_id] = reporter
        try:
            fn = getattr(reporter, "set_user", None)
            if callable(fn):
                fn(user_id)
            else:
                setattr(reporter, "user_id", user_id)
        except Exception:
            pass

        # 若外部未提供 clients，尝试按健康态自动准备一批（最小改动）
        if not clients:
            try:
                clients = await unified_prepare_and_select_clients(
                    user_id,
                    validator=self.validator,
                    log_prefix="scheduler.start",
                )
            except Exception as e:
                log_warning("client_autoprepare_failed", extra={"user_id": user_id, "err": str(e)})
                clients = {}

        if not clients:
            try:
                await reporter.notify_no_valid_clients(user_id, to_user_id=recv_uid)
            except Exception:
                log_warning("notify_no_valid_clients 发送失败", extra={"user_id": user_id})
            return

        t = asyncio.create_task(
            self._start_internal(user_id, task, clients, reporter, to_user_id=recv_uid),
            name=f"sched-{user_id}-{trace_id}",
        )
        self._active_tasks[user_id] = t
        log_debug("🎯 已提交内部调度任务", extra={"user_id": user_id, "trace_id": trace_id})

        try:
            self._ensure_reassign_loop(user_id, reporter)
        except Exception as e:
            log_warning("reassign_loop_start_failed", extra={"user_id": user_id, "err": str(e)})

    async def run_queue_once(self):
        started = 0
        while True:
            item = self.task_queue.try_pop_nowait()
            if not item:
                break
            if item.reporter:
                self._reporters[item.user_id] = item.reporter
            await self.start(
                user_id=item.user_id,
                task=item.task,
                clients=item.clients,
                event=item.event,
                reporter=item.reporter,
                to_user_id=(item.to_user_id or item.user_id),
            )
            t = self._active_tasks.get(item.user_id)
            if t:
                t.add_done_callback(lambda _: self.task_queue.task_done())
            started += 1
        if started > 0:
            log_info("📦 队列批量启动完成", extra={"count": started})

    async def stop(self, user_id: int) -> None:
        task = self._active_tasks.pop(user_id, None)
        reporter = self._reporters.get(user_id)
        trace_id = get_trace_id()
        set_log_context({"user_id": user_id, "trace_id": trace_id})

        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                log_info("✅ 任务取消完成", extra={"user_id": user_id})

        try:
            await self.fsm.update_status(user_id, TaskStatus.STOPPED)
            await self.fsm.auto_update_stage(user_id)
        except Exception as e:
            log_exception("停止时状态/阶段更新失败", exc=e, extra={"user_id": user_id})

        try:
            if reporter:
                await reporter.update_status(user_id, TaskStatus.STOPPED)
        except Exception:
            pass

        self.executors.pop(user_id, None)
        t = self._reassign_loops.pop(user_id, None)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                log_debug("reassign_loop_cancelled", extra={"user_id": user_id})
        try:
            await self.fsm.remove_active_task(user_id)
        except Exception:
            pass
        # 清理 FloodController
        try:
            self._floods.pop(user_id, None)
        except Exception:
            pass
        log_info("🧹 stop() 完成", extra={"user_id": user_id})

    async def _start_internal(
        self,
        user_id: int,
        task: SendTask,
        clients: Dict[str, TelegramClient],
        reporter: TaskStatusReporter,
        *,
        to_user_id: Optional[int] = None,
    ) -> None:
        recv_uid = int(to_user_id or user_id)
        ctx = {"user_id": user_id, "task_id": getattr(task, "task_id", "-"), "trace_id": get_trace_id()}
        set_log_context(ctx)

        # —— 健康态优先：对传入/已备好的 clients 做一次统一排序/过滤（同级打散；最小改动） —— #
        try:
            phones_whitelist = list((clients or {}).keys()) or None
            selected = await unified_prepare_and_select_clients(
                user_id,
                client_manager=self.client_manager,  # ← 必传
                prefer_ok=True,
                allow_unknown_fallback=True,
                exclude_bad=True,
                shuffle_within_tier=True,
                max_pick=MAX_CONCURRENCY,
                log_prefix="selection",
            )
            if selected:
                clients = selected
        except Exception as e:
            log_warning("unified_select_clients_failed", extra={"user_id": user_id, "err": str(e)})

        # 预热对话缓存，降低后续解析/发送的首包延迟
        try:
            await asyncio.gather(
                *(ensure_entity_seen(c, dialogs_limit=ENTITY_WARM_DIALOGS_LIMIT) for c in (clients or {}).values()),
                return_exceptions=True
            )
            log_debug("entity_cache_warm_done", extra={"user_id": user_id, "clients": len(clients)})
        except Exception:
            log_debug("entity_cache_warm_skip", extra={"user_id": user_id})

        executor = TaskExecutor(
            fsm=self.fsm,
            reporter=reporter,
            group_assigner=self.assigner_factory(user_id),
            concurrency_limit=self.max_concurrency,
            sender_factory=self.sender_factory,
            flood=self._get_flood(user_id),
        )
        self.executors[user_id] = executor

        try:
            await self.fsm.update_status(user_id, TaskStatus.RUNNING)
            try:
                await reporter.update_status(user_id, TaskStatus.RUNNING)
            except Exception:
                log_warning("reporter.update_status 失败（忽略）", extra={"user_id": user_id})
            await self.fsm.auto_update_stage(user_id)

            groups = list(getattr(task, "group_list", []) or [])
            log_info(
                "🚀【群发任务】任务启动",
                extra={**ctx, "clients": len(clients), "groups": len(groups), "max_concurrency": self.max_concurrency},
            )

            if not groups:
                try:
                    await reporter.notify_no_groups(user_id, to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_no_groups 发送失败", extra={"user_id": user_id})
                return

            tracker = ResultTracker(user_id=user_id, reporter=reporter)
            round_no = 1

            log_debug("直接 prepare_assignment（已禁用 recover_joined）", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
            try:
                joined = await executor.prepare_assignment(user_id, clients, task, to_user_id=recv_uid)
            except Exception as e:
                log_exception("prepare_assignment 抛出异常（被捕获）", exc=e, extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
                joined = {}

            if not joined:
                try:
                    ga = getattr(executor, "group_assigner", None)
                    last_assign = getattr(ga, "_last_assignments", None) if ga is not None else None
                    last_joined = getattr(ga, "_last_joined", None) if ga is not None else None
                    last_already = getattr(ga, "_last_already", None) if ga is not None else None
                except Exception:
                    last_assign = last_joined = last_already = None

                try:
                    reporter_results = getattr(reporter, "results_by_user", None)
                    rep_sample = reporter_results.get(user_id, []) if isinstance(reporter_results, dict) else None
                except Exception:
                    rep_sample = None

                log_warning(
                    "❗ 未生成 joined 列表：可能分配/入群未执行或全部失败",
                    extra={
                        "user_id": user_id,
                        "task_id": getattr(task, "task_id", None),
                        "trace_id": ctx.get("trace_id"),
                        "clients": list(clients.keys()),
                        "raw_groups_sample": list(getattr(task, "group_list", []) or [])[:10],
                        "group_assigner_last_assignments_present": bool(last_assign),
                        "group_assigner_last_joined_present": bool(last_joined),
                        "group_assigner_last_already_present": bool(last_already),
                        "reporter_present": bool(reporter),
                        "reporter_results_sample_len": len(rep_sample) if isinstance(rep_sample, list) else "N/A",
                    }
                )

                try:
                    if last_assign:
                        log_debug("group_assigner._last_assignments sample", extra={"user_id": user_id, "sample": {p: len(l) for p, l in list(last_assign.items())[:10]}})
                    if last_joined:
                        log_debug("group_assigner._last_joined sample", extra={"user_id": user_id, "sample": {p: len(l) for p, l in list(last_joined.items())[:10]}})
                    if last_already:
                        log_debug("group_assigner._last_already sample", extra={"user_id": user_id, "sample": {p: len(l) for p, l in list(last_already.items())[:10]}})
                    if rep_sample:
                        log_debug("reporter.results_by_user sample", extra={"user_id": user_id, "sample": rep_sample[:50]})
                except Exception:
                    log_debug("打印 diagnostic 快照失败", extra={"user_id": user_id})

                try:
                    await reporter.notify_assignment_fail(user_id, "无可用账号或全部进群失败", to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_assignment_fail 发送失败（joined empty）", extra={"user_id": user_id})
                return

            log_debug("✅ 准备就绪，进入轮次循环", extra={**ctx, "joined": True})

            while True:
                try:
                    cur = await self.fsm.get_task_status(user_id)
                    if cur not in (TaskStatus.RUNNING, TaskStatus.PENDING):
                        log_info("⏹️ 外部状态不再运行，退出轮次循环", extra={"user_id": user_id, "status": str(cur)})
                        break
                except Exception:
                    log_warning("读取任务状态失败（继续本轮）", extra=ctx)

                ctx["round"] = round_no
                set_log_context(ctx)
                log_info("🔁 启动本轮群发", extra={"round": round_no, **ctx})

                result = await executor.execute_round(
                    user_id=user_id, clients=clients, task=task, round_no=round_no, to_user_id=recv_uid
                )
                if not isinstance(result, dict):
                    try:
                        await reporter.notify_task_error(user_id, "任务执行失败或未返回结果", "runner returned empty", to_user_id=recv_uid)
                    except Exception:
                        log_warning("notify_task_error 发送失败（runner empty）", extra={"user_id": user_id})
                    return

                for r in result.get("results", []) or []:
                    try:
                        tracker.track(r)
                    except Exception:
                        log_debug("tracker.track 失败（忽略）", extra={"user_id": user_id})

                try:
                    summ = tracker.summary()
                    log_info("📊 本轮执行结果汇总", extra={**summ, "user_id": user_id})
                except Exception as e:
                    log_exception("生成 tracker.summary 失败", exc=e, extra={"user_id": user_id})

                try:
                    await self.fsm.auto_update_stage(user_id)
                except Exception as e:
                    log_exception("FSM 阶段推进失败（轮次后）", exc=e, extra=ctx)

                try:
                    interval_seconds = max(1, int(await self.fsm.get_interval(user_id) or TASK_ROUND_INTERVAL_DEFAULT))
                except Exception:
                    interval_seconds = TASK_ROUND_INTERVAL_DEFAULT

                log_debug("⏳ 本轮结束，等待下一轮", extra={**ctx, "round": round_no, "sleep": interval_seconds})
                await asyncio.sleep(interval_seconds)
                round_no += 1

        except asyncio.CancelledError:
            log_info("🛑 调度任务被取消", extra=ctx)
        except Exception as e:
            log_exception("任务调度异常", exc=e, extra=ctx)
            try:
                await reporter.notify_task_error(user_id, "任务异常", e, to_user_id=recv_uid)
            except Exception:
                log_warning("notify_task_error 发送失败（exception）", extra={"user_id": user_id})
        finally:
            try:
                await self.fsm.auto_update_stage(user_id)
            except Exception as e:
                log_exception("FSM 阶段推进失败（finally）", exc=e, extra=ctx)

            self.executors.pop(user_id, None)
            self._active_tasks.pop(user_id, None)
            try:
                await self.fsm.update_status(user_id, TaskStatus.STOPPED)
            except Exception as e:
                log_exception("停止时更新状态失败", exc=e, extra=ctx)

            try:
                await reporter.update_status(user_id, TaskStatus.STOPPED)
                try:
                    await reporter.notify_task_stopped(user_id, task_id=str(getattr(task, "task_id", "-")), to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_task_stopped 发送失败（finally）", extra={"user_id": user_id})
            except Exception:
                pass

            try:
                await self.fsm.remove_active_task(user_id)
            except Exception:
                pass
            try:
                t = self._reassign_loops.pop(user_id, None)
                if t:
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        log_debug("reassign_loop_cancelled(finally)", extra={"user_id": user_id})
            except Exception:
                pass
            # 清理该用户的 FloodController（避免泄漏）
            try:
                self._floods.pop(user_id, None)
            except Exception:
                pass
            log_info("🏁 调度任务结束", extra=ctx)

    def _ensure_reassign_loop(self, user_id: int, reporter: TaskStatusReporter) -> None:
        t = self._reassign_loops.get(user_id)
        if t and not t.done():
            return
        loop = asyncio.create_task(
            self._reassign_pending_groups_loop(user_id, reporter),
            name=f"reassign-{user_id}"
        )
        self._reassign_loops[user_id] = loop

    async def _reassign_pending_groups_loop(
        self,
        user_id: int,
        reporter: TaskStatusReporter,
        *,
        interval: int = REASSIGN_LOOP_INTERVAL,
    ) -> None:
        # 绑定 Redis 客户端（来自 FSM）
        redis: Optional[Redis] = getattr(self.fsm, "redis", None)

        key_tpl = K_PENDING_REASSIGN
        key_fail_tpl = K_PENDING_REASSIGN + ":fails"
        random_jitter = (REASSIGN_JITTER_MIN, REASSIGN_JITTER_MAX)
        max_try_per_round = REASSIGN_MAX_TRY_PER_ROUND
        assigner_factory = self.assigner_factory

        log_debug("reassign_loop_started", extra={"user_id": user_id})
        try:
            while True:
                try:
                    try:
                        cur = await self.fsm.get_task_status(user_id)
                        if cur not in (TaskStatus.RUNNING, TaskStatus.PENDING):
                            log_debug("reassign_loop_exit_by_status", extra={"user_id": user_id, "status": str(cur)})
                            break
                    except Exception:
                        pass

                    # 无 redis 客户端时，休眠后重试
                    if not redis:
                        await asyncio.sleep(interval)
                        continue

                    # 从集合中取出 chat_key_raw（= cache_key），解析失败的直接剔除
                    raw = await redis.smembers(key_tpl.format(uid=user_id))
                    items: list[tuple[str, ParsedLink]] = []
                    for it in (raw or []):
                        chat_key_raw = it.decode("utf-8", errors="ignore") if isinstance(it, (bytes, bytearray)) else str(it)
                        chat_key_raw = (chat_key_raw or "").strip()
                        if not chat_key_raw:
                            continue
                        pl: Optional[ParsedLink] = None
                        try:
                            if hasattr(ParsedLink, "from_cache_key"):
                                # 优先从 cache_key 直接还原（如有）
                                pl = ParsedLink.from_cache_key(chat_key_raw)  # type: ignore[attr-defined]
                            else:
                                # 回落通用解析（要求能从 cache_key 再解析）
                                pl = ParsedLink.auto_parse(chat_key_raw)
                        except Exception:
                            pl = None
                        if pl is None:
                            # 无法解析：直接移出 pending，避免死循环
                            try:
                                await redis.srem(key_tpl.format(uid=user_id), chat_key_raw)
                            except Exception:
                                pass
                            continue
                        items.append((chat_key_raw, pl))

                    if not items:
                        await asyncio.sleep(interval)
                        continue

                    # 健康态优先准备/筛选（最小改动：若有已注册账号，则作为白名单，不深扫）
                    try:
                        existing = self.client_manager.get_clients_for_user(user_id) or {}
                        clients = await unified_prepare_and_select_clients(
                            user_id,
       
                            validator=self.validator,
                            phones_whitelist=list(existing.keys()) if existing else None,
                            deep_scan=not bool(existing),
                            log_prefix="reassign_loop.select",
                        )
                    except Exception:
                        clients = {}
                    if not clients:
                        await asyncio.sleep(interval)
                        continue

                    phones = list(clients.keys())
                    if not phones:
                        await asyncio.sleep(interval)
                        continue

                    random.shuffle(items)
                    try_count = min(max_try_per_round, len(items))
                    ga = assigner_factory(user_id)

                    for chat_key_raw, pl in items[:try_count]:
                        phone = random.choice(phones)
                        try:
                            assignment = {phone: [pl]}
                            sendable = await ga.execute_assignment(
                                user_id,
                                clients,
                                assignment,
                                reporter=reporter,
                                max_parallel_accounts=1,
                                to_user_id=user_id,
                                notify_summary=False,
                                flood=self._get_flood(user_id),
                            )
                            ok = any(
                                (pl2.to_link() == pl.to_link())
                                for plist in (sendable or {}).values()
                                for pl2 in plist
                            )
                            if ok:
                                try:
                                    # 成功：从 pending 集合中移除，并清掉失败计数
                                    await redis.srem(key_tpl.format(uid=user_id), chat_key_raw)
                                    await redis.hdel(key_fail_tpl.format(uid=user_id), chat_key_raw)
                                except Exception:
                                    pass
                                log_info("reassign_ok", extra={"user_id": user_id, "phone": phone, "group": pl.to_link()})
                            else:
                                log_debug("reassign_try_failed", extra={"user_id": user_id, "phone": phone, "group": pl.to_link()})
                                # 失败计数：限制重试次数（≥3 次则丢弃）
                                try:
                                    fkey = key_fail_tpl.format(uid=user_id)
                                    c = await redis.hincrby(fkey, chat_key_raw, 1)
                                    if c >= 3:
                                        await redis.srem(key_tpl.format(uid=user_id), chat_key_raw)
                                        await redis.hdel(fkey, chat_key_raw)
                                        log_warning("reassign_drop_by_retry_limit", extra={"user_id": user_id, "group": pl.to_link(), "tries": c})
                                except Exception:
                                    pass

                        except Exception as e:
                            log_warning("reassign_try_exception", extra={"user_id": user_id, "group": pl.to_link(), "err": str(e)})
                        finally:
                            await asyncio.sleep(random.uniform(*random_jitter))

                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log_warning("reassign_loop_error", extra={"user_id": user_id, "err": str(e)})
                    await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log_debug("reassign_loop_stopped", extra={"user_id": user_id})
            return
