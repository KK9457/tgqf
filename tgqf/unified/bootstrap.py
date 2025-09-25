# unified/bootstrap.py
from __future__ import annotations

from typing import Optional
import os
import asyncio

from redis.asyncio import Redis
from core.defaults.client_manager import SessionClientManager
from core.defaults.group_assigner import BalancedGroupAssigner
from core.defaults.message_sender import MessageSender
from core.defaults.reporter_factory import ReporterFactory
from core.redis_fsm import RedisCommandFSM
from core.task_control import TaskControl
from core.task_status_reporter import TaskStatusReporter
from scheduler.base_scheduler import BaseScheduler
from unified.config import MAX_CONCURRENCY, SESSION_DIR, ensure_all_dirs
from unified.context import (
    set_client_manager,
    set_fsm,
    set_redis,
    set_reporter,
    set_reporter_factory,
    set_scheduler,
    set_task_control,
)
from unified.logger import log_debug, log_info, log_warning, log_exception, log_error
from typess.message_enums import TaskStatus

__all__ = ["Bootstrap"]


class Bootstrap:
    """
    👷 项目启动器
    自动初始化 FSM、Scheduler、ClientManager、TaskControl、Reporter 等核心依赖，
    并把它们注入全局上下文（unified.context）。
    """

    fsm: RedisCommandFSM
    redis: Redis
    client_manager: SessionClientManager
    scheduler: BaseScheduler
    reporter: TaskStatusReporter
    task_control: Optional[TaskControl]

    def __init__(
        self,
        session_dir: str = SESSION_DIR,
        redis_url: str = "redis://localhost:6379/0",
        concurrency: int = MAX_CONCURRENCY,
        assigner_factory=None,
        sender_factory=None,
        *,
        register_commands: bool = False,
    ):
        log_debug(
            "🧱 Bootstrap 初始化启动...",
            extra={"session_dir": session_dir, "concurrency": int(concurrency)},
        )

        self.task_control = None  # 先占位，避免属性不存在

        # ---- 0) sanity checks ----
        try:
            ensure_all_dirs()
        except Exception:
            pass

        # 1) Redis & FSM
        try:
            self.redis = Redis.from_url(redis_url, decode_responses=True)
            set_redis(self.redis)
            self.fsm = RedisCommandFSM(self.redis)
            set_fsm(self.fsm)
            log_debug("✅ Redis 和 FSM 初始化完成", extra={"redis_url": redis_url})
        except Exception as e:
            log_exception("Redis/FSM 初始化失败", exc=e, extra={"redis_url": redis_url})
            self.redis = None  # type: ignore[assignment]
            self.fsm = None    # type: ignore[assignment]

        # 2) Reporter / ClientManager
        try:
            self.reporter = TaskStatusReporter(redis_client=self.redis)
            set_reporter(self.reporter)
            log_debug("✅ TaskStatusReporter 初始化完成", extra={})
        except Exception as e:
            log_exception("初始化 TaskStatusReporter 失败，创建一个空占位 reporter（继续）", exc=e)
            self.reporter = TaskStatusReporter(redis_client=None)
            set_reporter(self.reporter)

        try:
            self.client_manager = SessionClientManager(session_dir=session_dir)
            set_client_manager(self.client_manager)
            log_debug("✅ 客户端管理器初始化完成", extra={"session_dir": session_dir})
        except Exception as e:
            log_exception("SessionClientManager 初始化失败", exc=e, extra={"session_dir": session_dir})
            self.client_manager = SessionClientManager(session_dir=session_dir)
            set_client_manager(self.client_manager)

        # 3) 工厂
        self.assigner_factory = assigner_factory or (lambda uid: BalancedGroupAssigner())

        self.sender_factory = sender_factory or (lambda uid, client: MessageSender(client=client, phone=getattr(client, "phone", ""), user_id=uid))
        # 3) ReporterFactory 持有 Redis/Notify Bot
        try:
            self.reporter_factory = ReporterFactory(redis=self.redis)
            set_reporter_factory(self.reporter_factory)
            log_debug("✅ ReporterFactory 初始化完成", extra={"reporter_factory": str(type(self.reporter_factory))})
            try:
                _ = self.reporter_factory.create()
                log_debug("ReporterFactory.create() 自检通过", extra={})
            except Exception as e:
                log_warning("ReporterFactory.create() 自检失败（继续）", extra={"err": str(e)})
        except Exception as e:
            log_exception("ReporterFactory 初始化失败（继续）", exc=e)
            self.reporter_factory = None  # type: ignore[assignment]
            set_reporter_factory(self.reporter_factory)

        # 4) BaseScheduler（核心调度）
        try:
            self.scheduler = BaseScheduler(
                fsm=self.fsm,
                client_manager=self.client_manager,
                assigner_factory=self.assigner_factory,
                sender_factory=self.sender_factory,
                reporter_factory=self.reporter_factory,
                max_concurrency=concurrency,
            )
            set_scheduler(self.scheduler)
            log_debug("✅ BaseScheduler 初始化完成", extra={"max_concurrency": int(concurrency)})
        except Exception as e:
            log_exception("BaseScheduler 初始化失败（以兜底对象继续）", exc=e, extra={"concurrency": concurrency})
            self.scheduler = BaseScheduler(
                fsm=self.fsm,
                client_manager=self.client_manager,
                assigner_factory=self.assigner_factory,
                sender_factory=self.sender_factory,
                reporter_factory=self.reporter_factory,
                max_concurrency=concurrency,
            )
            set_scheduler(self.scheduler)

        # 5) TaskControl —— 必须先创建，再注入 context
        try:
            self.task_control = TaskControl(
                fsm=self.fsm,
                scheduler=self.scheduler,
                client_manager=self.client_manager,
                reporter=self.reporter,
                reporter_factory=self.reporter_factory,
            )
            set_task_control(self.task_control)
            log_debug("✅ TaskControl 初始化完成", extra={"TaskControl": str(type(self.task_control))})
        except Exception as e:
            log_exception("TaskControl 初始化失败（继续）", exc=e)
            self.task_control = None
            set_task_control(self.task_control)

        # 6) 上下文注入完成
        log_debug("✅ 全局上下文注入完成（set_*）")

        # 7) 可选命令注册（通常不在这里做；统一在 handlers/registry.register_all）
        if register_commands:
            try:
                from handlers.commands import control_command, group_command, start_command
                start_command.init_fsm(self.fsm)
                group_command.init_group_command(self.fsm)
                control_command.init_control_command(self.task_control)
                log_debug("✅ 所有命令已注册（由 Bootstrap）")
            except Exception as e:
                log_warning("⚠️ 命令初始化出错", extra={"error": str(e)})

        # 8) 告知 client_manager 当前调度器（便于其内部回调/健康检查等）
        try:
            if hasattr(self.client_manager, "register_scheduler"):
                self.client_manager.register_scheduler(self.scheduler)
                log_debug("✅ client_manager.register_scheduler 调用成功")
            else:
                log_warning("client_manager 不支持 register_scheduler", extra={"client_manager": str(type(self.client_manager))})
        except Exception as e:
            log_warning("⚠️ 注册调度器到 client_manager 失败", extra={"error": str(e)})

        log_debug("🟢 Bootstrap 完成：FSM、Scheduler、TaskControl、ClientManager 全部就绪")
        log_debug(
            "对象摘要",
            extra={
                "FSM": id(self.fsm) if self.fsm is not None else None,
                "Scheduler": id(self.scheduler) if self.scheduler is not None else None,
                "TaskControl": id(self.task_control) if self.task_control is not None else None,
            },
        )

    async def resume_pending_tasks(self) -> None:
        """
        重启后自动恢复未完成任务：
        - 读取活跃任务列表
        - 将 RUNNING 修正为 PENDING
        - 为 PENDING 任务加载所有客户端并交给调度器启动
        """
        fsm = self.fsm
        cm = self.client_manager
        sched = self.scheduler

        user_ids = await fsm.get_all_active_tasks()
        log_info(f"检测到有 {len(user_ids)} 个未完成任务，自动恢复...", extra={"user_ids": user_ids})

        async def try_resume(uid: int):
            try:
                status = await fsm.get_task_status(uid)
                if status is None:
                    log_debug("用户无任务状态，跳过", extra={"user_id": uid})
                    return

                # 兼容字符串
                if isinstance(status, str):
                    try:
                        status = TaskStatus(status)
                    except Exception:
                        pass

                if status == TaskStatus.RUNNING:
                    log_warning("状态为 RUNNING，自动修正为 PENDING", extra={"user_id": uid})
                    await fsm.update_status(uid, TaskStatus.PENDING)
                    status = TaskStatus.PENDING

                if status == TaskStatus.PENDING:
                    task_obj = await fsm.get_state(uid)
                    await cm.load_all_for_user(uid)
                    clients = cm.get_clients_for_user(uid) or {}
                    if not clients:
                        log_warning("无可用客户端，无法恢复", extra={"user_id": uid})
                        return
                    log_info("恢复任务...", extra={"user_id": uid, "clients": list(clients.keys())})
                    await sched.start(user_id=uid, task=task_obj, clients=clients, event=None)
                else:
                    log_debug("状态无需恢复", extra={"user_id": uid, "status": getattr(status, "value", str(status))})
            except Exception as e:
                log_error("恢复任务失败", extra={"user_id": uid, "err": str(e)})

        await asyncio.gather(*(try_resume(uid) for uid in user_ids), return_exceptions=True)
