# core/task_control.py
# -*- coding: utf-8 -*-
"""
入口控制层：不直接做并发，统一委托 BaseScheduler → TaskExecutor。
"""
from __future__ import annotations

from typing import Dict, Any, Optional, Union

from typess.message_enums import TaskStatus, SendTask  # noqa: F401  # 可能在其它扩展处使用
from core.redis_fsm import RedisCommandFSM
from core.task_status_reporter import TaskStatusReporter
from scheduler.base_scheduler import BaseScheduler
from core.defaults.bot_utils import BotUtils
from unified.logger import log_info, log_exception, log_debug, log_warning
from unified.trace_context import set_trace_context, get_trace_id


class TaskControl:
    def __init__(
        self,
        fsm: RedisCommandFSM,
        scheduler: "BaseScheduler",
        client_manager,
        reporter: TaskStatusReporter | None,
        reporter_factory: Any | None = None,  # 预留：外部注入 Reporter
    ):
        self.fsm = fsm
        self.scheduler = scheduler
        self.client_manager = client_manager
        self.reporter = reporter  # 建议由 ReporterFactory.create(event) 注入

    # --------- 工具 ---------
    @staticmethod
    def _normalize_action(action: str) -> str:
        a = (action or "").strip().lower()
        # 停止
        if a in {"stop", "/stop", "stop_task", "task:stop", "stopped", "stoped", "halt", "terminate"}:
            return "stop"
        # 启动
        if a in {"start", "/ok", "run", "go"}:
            return "start"
        # 恢复（resume/recover 等价，这里走与 start 相同流程）
        if a in {"recover", "resume", "continue"}:
            return "recover"
        # 暂停（仅写 FSM，调度会在轮询中停止推进）
        if a in {"pause", "paused"}:
            return "pause"
        # 重置
        if a in {"reset", "/reset", "clear", "init"}:
            return "reset"
        return a

    async def _prepare_clients(self, user_id: int, *, log_prefix: str) -> Dict[str, Any]:
        """
        统一账号准备，透传 scheduler.validator 以保持一致。
        """
        validator = getattr(self.scheduler, "validator", None)
        clients = await self.client_manager.prepare_clients_for_user(
            user_id, validator=validator, log_prefix=log_prefix
        )
        return clients or {}

    async def _respond(self, user_id: int, event, text: str, *, html: bool = True) -> None:
        """
        事件回包（显式贯穿 user_id，便于上层审计/调用一致性）
        """
        try:
            # 这里直接响应 event，不走 reporter；event 通道天然只回当前用户
            await BotUtils.safe_respond(event, event.client, text, target_user_id=user_id)
        except Exception:
            pass

    async def _notify_error(self, user_id: int, event, title: str, err: Union[Exception, str, None]) -> None:
        """
        UI 提示 + Reporter 记录（显式 user_id / to_user_id=user_id）
        """
        await self._respond(user_id, event, title)
        try:
            if self.reporter:
                await self.reporter.notify_task_error(user_id, title, err, to_user_id=user_id)
        except Exception:
            pass

    # --------- 统一入口 ---------
    async def manage_task(
        self,
        action: str,
        user_id: int,
        event,
        clients: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        高层入口：根据 action 分流。
        """
        action = self._normalize_action(action)
        set_trace_context(user_id=user_id)
        log_debug("TaskControl.manage_task", extra={"user_id": user_id, "action": action, "trace_id": get_trace_id()})

        try:
            if action == "stop":
                await self.stop_task(user_id, event)
                return

            if action == "pause":
                await self.pause_task(user_id, event)
                return

            if action == "reset":
                await self.reset(user_id, event)
                return

            # 需要任务 + 客户端的动作
            task: SendTask = await self.fsm.to_task(user_id)
            if not task:
                await self._respond(user_id, event, "⚠️ 未找到任务配置，请先完成 /a /b 配置流程。")
                return

            if clients is None:
                clients = await self._prepare_clients(user_id, log_prefix=f"TaskControl:{action}")

            if not clients:
                await self._respond(user_id, event, "⚠️ 无可用账号，请先上传 session 或修复。")
                try:
                    if self.reporter:
                        await self.reporter.notify_no_valid_clients(user_id, to_user_id=user_id)
                except Exception:
                    pass
                return

            if action == "start":
                await self.start_task(user_id, event, task, clients)
            elif action == "recover":
                # 恢复：与 start 等价（由调度器负责状态推进与轮次恢复）
                await self.start_task(user_id, event, task, clients)
            else:
                await self._respond(user_id, event, f"⚠️ 未知操作：{action}")

        except Exception as e:
            log_exception("操作失败", exc=e, extra={"user_id": user_id, "action": action})
            await self._notify_error(user_id, event, "❌ 操作失败，请稍后再试。", e)

    # --------- 具体动作 ---------
    async def start_task(
        self,
        user_id: int,
        event,
        task: SendTask,
        clients: Dict[str, Any]
    ) -> None:
        """
        启动/恢复统一走 BaseScheduler.start：
        - 由 BaseScheduler 负责：取消旧任务、设置 RUNNING、推进阶段、入队并执行。
        - 这里不再提前写 RUNNING，避免重复/竞态。
        """
        try:
            await self.scheduler.start(user_id=user_id, task=task, clients=clients, event=event, reporter=self.reporter)
            log_info("任务已提交到调度器", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
        except Exception as e:
            log_exception("任务启动失败", exc=e, extra={"user_id": user_id})
            await self._notify_error(user_id, event, "❌ 启动任务失败，请稍后再试。", e)

    async def stop_task(self, user_id: int, event) -> None:
        """
        停止任务：
        - 由 BaseScheduler.stop() 负责取消内部循环，并统一写 STOPPED。
        - 这里不重复写入 FSM/Reporter 状态，避免覆盖 finally 分支的结果。
        """
        try:
            await self.scheduler.stop(user_id)
            # 可选：展示菜单（有则调，无则忽略）
            try:
                from unified.accounts_ui import send_task_menu
                from ui.constants import ACCOUNTS_OPEN
                await send_task_menu(event, ACCOUNTS_OPEN)
            except Exception:
                pass
  
            # 可选同步通知（如需）：若需要外发卡片，可启用下面两行
            # if self.reporter:
            #     await self.reporter.notify_task_stopped(user_id, task_id=getattr(self.fsm, "task_id", "-"), to_user_id=user_id)
        except Exception as e:
            log_exception("任务停止失败", exc=e, extra={"user_id": user_id})
            await self._notify_error(user_id, event, "❌ 停止任务失败，请稍后再试。", e)

    async def pause_task(self, user_id: int, event) -> None:
        """
        暂停：仅写 FSM 为 PAUSED，调度器轮询到非 RUNNING 即自行退出下一轮。
        若需强制中断，可同步调用 scheduler.stop。
        """
        try:
            await self.fsm.pause_task(user_id)
            await self._respond(user_id, event, "⏸️ 任务已暂停")
        except Exception as e:
            log_exception("任务暂停失败", exc=e, extra={"user_id": user_id})
            await self._notify_error(user_id, event, "❌ 暂停任务失败，请稍后再试。", e)

    async def reset(self, user_id: int, event) -> None:
        """
        重置任务配置：
        - 清除状态/镜像/KV/metadata
        - Reporter 仅做状态镜像提示，不驱动调度
        """
        try:
            await self.fsm.reset(user_id)
            try:
                if self.reporter:
                    await self.reporter.update_status(user_id, "cleared")
            except Exception:
                pass
            await self._respond(user_id, event, "🧹 配置已重置，请重新开始 /a")
        except Exception as e:
            log_exception("任务重置失败", exc=e, extra={"user_id": user_id})
            await self._notify_error(user_id, event, "❌ 重置失败，请稍后再试。", e)

    # --------- 附加工具 ---------
    async def get_clients_for_user(self, user_id: int) -> Dict[str, Any]:
        """
        兼容旧调用：主动加载后返回注册的客户端。
        """
        try:
            await self.client_manager.load_all_for_user(user_id)
        except Exception:
            pass
        return self.client_manager.get_clients_for_user(user_id)
