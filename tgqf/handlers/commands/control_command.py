# -*- coding: utf-8 -*-
# handlers/commands/control_command.py
from __future__ import annotations

from typing import Optional

from core.decorators import super_command
from core.defaults.bot_utils import BotUtils
from core.task_control import TaskControl
from unified.context import get_client_manager, get_fsm, get_scheduler, get_task_control
from unified.logger import log_exception, log_info
from unified.trace_context import generate_trace_id, get_log_context, set_log_context


@super_command(trace_action="启动任务", white_only=True, session_lock=True)
async def start_task(event):
    """
    /ok：只做调度入口分发，不在命令层做客户端准备/并发控制。
    有 TaskControl → 交给 TaskControl（其内部会准备客户端、调用 BaseScheduler.start）
    无 TaskControl → 交给 BaseScheduler.start_user_task（其内部做并发与幂等占用）
    """
    user_id = event.sender_id
    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "cmd": "/ok", "trace_id": trace_id})

    ack = None
    try:
        ack = await event.respond("🚀 好的，马上开始…")
    except Exception:
        pass

    fsm = get_fsm()
    scheduler = get_scheduler()
    client_manager = get_client_manager()

    if not (fsm and scheduler and client_manager):
        await BotUtils.safe_respond(event, event.client, "⚠️ 系统尚未初始化，请稍后重试")
        return

    # 轻微节流（如有提供）
    try:
        if getattr(
            client_manager, "is_user_in_cooldown", None
        ) and client_manager.is_user_in_cooldown(user_id):
            await BotUtils.safe_respond(event, event.client, "⏱️ 操作太频繁，请 5 秒后重试")
            return
    except Exception:
        pass

    try:
        # 优先通过 TaskControl（其自身会做账号准备与启动）
        tc: Optional[TaskControl] = (
            get_task_control() if callable(get_task_control) else None
        )
        if tc:
            await tc.manage_task("start", user_id, event, clients=None)
        else:
            # 退化路径：统一走 BaseScheduler.start_user_task（内部含并发限制与队列/幂等占用）
            await scheduler.start_user_task(user_id, event=event)

        if ack:
            try:
                await ack.edit("🧭 群发任务执行中...")
            except Exception:
                pass

    except Exception as e:
        log_exception("❌ /ok 命令处理异常", exc=e, extra=get_log_context())
        await BotUtils.safe_respond(event, event.client, f"❌ 启动失败：{e}")


@super_command(white_only=True)
async def stop_task(event):
    """
    /stop：优先 TaskControl；否则直接调度器 stop。
    """
    user_id = event.sender_id
    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "cmd": "/stop", "trace_id": trace_id})
    log_info("收到 /stop 停止任务指令", extra=get_log_context())

    try:
        tc: Optional[TaskControl] = (
            get_task_control() if callable(get_task_control) else None
        )
        if tc:
            await tc.manage_task("stop", user_id, event)
            return

        scheduler = get_scheduler()
        if not scheduler:
            await BotUtils.safe_respond(event, event.client, "⚠️ 系统尚未初始化，请稍后重试")
            return

        await scheduler.stop(user_id=user_id)
        await BotUtils.safe_respond(event, event.client, "🛑 已停止当前任务")

    except Exception as e:
        log_exception("❌ 停止任务异常", exc=e, extra=get_log_context())
        await BotUtils.safe_respond(event, event.client, f"❌ 停止任务失败：{e}")


@super_command(white_only=True)
async def reset_task(event):
    """
    /p：只做状态清理与单条提示，避免 fsm.reset 内外重复回消息。
    """
    user_id = event.sender_id
    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "cmd": "/p", "trace_id": trace_id})
    log_info("收到 /p 清空任务指令", extra=get_log_context())

    fsm = get_fsm()
    if not fsm:
        await BotUtils.safe_respond(event, event.client, "⚠️ 系统尚未初始化，请稍后重试")
        return

    try:
        # 不传 event，避免 reset 内部回消息导致重复提示
        await fsm.reset(user_id)
        log_info("状态已重置", extra=get_log_context())
        await BotUtils.safe_respond(event, event.client, "🧹 状态已清空，可重新使用 /a /b /c 配置任务")
    except Exception as e:
        log_exception("❌ 重置任务异常", exc=e, extra=get_log_context())
        await BotUtils.safe_respond(event, event.client, f"❌ 状态重置失败：{e}")


def init_control_command(control: TaskControl):
    from unified.context import set_task_control

    set_task_control(control)


def register_commands():
    from router.command_router import main_router

    main_router.command("/ok", trace_name="启动任务", white_only=True)(start_task)
    main_router.command("/stop", trace_name="停止任务", white_only=True)(stop_task)
    main_router.command("/p", trace_name="清除任务配置", white_only=True)(reset_task)
    return True
