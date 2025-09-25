# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import traceback
from unified.config import ensure_all_dirs
from core.bot_client import create_bot_client
from handlers.commands import control_command, group_command, start_command
from handlers.registry import register_all
from unified.bootstrap import Bootstrap
from unified.config import ensure_admin_ids
from unified.context import (
    get_fsm,
    set_bot,
    set_client_manager,
    set_white_user,
)
from unified.logger import log_debug, log_error, log_info, log_warning


async def main():
    # 0) 兜底：确保目录/日志文件存在
    try:
        ensure_all_dirs()
    except Exception:
        pass

    # 1) 创建并连接 bot（单例）
    try:
        bot = await create_bot_client()
    except Exception as e:
        log_error(f"❌ 无法初始化 Bot：{e}")
        return
    set_bot(bot)
    
    # 2) 初始化 logger（控制台 + TG 错误上报）

    from unified.logger import init_logger
    init_logger(
        to_console=True,
        tg_client=bot,
        tg_target=-1002652424988,  # 你的日志群/频道（请按需调整）
        tg_level=logging.ERROR,
    )

    # cryptg 检测（真实检测 import 结果）
    try:
        import cryptg  # noqa: F401
        log_debug("✅ 已检测到 cryptg，加密计算将加速")
    except Exception:
        log_warning("ℹ️ 未检测到 cryptg，文件传输/下载可能偏慢（可选 pip install cryptg）")

    # 3) 启动 Bootstrap（唯一来源：FSM/Redis/Scheduler/ClientManager）
    bootstrap = Bootstrap(register_commands=False)

    # 4) 解析管理员（支持 @username 或纯数字）
    try:
        await ensure_admin_ids(bot)
    except Exception as e:
        log_warning(f"⚠️ 管理员用户名解析失败，将仅使用数字ID：{e}")

    # 5) 给模块级命令做依赖注入
    fsm = get_fsm()
    group_command.init_group_command(fsm)
    start_command.init_fsm(fsm)
    control_command.init_control_command(bootstrap.task_control)

    set_white_user(fsm)
    set_client_manager(bootstrap.client_manager)

    # 6) 统一注册
    register_all(bot)

    # 7) 自动恢复未完成任务
    await bootstrap.resume_pending_tasks()

    log_info("🤖 Bot 已启动，开始监听指令与事件...")
    try:
        await bot.run_until_disconnected()
    finally:
        try:
            await bot.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("🛑 收到中断信号，Bot 已安全退出")
    except Exception as e:
        log_error(f"❌ 主程序异常终止: {e}")
        log_error(traceback.format_exc())