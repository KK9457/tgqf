# -*- coding: utf-8 -*-
# handlers/registry.py
from __future__ import annotations
from telethon import TelegramClient, events
from core.registry_guard import (
    mark_callbacks_registered, mark_client_registered, mark_commands_registered, safe_add_event_handler,
)
from handlers.commands import (
    accounts_command, admin_command, channel_command, control_command, group_command, help_command,
    interval_command, message_command, report_command, start_command, upload_command,
)
from router.command_router import main_router
from unified.logger import log_exception, log_debug, log_warning
from ui.constants import PATTERN_WHITE
import os

# ★ 引入全局 cancel_conv 注册函数
from tg.conversation_flow import register_cancel_callback


async def _debug_log_all_callbacks(event):
    try:
        data = event.data or b""
        log_debug("🔎 DEBUG: 捕获任意 CallbackQuery", extra={
            "sender_id": getattr(event, "sender_id", None),
            "chat_id": getattr(event, "chat_id", None),
            "data": data[:200],
            "len": len(data) if isinstance(data, (bytes, bytearray)) else None,
            "module": __name__, "signal": "callback"
        })
        try:
            await event.answer()
        except Exception:
            pass
    except Exception as e:
        log_exception("DEBUG CallbackQuery 记录失败", exc=e)


def register_all(bot: TelegramClient) -> None:
    if not mark_client_registered(bot):
        log_warning("handlers/registry: 此 client 已整体注册，跳过")
        return

    if mark_commands_registered(bot, "commands"):
        try:
            start_command.register_commands()
            group_command.register_commands()
            control_command.register_commands()
            help_command.register_commands()
            interval_command.register_commands()
            message_command.register_commands()
            admin_command.register_commands(bot=bot)
            upload_command.register_commands()
            channel_command.register_commands()
            report_command.register_commands()
            accounts_command.register_commands()

            # ✅ 消息类型/转发方式回调（Telethon）
            try:
                message_command.register_callbacks(bot)
            except Exception as e:
                log_exception("❌ message_command 回调注册失败", exc=e)

            main_router.register_to(bot)
            log_debug("✅ 所有命令已注册（handlers/registry）")
        except Exception as e:
            log_exception("❌ 命令注册失败", exc=e)

    # —— 全局 CallbackQuery 调试探针（一次挂好就行）——
    try:
        ok = safe_add_event_handler(
            bot,
            _debug_log_all_callbacks,
            events.CallbackQuery(),    # 捕获所有回调
            tag="debug:cb:all",
        )
        if ok:
            log_debug("🟡 DEBUG: 全局 CallbackQuery 调试探针已挂载")
        else:
            log_debug("🟡 DEBUG: 全局 CallbackQuery 调试探针已存在（幂等跳过）")
    except Exception as e:
        log_exception("❌ 挂载全局回调调试探针失败", exc=e)

    if mark_callbacks_registered(bot, "upload_and_accounts_v2"):
        upload_command.register_callbacks(bot)
        accounts_command.register_callbacks(bot)
        log_debug("✅ 上传/账号管理 回调已注册")
    else:
        log_debug("⚠️ upload_and_accounts_v2 已注册过，跳过本轮挂载")

    if mark_callbacks_registered(bot, "white_panel"):
        try:
            from unified.white_panel import handle_white_callback
            ok = safe_add_event_handler(
                bot,
                handle_white_callback,
                events.CallbackQuery(pattern=PATTERN_WHITE),  # 统一正则
                tag="callbacks:white_panel",
            )
            if ok:
                log_debug("✅ 白名单回调已注册")
            else:
                log_debug("⚠️ 白名单回调已存在（幂等跳过）")
        except Exception as e:
            log_exception("❌ 白名单回调注册失败", exc=e)

    # ★ 新增：举报回调
    if mark_callbacks_registered(bot, "report"):
        try:
            report_command.register_callbacks(bot)
            log_debug("✅ 举报回调已注册")
        except Exception as e:
            log_exception("❌ 举报回调注册失败", exc=e)
    try:
        group_command.register_callbacks(bot)
    except Exception as e:
        log_exception("❌ group_command 回调注册失败", exc=e)
    # —— 全局 cancel_conv 回调 —— #
    try:
        ok = register_cancel_callback(bot)
        if ok:
            log_debug("✅ 全局 cancel_conv 回调已注册")
        else:
            log_debug("⚠️ cancel_conv 回调已存在（幂等跳过）")
    except Exception as e:
        log_exception("❌ 注册 cancel_conv 回调失败", exc=e)

    # —— 可选：开启全局回调调试（环境变量控制）—— #
    if os.environ.get("DEBUG_CB_LOG") == "1":
        try:
            ok2 = safe_add_event_handler(
                bot,
                _debug_log_all_callbacks,
                events.CallbackQuery(),   # 捕获所有回调
                tag="debug:cb:all",
            )
            if ok2:
                log_debug("🟡 DEBUG: 全局 CallbackQuery 调试探针已挂载（DEBUG_CB_LOG=1）")
            else:
                log_debug("🟡 DEBUG: 全局 CallbackQuery 调试探针已存在（DEBUG_CB_LOG=1，幂等跳过）")
        except Exception as e:
            log_exception("❌ 挂载全局回调调试探针失败（DEBUG_CB_LOG=1）", exc=e)
