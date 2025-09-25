# handlers/commands/start_command.py
from __future__ import annotations

from telethon import Button

from core.decorators import super_command
from unified import context
from unified.context import get_client_manager
from ui.constants import ACCOUNTS_OPEN,TASKMENU_OPEN
from unified.logger import log_info, log_exception,log_debug


@super_command(trace_action="启动机器人", white_only=True)
async def handle_start(event):
    user_id = event.sender_id
    

    client_manager = get_client_manager()
    try:
        if client_manager:
            client_manager._ensure_session_dirs(user_id)
    except Exception:
        log_exception("start_command: ensure_session_dirs failed（继续）")

    buttons = [
        [Button.inline("📤 群发配置菜单", data=TASKMENU_OPEN)],
        [Button.inline("👥 账号管理", data=ACCOUNTS_OPEN)],
    ]
    log_debug("🔘 /start 按钮构造完成", extra={"rows": len(buttons), "has_accounts_open": True, "has_taskmenu_open": True})
    markup = event.client.build_reply_markup(buttons)

    # 更友好的拟态
    async with event.client.action(event.chat_id, "typing"):
        await event.respond(
            "🤖 KK群发神器准备就绪。\n"
            "• 使用 /help 查看全部命令\n"
            "• 或点击下方按钮直接进行操作",
            buttons=markup,
            link_preview=False,
        )


def register_commands():
    from router.command_router import main_router

    main_router.command("/start", trace_name="启动机器人", white_only=True)(
        handle_start
    )


# ✅ 外部注入 FSM（推荐方式）
def init_fsm(fsm_instance):
    context.set_fsm(fsm_instance)
