# handlers/commands/interval_command.py
# -*- coding: utf-8 -*-
import asyncio
from telethon.errors.common import AlreadyInConversationError
from telethon import Button

from core.decorators import super_command
from core.defaults.bot_utils import BotUtils
from tg.conversation_flow import ConversationFlow
from unified.config import RUNNER_TIMEOUT
from unified.context import get_fsm
from unified.logger import log_exception, log_info, log_warning
from unified.trace_context import inject_trace_context
from unified.lock_utils import _inproc_lock
from ui.constants import ACCOUNTS_OPEN

PROMPT = "⏱ 请输入发送间隔（分钟，<b>1~10000</b> 的整数）"
RETRY = "❌ 无效输入，请输入 <b>1~10000</b> 的整数（分钟）"
MAX_RETRY = 3

async def _ask_interval_with_validation(flow: ConversationFlow, conv) -> int | None:
    for i in range(MAX_RETRY):
        mins = await flow.ask_number(PROMPT, retry=RETRY, step_timeout=300)
        if mins is None:
            await flow.cancel("🚫 输入无效，已取消设置")
            return None
        if 1 <= int(mins) <= 10000:
            return int(mins)
        await conv.send_message(
            RETRY,
            parse_mode="html",
            buttons=[[Button.inline("❌ 取消", data=b"cancel_conv")]],
        )
    await flow.cancel("🚫 重试次数过多，已取消设置")
    return None

@super_command(trace_action="设置发送间隔", white_only=True)
async def handle_interval_input(event):
    fsm = get_fsm()
    if fsm is None:
        return

    user_id = event.sender_id
    chat_id = event.chat_id
    inject_trace_context("handle_interval_input", user_id=user_id)

    async def run_once():
        async with _inproc_lock(f"conv:{chat_id}"):
            async with event.client.conversation(chat_id, timeout=RUNNER_TIMEOUT, exclusive=True) as conv:
                flow = ConversationFlow(conv, default_parse_mode="html")
                mins = await _ask_interval_with_validation(flow, conv)
                if mins is None:
                    return
                await fsm.set_interval(user_id, mins)
                await conv.send_message(f"✅ 已设置发送间隔：{mins} 分钟")

    try:
        await run_once()
        from unified.accounts_ui import build_task_menu
        menu_text, menu_buttons = build_task_menu(ACCOUNTS_OPEN)
        markup = event.client.build_reply_markup(menu_buttons)
        await BotUtils.safe_respond(event, event.client, menu_text, buttons=markup,parse_mode="html",  link_preview=False)
    except AlreadyInConversationError:
        ret = event.client.conversation(chat_id).cancel_all()
        if asyncio.iscoroutine(ret):
            await ret
        await run_once()

def register_commands():
    from router.command_router import main_router
    main_router.command("/c", trace_name="设置发送间隔", white_only=True)(handle_interval_input)
    return True
