# -*- coding: utf-8 -*-
# handlers/commands/message_command.py
from __future__ import annotations

import asyncio
from telethon.errors.common import AlreadyInConversationError
from telethon import Button, events

from core.registry_guard import safe_add_event_handler
from core.decorators import super_command
from core.defaults.bot_utils import BotUtils
from unified.callback_proto import pack, unpack

from unified.context import get_fsm
from unified.logger import log_info, log_exception, log_warning, log_debug
from unified.trace_context import inject_trace_context
from unified.config import RUNNER_TIMEOUT
from unified.lock_utils import _inproc_lock

from typess.message_enums import MessageType, MessageContent
from unified.message_builders import MessageBuilder
from ui.constants import TASKMENU_OPEN, ACCOUNTS_OPEN


def _menu_text() -> str:
    return "🧾 <b>请选择消息类型：</b>\n"

def _mk_markup(client, buttons):
    # 优先用 Telethon 原生的二维数组；如果你们封装存在，则兜底兼容
    try:
        return client.build_reply_markup(buttons)
    except Exception:
        return buttons
def _menu_buttons_raw(user_id: int):
    return [
        [
            Button.inline("📝 文本消息", data=pack("task", "set_msg_type", user_id, "text")),
            Button.inline("🖼️ 图文消息", data=pack("task", "set_msg_type", user_id, "media")),
            Button.inline("🔁 转发消息", data=pack("task", "set_msg_type", user_id, "forward")),
        ],
        [Button.inline("📤 群发配置菜单", data=TASKMENU_OPEN)],
        [Button.inline("❌ 取消", data=b"cancel_conv")],
    ]


def _forward_mode_text() -> str:
    return "🔁 <b>请选择转发方式：</b>\n• <b>隐藏消息来源</b>\n• <b>显示消息来源</b>\n"


def _forward_mode_buttons_raw(user_id: int):
    return [
        [
            Button.inline("🙈 隐藏消息来源", data=pack("task", "set_fwd_mode", user_id, "hide")),
            Button.inline("👁️ 显示消息来源", data=pack("task", "set_fwd_mode", user_id, "show")),
        ],
        [Button.inline("📤 群发配置菜单", data=TASKMENU_OPEN)],
        [Button.inline("❌ 取消", data=b"cancel_conv")],
    ]


@super_command(trace_action="设置消息内容", white_only=True)
async def handle_message(event):
    fsm = get_fsm()
    if fsm is None:
        log_warning("FSM 未注入", extra={"user_id": event.sender_id})
        return
    user_id = event.sender_id
    inject_trace_context("handle_message", user_id=user_id, module=__name__)
    log_info("📩 已启动消息类型配置会话", extra={"user_id": user_id})
    try:
        async with event.client.action(event.chat_id, "typing"):
            buttons = _menu_buttons_raw(user_id)
  
            await BotUtils.safe_respond(
                event,
                event.client,
                _menu_text(),
                buttons=buttons,
                parse_mode="html", 
                link_preview=False,
            )

    except Exception as e:
        log_exception("❌ 打开配置菜单失败", exc=e, extra={"user_id": user_id})
        await BotUtils.safe_respond(
            event,event.client, "⚠️ 无法打开配置菜单，请稍后重试。",parse_mode="html",  link_preview=False
        )


async def _run_build_flow(event, mtype: MessageType):
    """进入对话流，构建消息，并写入 FSM"""
    from unified.accounts_ui import build_task_menu

    fsm = get_fsm()
    user_id = event.sender_id
    peer = event.chat_id or event.sender_id

    async def _do_flow():
        try:
            if hasattr(event, "answer"):
                await event.answer(cache_time=0)
        except Exception:
            pass

        async with event.client.action(peer, "typing"):
            async with _inproc_lock(f"conv:{peer}"):
                async with event.client.conversation(
                    peer, timeout=RUNNER_TIMEOUT, exclusive=True
                ) as conv:
                    log_info(
                        "🧱 进入消息构建会话",
                        extra={"user_id": user_id, "type": mtype.name},
                    )
                    mc: MessageContent = await MessageBuilder.interactive_build(
                        conv, mtype
                    )
                    if not mc:
                        await conv.send_message("🚫 消息未配置，操作已取消。", link_preview=False)
                        await BotUtils.safe_respond(event,event.client, "❌ 消息构建已取消或未完成。",parse_mode="html",  link_preview=False)
                        log_warning(
                            "消息构建返回空，已取消",
                            extra={"user_id": user_id, "type": mtype.name},
                        )
                        return False

                    # —— 读取/合并 metadata（转发方式）——
                    try:
                        meta = await fsm.get_metadata(user_id) if fsm else {}
                    except Exception:
                        meta = {}
                    forward_as_copy = bool((meta or {}).get("forward_as_copy", False))
                    if mtype == MessageType.FORWARD:
                        try:
                            mc.forward_as_copy = forward_as_copy
                            mc.forward_show_author = not forward_as_copy
                        except Exception:
                            pass

                    inject_trace_context(
                        "_run_build_flow", user_id=user_id, mtype=mtype.name
                    )
                    log_info(
                        "⚙️ 开始消息构建流程",
                        extra={
                            "user_id": user_id,
                            "message_type": mtype.name,
                            "forward_as_copy": forward_as_copy
                            if mtype == MessageType.FORWARD
                            else None,
                        },
                    )

                    # —— 写入 FSM（含 emoji_ids metadata 合并）——
                    emoji_ids = getattr(mc, "emoji_ids", [])
                    try:
                        old_meta = await fsm.get_metadata(user_id) or {}
                    except Exception:
                        old_meta = {}
                    merged = dict(old_meta)
                    merged.update({"emoji_ids": emoji_ids})

                    await fsm.set_message_content(user_id, mc)
                    await fsm.set_metadata(user_id, merged)
                    log_info(
                        "✅ 消息内容已写入 FSM",
                        extra={"user_id": user_id, "emoji_ids": len(emoji_ids)},
                    )

        menu_text, menu_buttons = build_task_menu(ACCOUNTS_OPEN)
        try:
            await BotUtils.safe_respond(event, event.client, menu_text, buttons=menu_buttons,parse_mode="html",  link_preview=False)
        except Exception:
            await BotUtils.safe_respond(event, event.client, menu_text,parse_mode="html",  link_preview=False)
        log_info("📤 群发配置菜单已推送", extra={"user_id": user_id})
        return True

    try:
        ok = await _do_flow()
    except AlreadyInConversationError:
        try:
            ret = event.client.conversation(peer).cancel_all()
            if asyncio.iscoroutine(ret):
                await ret
            ok = await _do_flow()
        except Exception as e:
            log_exception("❌ 配置消息内容失败", exc=e, extra={"user_id": user_id})
            ok = False
    except Exception as e:
        log_exception("❌ 配置消息内容失败", exc=e, extra={"user_id": user_id})
        ok = False

    return ok


async def handle_task_callbacks(event):
    """统一处理 task 命名空间的回调"""
    user_id = event.sender_id
    try:
        decoded = unpack(event.data)
        if not decoded:
            return
        ns, act, uid, args = decoded
        if ns != "task":
            return

        # ✅ 先消 loading
        try:
            await event.answer()
        except Exception:
            pass

        # 消息类型选择
        if act == "set_msg_type":
            arg = args[0] if args else None
            if arg == "text":
                await _run_build_flow(event, MessageType.TEXT)
            elif arg == "media":
                await _run_build_flow(event, MessageType.MEDIA)
            elif arg == "forward":
                # 打开发送方式选择菜单
                buttons = _forward_mode_buttons_raw(user_id)
                await BotUtils.safe_respond(
                    event, event.client, _forward_mode_text(),
                    buttons=buttons, parse_mode="html", link_preview=False
                )
                log_info("🔁 打开转发方式菜单", extra={"user_id": user_id})
            elif arg == "album":
                await _run_build_flow(event, MessageType.ALBUM)


        # 转发方式选择
        elif act == "set_fwd_mode":
            arg = args[0] if args else None
            fsm = get_fsm()
            if not fsm:
                return
            meta = await fsm.get_metadata(user_id) or {}
            if arg == "hide":
                meta["forward_as_copy"] = True
                await fsm.set_metadata(user_id, meta)
                log_info("🔧 设置转发方式=隐藏来源", extra={"user_id": user_id})
                await _run_build_flow(event, MessageType.FORWARD)
            elif arg == "show":
                meta["forward_as_copy"] = False
                await fsm.set_metadata(user_id, meta)
                log_info("🔧 设置转发方式=显示来源", extra={"user_id": user_id})
                await _run_build_flow(event, MessageType.FORWARD)

    except Exception as e:
        log_exception("task callback 处理异常", exc=e, extra={"user_id": user_id})
        await BotUtils.safe_respond(
            event,event.client, "❌ 处理消息配置回调失败，请重试。",parse_mode="html",  link_preview=False
        )


def register_commands():
    from router.command_router import main_router
    main_router.command("/b", trace_name="设置消息内容")(handle_message)
    log_debug("✅ message_command 已注册 /b")
    return True


def register_callbacks(bus_or_client):
    """
    回调注册：统一挂载 handle_task_callbacks
    """
    if hasattr(bus_or_client, "register"):
        bus_or_client.register("task:set_msg_type", handle_task_callbacks)
        bus_or_client.register("task:set_fwd_mode", handle_task_callbacks)
        log_debug("✅ message_command 回调已注册（bus）")
        return True

    if hasattr(bus_or_client, "add_event_handler"):
        c = bus_or_client
        safe_add_event_handler(
            c, handle_task_callbacks, events.CallbackQuery(), tag="cb:task"
        )
        log_debug("✅ message_command 回调已注册（telethon）")
        return True

    log_warning("⚠️ message_command.register_callbacks 未识别的对象，已跳过")
    return False
