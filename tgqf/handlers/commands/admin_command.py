# handlers/commands/admin_command.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import unicodedata
from typing import Dict, Optional, Tuple

from telethon import Button, TelegramClient, events
from telethon.errors.common import AlreadyInConversationError
from telethon.events import CallbackQuery
from telethon.tl.custom.message import Message
from telethon.tl.types import PeerUser

from core.decorators import super_command
from core.defaults.bot_utils import BotUtils
from core.registry_guard import safe_add_event_handler
from typess.fsm_keys import USER_ID, USERNAME, USERNAME_LOG
from unified.config import RUNNER_TIMEOUT
from unified.context import get_bot, get_fsm
from unified.logger import log_exception, log_info, log_warning

# =========================
# 模块级状态 & 幂等
# =========================
_ADMIN_CB_REGISTERED = False
ACTIVE_CONVS: Dict[int, events.conversation.Conversation] = {}  # chat_id -> conv


# =========================
# 小工具：UI/答复/拟态
# =========================
def _markup(client: TelegramClient, buttons):
    """返回二维按钮数组；由 send_message(..., buttons=...) 渲染。"""
    return buttons

async def _safe_answer(
    event: events.CallbackQuery.Event,
    text: Optional[str] = None,
    *,
    alert: bool = False,
):
    try:
        await event.answer(text or "", alert=alert)
    except Exception:
        pass


class _with_typing:
    """async with client.action(chat, 'typing') 的轻薄封装。"""

    def __init__(self, event, action: str = "typing"):
        self._event = event
        self._action = action
        self._ctx = None

    async def __aenter__(self):
        try:
            self._ctx = self._event.client.action(self._event.chat_id, self._action)
            return await self._ctx.__aenter__()  # type: ignore
        except Exception:
            return None

    async def __aexit__(self, et, ev, tb):
        try:
            if self._ctx:
                return await self._ctx.__aexit__(et, ev, tb)  # type: ignore
        except Exception:
            return False
        return False


# =========================
# 规范化 & 解析
# =========================
def _normalize_target(s: str) -> str:
    s = (s or "").strip()
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf")
    s = unicodedata.normalize("NFKC", s)
    return s


async def get_uid_username(client: TelegramClient, target: str) -> Tuple[int, str]:
    target = _normalize_target(target)
    if not target:
        raise ValueError("❌ 输入为空")

    if target.startswith("@"):
        from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError

        try:
            ent = await client.get_entity(target)
        except UsernameNotOccupiedError:
            raise ValueError(f"❌ 用户名未被注册：{target}")
        except UsernameInvalidError:
            raise ValueError(f"❌ 用户名无效：{target}")
        except Exception as e:
            raise ValueError(f"❌ 无法解析用户名 {target} → {e}")

        uid = ent.id
        username = f"@{getattr(ent, 'username', '')}" if getattr(ent, "username", None) else ""
        return uid, username

    try:
        uid = int(target)
    except ValueError:
        raise ValueError(f"❌ 无效 ID：{target} 不是数字格式")

    try:
        ent = await client.get_entity(PeerUser(uid))
        username = f"@{ent.username}" if getattr(ent, "username", None) else ""
        return uid, username
    except Exception:
        return uid, ""


# =========================
# 主流程：白名单新增/移除
# =========================
async def process_white_list_operation(event: Message, action: str, prompt_message: str):
    """
    action: "add" / "remove"
    """
    client = event.client
    chat_id = event.chat_id
    sender_id = event.sender_id

    # 若已有活动会话，先尝试取消
    old = ACTIVE_CONVS.pop(chat_id, None)
    if old:
        try:
            old.cancel()
        except Exception:
            pass

    try:
        # 独占会话避免并发混乱
        conv = client.conversation(chat_id, timeout=RUNNER_TIMEOUT, exclusive=True)
        ACTIVE_CONVS[chat_id] = await conv.__aenter__()  # type: ignore
    except AlreadyInConversationError:
        # 走通知 Bot，兼容新的 BotUtils 接口
        await BotUtils.notify_user(chat_id, "⚠️ 当前有进行中的操作，请先完成或稍后再试。")
        return
    except Exception as e:
        log_exception("❌ 创建会话失败", exc=e)
        await BotUtils.notify_user(chat_id, f"❌ 无法启动会话：{e}")
        return

    try:
        cancel_btns = [[Button.inline("❌ 取消操作", b"admin:cancel")]]
        async with _with_typing(event):
            await ACTIVE_CONVS[chat_id].send_message(prompt_message, buttons=_markup(client, cancel_btns))
            log_info("📩 等待管理员输入白名单...")

        try:
            # 只接受来自当前操作者的文本消息
            resp = await ACTIVE_CONVS[chat_id].wait_event(
                events.NewMessage(from_users=sender_id), timeout=RUNNER_TIMEOUT
            )
        except Exception:
            log_warning("⚠️ 超时未响应，操作流程结束")
            async with _with_typing(event):
                await ACTIVE_CONVS[chat_id].send_message("⚠️ 超时未响应，操作流程结束")
            return

        raw = (resp.text or "").strip()
        if not raw:
            async with _with_typing(event):
                await ACTIVE_CONVS[chat_id].send_message("❗ 输入为空，操作结束")
            return

        # 支持换行/空格/逗号分隔的批量输入
        targets = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = re.split(r"[,\s]+", line)
            targets.extend(p for p in parts if p)

        fsm = get_fsm()
        if not fsm:
            async with _with_typing(event):
                await ACTIVE_CONVS[chat_id].send_message("⚠️ FSM 尚未初始化，请联系管理员")
            return

        results = []
        for target in targets:
            try:
                uid, username = await get_uid_username(client, target)
                log_info("🎯 处理目标", extra={USER_ID: uid, USERNAME_LOG: username})

                stored_name = await fsm.get_data(uid, USERNAME)
                username = username or stored_name or "(未知用户名)"

                if action == "add":
                    ok = await fsm.add_to_whitelist(0, uid, username=username)
                    if ok:
                        if username != "(未知用户名)":
                            await fsm.set_data(uid, USERNAME, username)
                        results.append(f"✅ <b>新增成功</b>：<code>{uid}</code> {username}")
                    else:
                        results.append(f"ℹ️ <b>已存在</b>：<code>{uid}</code> {username}")

                elif action == "remove":
                    ok = await fsm.remove_from_whitelist(0, uid)
                    if ok:
                        results.append(f"🗑️ <b>已移除</b>：<code>{uid}</code> {username}")
                    else:
                        results.append(f"⚠️ <b>不在白名单</b>：<code>{uid}</code> {username}")

                else:
                    results.append(f"❓ 未知操作：{action}")

            except Exception as e:
                log_exception(f"❌ 处理失败：{target} → {str(e)}")
                results.append(f"❌ <b>失败</b>：{target} → <code>{str(e)}</code>")

        title = "⭐️ 新增白名单：" if action == "add" else "⭐️ 移除白名单："
        async with _with_typing(event):
            await ACTIVE_CONVS[chat_id].send_message(title + "\n" + "\n".join(results), parse_mode="html")

    except Exception as e:
        log_exception("❌ 白名单流程异常", exc=e)
        await BotUtils.notify_user(chat_id, "⚠️ 当前有进行中的操作，请先完成或稍后再试。")
    finally:
        try:
            await ACTIVE_CONVS[chat_id].__aexit__(None, None, None)  # type: ignore
        except Exception:
            pass
        ACTIVE_CONVS.pop(chat_id, None)


# =========================
# 对外命令
# =========================
def register_commands(bot: Optional[TelegramClient] = None):
    from router.command_router import main_router

    main_router.command("/ad", trace_name="添加白名单", admin_only=True)(add_white)
    main_router.command("/re", trace_name="移除白名单", admin_only=True)(remove_white)
    main_router.command("/see", trace_name="查看白名单", admin_only=True)(list_white_command)

    # 幂等注册取消按钮
    global _ADMIN_CB_REGISTERED
    if not _ADMIN_CB_REGISTERED:
        try:
            real_bot = bot or get_bot()
            if real_bot is not None:
                safe_add_event_handler(
                    real_bot,
                    handle_cancel_add_remove,
                    events.CallbackQuery(data=b"admin:cancel"),
                    tag="admin:cancel",
                )
                _ADMIN_CB_REGISTERED = True
        except Exception as e:
            log_exception("❌ admin_command 回调注册失败", exc=e)

    return True


@super_command(trace_action="添加白名单", admin_only=True)
async def add_white(event: Message):
    await process_white_list_operation(event, "add", "⭐️ 输入要新增的 ID / ＠用户名（可换行/空格/逗号分隔）")


@super_command(trace_action="移除白名单", admin_only=True)
async def remove_white(event: Message):
    await process_white_list_operation(event, "remove", "⭐️ 输入要移除的 ID / ＠用户名（可换行/空格/逗号分隔）")


@super_command(trace_action="查看白名单", admin_only=True)
async def list_white_command(event: Message):
    # 复用现有白名单展示
    from unified.white_panel import list_white
    await list_white(event, page=1, edit=False)


# =========================
# 按钮回调：取消当前会话
# =========================
async def handle_cancel_add_remove(event: CallbackQuery.Event):
    await _safe_answer(event, "❌ 已取消操作", alert=True)
    chat_id = event.chat_id
    conv = ACTIVE_CONVS.pop(chat_id, None)
    if conv:
        try:
            conv.cancel()
        except Exception:
            pass
    try:
        await event.edit("❌ 操作已取消")
    except Exception:
        pass