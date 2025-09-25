# tg/conversation_flow.py
# -*- coding: utf-8 -*-
"""
封装 Conversation 对象的对话控制逻辑，包括常用的问答流程、等待响应等。
"""

from __future__ import annotations

import asyncio
from typing import Callable, List, Optional, Set
from dataclasses import dataclass, field
from weakref import WeakValueDictionary

from telethon.errors.common import AlreadyInConversationError
from telethon.tl.custom.conversation import Conversation
from telethon.tl.custom.message import Message
from telethon import Button, events

from core.registry_guard import safe_add_event_handler
from unified.config import ATTEMPTS
from unified.logger import log_exception, log_warning, log_info

__all__ = [
    "ConversationStrategy",
    "ConversationFlow",
    "register_cancel_callback",
    "handle_cancel_conv",
]

_DEFAULT_STEP_TIMEOUT = 300  # 默认单步超时（秒）
_CANCEL_TEXT = "❌ 取消"

# (client_id, peer_id) -> Conversation
_ACTIVE_CONVS: "WeakValueDictionary[tuple[int, int], Conversation]" = WeakValueDictionary()


@dataclass
class ConversationStrategy:
    """对话策略（可注入/便于 A/B）"""
    step_timeout: int = _DEFAULT_STEP_TIMEOUT
    max_attempts: int = 3
    cancel_commands: Set[str] = field(default_factory=lambda: {
        "/cancel", "cancel", "stop", "取消", "❌ 取消"
    })
    cancel_button_text: str = _CANCEL_TEXT

    @classmethod
    def from_overrides(cls, **overrides):
        base = cls()
        for k, v in (overrides or {}).items():
            if hasattr(base, k) and v is not None:
                setattr(base, k, v if k != "cancel_commands" else set(v))
        return base


DEFAULT_STRATEGY = ConversationStrategy()


class ConversationFlow:
    """
    对话流封装类，支持自动问答、验证、超时、命令拦截、取消。
    """

    def __init__(self, conv: Conversation, default_parse_mode: Optional[str] = "html", *, strategy: ConversationStrategy = DEFAULT_STRATEGY):
        self.conv = conv
        self.parse_mode = default_parse_mode
        self.strategy = strategy or DEFAULT_STRATEGY

    @classmethod
    async def start(cls, event, timeout: int = ATTEMPTS * 60, *, strategy: ConversationStrategy = DEFAULT_STRATEGY) -> "ConversationFlow":
        """从 event 快速启动对话流（内部处理已有对话冲突）。"""
        try:
            peer = getattr(event, "chat_id", None) or getattr(event, "sender_id", None)
            log_info("🧭【对话状态】进入会话状态", extra={"peer": peer, "timeout": timeout})

            # 建立会话：优先独占；失败则退回非独占以复用已有会话
            try:
                conv = event.client.conversation(peer, timeout=timeout, exclusive=True)
            except AlreadyInConversationError:
                conv = event.client.conversation(peer, timeout=timeout, exclusive=False)

            inst = cls(conv, strategy=strategy)
            _ACTIVE_CONVS[(id(event.client), int(peer))] = conv  # 登记
            return inst
        except Exception as e:
            log_exception("❌【对话状态】进入会话状态失败", exc=e)
            raise

    async def _get_valid_response(
        self,
        validate: Optional[Callable[[Message], bool]] = None,
        step_timeout: Optional[int] = None,
    ) -> Optional[Message]:
        """通用输入校验 + 文本命令拦截 + 取消按钮支持"""
        try:
            msg = await self.conv.get_response(timeout=step_timeout or self.strategy.step_timeout)
        except (asyncio.TimeoutError, TimeoutError):
            log_warning("⏰【对话状态】输入超时", extra={"timeout": step_timeout or self.strategy.step_timeout})
            return None

        # 文本命令拦截（/cancel、cancel、stop、取消、❌ 取消 等）
        if msg and getattr(msg, "text", None):
            t = msg.text.strip()
            if t.lower() in self.strategy.cancel_commands:
                await self.cancel("🚫【对话状态】取消会话状态")
                return None

        ok = (validate is None) or (msg and getattr(msg, "text", None) and validate(msg))
        log_info("📩【对话状态】接收消息", extra={"validated": bool(ok), "text": getattr(msg, "text", None)})
        return msg if ok else None

    async def ask(
        self,
        prompt: str,
        validate: Optional[Callable[[Message], bool]] = None,
        retry_prompt: Optional[str] = None,
        max_attempts: Optional[int] = None,
        *,
        step_timeout: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> Optional[Message]:
        """核心问答逻辑，带重试、超时和取消按钮"""
        try:
            if prompt and prompt.strip():
                await self.conv.send_message(
                    prompt,
                    parse_mode=parse_mode or self.parse_mode,
                    buttons=[[Button.inline(self.strategy.cancel_button_text, data=b"cancel_conv")]],
                )
                log_info("📨【对话状态】发送问题", extra={"prompt_len": len(prompt)})

            attempts = int(max_attempts or self.strategy.max_attempts)
            for _ in range(attempts):
                msg = await self._get_valid_response(validate, step_timeout)
                if msg:
                    return msg
                if retry_prompt:
                    await self.conv.send_message(
                        retry_prompt,
                        parse_mode=parse_mode or self.parse_mode,
                        buttons=[[Button.inline(self.strategy.cancel_button_text, data=b"cancel_conv")]],
                    )
        except Exception as e:
            log_exception("【对话状态】发送问题异常错误", exc=e)
        return None

    async def ask_text(
        self,
        prompt: str,
        *,
        retry: str = "❌ 无效输入，请重试",
        strip: bool = True,
        step_timeout: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> str:
        msg = await self.ask(
            prompt,
            validate=lambda m: bool(getattr(m, "text", "") and m.text.strip()),
            retry_prompt=retry,
            max_attempts=None,
            step_timeout=step_timeout,
            parse_mode=parse_mode,
        )
        out = (msg.text.strip() if strip else msg.text) if msg else ""
        log_info("【对话状态】问题回答完成", extra={"empty": not bool(out), "len": len(out or "")})
        return out

    async def ask_number(
        self,
        prompt: str,
        *,
        retry: str = "❗ 请输入有效数字",
        step_timeout: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> Optional[int]:
        msg = await self.ask(
            prompt,
            validate=lambda m: bool(getattr(m, "text", None)) and m.text.isdigit(),
            retry_prompt=retry,
            max_attempts=None,
            step_timeout=step_timeout,
            parse_mode=parse_mode,
        )
        num = int(msg.text) if msg else None
        log_info("【对话状态】回答时量(秒)完成", extra={"ok": num is not None, "value": (num if num is not None else "NA")})
        return num

    async def ask_choice(
        self,
        prompt: str,
        choices: List[str],
        *,
        retry: str = "❗ 请输入有效选项",
        step_timeout: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> Optional[str]:
        lower_choices = [c.lower() for c in (choices or [])]
        msg = await self.ask(
            prompt,
            validate=lambda m: bool(getattr(m, "text", None)) and m.text.lower() in lower_choices,
            retry_prompt=retry,
            max_attempts=None,
            step_timeout=step_timeout,
            parse_mode=parse_mode,
        )
        choice = msg.text if msg else None
        log_info("【对话状态】ask_choice 完成", extra={"ok": bool(choice), "choice": (choice or "NA")})
        return choice

    async def confirm(
        self,
        prompt: str = "确认继续？(yes/no)",
        *,
        retry: str = "请输入 yes 或 no",
        step_timeout: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> bool:
        choice = await self.ask_choice(
            prompt,
            choices=["yes", "no"],
            retry=retry,
            step_timeout=step_timeout,
            parse_mode=parse_mode,
        )
        ok = choice.lower() == "yes" if choice else False
        log_info("confirm() 结果", extra={"ok": ok})
        return ok

    async def cancel(self, text: str = "🚫 会话已取消"):
        try:
            if self.conv:
                await self.conv.send_message(text, parse_mode=self.parse_mode)
                self.conv.cancel()
        finally:
            try:
                key = (
                    id(getattr(self.conv, "_client", None) or getattr(self.conv, "client", None)),
                    int(getattr(self.conv, "chat_id", 0))
                )
                _ACTIVE_CONVS.pop(key, None)
            except Exception:
                pass


# -------------------------
# 全局取消按钮回调
# -------------------------
async def handle_cancel_conv(event):
    try:
        await event.answer("🚫 已取消")
    except Exception:
        pass
    try:
        await event.respond("🚫 当前操作已取消", link_preview=False)
        try:
            await event.delete()
        except Exception:
            pass
    except Exception as e:
        log_exception("【对话状态】取消会话 响应回复失败", exc=e)

    # 真 · 取消会话
    key = (id(event.client), int(getattr(event, "chat_id", None) or getattr(event, "sender_id", 0)))
    conv = _ACTIVE_CONVS.get(key)
    if conv:
        try:
            conv.cancel()
        except Exception:
            pass

    log_info("【对话状态】用户点击取消按钮", extra={"user_id": getattr(event, 'sender_id', None)})


def register_cancel_callback(client_or_bus):
    """在全局注册 cancel_conv 回调（必须在 bot 初始化时调用一次）"""
    if hasattr(client_or_bus, "register"):
        client_or_bus.register("cancel_conv", handle_cancel_conv)
        log_info("✅【对话状态】取消按钮回调注册主函数入口")
        return True

    if hasattr(client_or_bus, "add_event_handler"):
        safe_add_event_handler(
            client_or_bus,
            handle_cancel_conv,
            events.CallbackQuery(data=b"cancel_conv"),
            tag="cb:cancel_conv",
        )
        log_info("✅【对话状态】取消按钮回调已注册")
        return True

    log_warning("⚠️【对话状态】取消按钮回调注册失败")
    return False
