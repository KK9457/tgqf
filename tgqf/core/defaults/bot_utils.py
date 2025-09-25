# core/defaults/bot_utils.py
# -*- coding: utf-8 -*-
"""
安全回信与通知工具
- 统一 parse_mode 可显式传入；若提供 entities/formatting_entities，自动忽略 parse_mode
- 优先 event.respond；失败时回退 client.send_message(chat_id)；再回退通知 Bot；最后 Saved Messages
- 支持 MessageContent（结构化消息），保留 entities/file/buttons/link_preview 透传
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional, Sequence

from telethon import TelegramClient
from telethon.tl.custom.message import Message
from telethon.tl.types import TypeMessageEntity

# 统一走 bot_client 的公开接口
from core.bot_client import create_bot_client as _create_bot_client
from core.bot_client import get_notify_bot as _get_bot
from unified.logger import log_info, log_warning, log_exception, log_debug
from unified.trace_context import get_log_context, get_user_id

# 懒创建锁，防并发重复启动
_bot_lock = asyncio.Lock()


async def _ensure_notify_bot() -> TelegramClient:
    """获取/懒创建 通知Bot 客户端（单例）"""
    try:
        return _get_bot()
    except Exception:
        pass
    async with _bot_lock:
        # 双检，避免竞态
        try:
            return _get_bot()
        except Exception:
            pass
        bot = await _create_bot_client()
        return bot


def _kwargs_for_send(
    text: Optional[str],
    *,
    parse_mode: Optional[str] = None,
    reply_to: Optional[int] = None,
    link_preview: Optional[bool] = None,
    buttons: Any = None,
    entities: Optional[Sequence[TypeMessageEntity]] = None,
    formatting_entities: Optional[Sequence[TypeMessageEntity]] = None,
    file: Any = None,
    **kwargs: Any,
) -> dict:
    """
    构造 Telethon 发送参数：
    - parse_mode 支持显式传入；若存在实体（entities/formatting_entities），自动移除 parse_mode
    - 透传 buttons/file/link_preview 及额外 **kwargs（例如 schedule 等）
    """
    out = dict(
        message=text or "-",
        parse_mode=(parse_mode if parse_mode is not None else None),
        link_preview=link_preview,
        buttons=buttons,
        # 统一优先 formatting_entities；否则接受 entities
        formatting_entities=(formatting_entities or entities),
        reply_to=reply_to,
        file=file,
    )
    # 透传其余不冲突参数
    for k, v in (kwargs or {}).items():
        if k not in out:
            out[k] = v
    return _normalize_send_kwargs(out)


def _normalize_send_kwargs(d: dict) -> dict:
    """兼容 Telethon 的签名：若有 formatting_entities 则移除 parse_mode，避免冲突"""
    if not isinstance(d, dict):
        return {}
    out = dict(d)
    # 避免同时 parse_mode 与 formatting_entities（优先实体）
    if out.get("formatting_entities") is not None:
        out.pop("parse_mode", None)
    return out


class BotUtils:
    @staticmethod
    async def notify_user(
        user_id: int,
        text: str,
        *,
        parse_mode: Optional[str] = None,
        reply_to: Optional[int] = None,
        link_preview: Optional[bool] = None,
        buttons: Any = None,
        entities: Optional[Sequence[TypeMessageEntity]] = None,
        formatting_entities: Optional[Sequence[TypeMessageEntity]] = None,
        file: Any = None,
        **kwargs: Any,
    ) -> Optional[Message]:
        """通过通知 Bot 向指定用户私聊发送消息"""
        try:
            bot = await _ensure_notify_bot()
            kwargs_send = _kwargs_for_send(
                text,
                parse_mode=parse_mode,
                reply_to=reply_to,
                link_preview=link_preview,
                buttons=buttons,
                entities=entities,
                formatting_entities=formatting_entities,
                file=file,
                **kwargs,
            )
            return await bot.send_message(user_id, **kwargs_send)
        except Exception as e:
            log_exception("notify_user 发送失败", e)
            return None

    @staticmethod
    async def safe_respond(
        event,
        client,
        text: Optional[str] = None,
        *,
        parse_mode: Optional[str] = None,
        reply_to: Optional[int] = None,
        link_preview: Optional[bool] = None,
        buttons: Any = None,
        entities: Optional[Sequence[TypeMessageEntity]] = None,
        formatting_entities: Optional[Sequence[TypeMessageEntity]] = None,
        file: Any = None,
        fallback_to_notify: bool = True,
        target_user_id: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Message]:
        """
        安全回信：
        1) 尝试 event.respond
        2) 失败则使用 client.send_message(chat_id)
        3) 若无事件/无法定位 chat_id 且允许回退，则用通知 Bot 按 user_id 私聊
        4) 最后兜底：发送到自己的 Saved Messages
        - parse_mode/formatting_entities/实体参数遵循 Telethon 规范（若提供实体，将忽略 parse_mode）
        """
        text = text or "-"
        kwargs_send = _kwargs_for_send(
            text,
            parse_mode=parse_mode,
            reply_to=reply_to,
            link_preview=link_preview,
            buttons=buttons,
            entities=entities,
            formatting_entities=formatting_entities,
            file=file,
            **kwargs,
        )

        # 1) event.respond 优先
        if event and getattr(event, "respond", None):
            try:
                return await event.respond(**kwargs_send)  # type: ignore
            except Exception as e:
                log_warning(
                    "event.respond 失败，转入回退逻辑",
                    extra={"err": str(e), **(get_log_context() or {})},
                )

        # 2) 使用 client + chat_id
        chat_id = getattr(event, "chat_id", None)
        if client and chat_id is not None:
            try:
                return await client.send_message(chat_id, **kwargs_send)
            except Exception as e:
                log_warning(
                    "client.send_message(chat_id) 失败，准备进一步回退",
                    extra={"err": str(e), **(get_log_context() or {})},
                )

        # 3) 回退到通知 Bot（若允许）
        if fallback_to_notify:
            uid = target_user_id if target_user_id is not None else get_user_id()
            if uid:
                try:
                    bot = await _ensure_notify_bot()
                    return await bot.send_message(uid, **kwargs_send)
                except Exception as e:
                    log_warning(
                        "通知 Bot 回退失败，将尝试发送到 Saved Messages",
                        extra={"err": str(e), **(get_log_context() or {})},
                    )

        # 4) 最后兜底：发送到自己的 Saved Messages
        try:
            if client:
                return await client.send_message("me", **kwargs_send)
        except Exception as e:
            log_exception("safe_respond 最终兜底失败", e)
        return None
