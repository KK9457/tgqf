# unified/message_builders.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from telethon import TelegramClient, events, types, utils
from telethon.errors.rpcerrorlist import BotMethodInvalidError
from telethon.tl.types import MessageEntityCustomEmoji

from typess.message_enums import (
    EntitiesSeq,
    MessageContent,
    MessageType,
    ParseMode,
    ensure_entities_objs,
)
from unified.config import MEDIA_DIR
from unified.emoji_store import EmojiStore
from unified.logger import log_debug, log_error, log_exception, log_info, log_warning


from unified.trace_context import inject_trace_context, get_log_context
from tg.telethon_aligned import clip_text_entities,CAPTION_LIMIT,sanitize_parse_mode,AutoParseMode,split_text_for_send
from unified.text_utils import PLACEHOLDER_PATTERN
from unified.smart_text import (
    smart_text_parse,
    extract_tgemoji_ids_from_html,
    extract_custom_emoji_ids,
)
emoji_store = EmojiStore()



def _trace(func_name: str, *, conv=None, client=None, **extras):
    ctx = {"phase": "message_build", **(extras or {})}
    try:
        if conv is not None:
            chat_id = getattr(conv, "chat_id", None)
            if chat_id is not None:
                ctx.setdefault("chat_id", chat_id)
                ctx.setdefault("user_id", chat_id)
    except Exception:
        pass
    try:
        if client is not None:
            sess = getattr(client, "session", None)
            sess_name = getattr(sess, "filename", None) or getattr(sess, "name", None)
            if sess_name:
                ctx.setdefault("session_name", str(sess_name))
    except Exception:
        pass
    inject_trace_context(func_name, **ctx)



def looks_like_template(text: str) -> bool:
    return bool(PLACEHOLDER_PATTERN.search(text or ""))

LINK_RE = re.compile(r"(?:https?://)?t\.me/(?P<chat>[\w\d_]+)/(?P<id>\d+)")



def guess_media_type(mm: Any) -> str:
    media = getattr(mm, "media", mm)
    document = None
    if isinstance(media, types.MessageMediaPhoto) or getattr(mm, "photo", None):
        return "photo"
    if isinstance(media, types.MessageMediaDocument):
        document = media.document
    elif isinstance(mm, types.Document):
        document = mm
    elif getattr(mm, "document", None):
        document = mm.document
    if document:
        try:
            if utils.is_sticker(document): return "sticker"
        except Exception: pass
        try:
            if utils.is_gif(document): return "gif"
        except Exception: pass
        try:
            if utils.is_voice(document): return "voice"
        except Exception: pass
        try:
            if utils.is_video(document): return "video"
        except Exception: pass
        try:
            if utils.is_audio(document): return "audio"
        except Exception: pass
        mime = (getattr(document, "mime_type", "") or "").lower()
        if mime.startswith("image/"): return "photo"
        if mime.startswith("video/"): return "video"
        if mime.startswith("audio/"): return "audio"
        if mime == "application/x-tgsticker": return "sticker"
        return "file"
    if getattr(mm, "video", None): return "video"
    if getattr(mm, "voice", None): return "voice"
    if getattr(mm, "audio", None): return "audio"
    if getattr(mm, "gif", None): return "gif"
    if getattr(mm, "sticker", None): return "sticker"
    return "unknown"

async def _safe_preview_media(
    conv,
    file_path: str,
    media_type: str,
    caption: str = "",
    *,
    parse_mode: Optional[ParseMode] = None,
    entities: Optional[EntitiesSeq] = None,
) -> bool:
    _trace("_safe_preview_media", conv=conv, media_type=media_type)
    try:
        if not file_path or not os.path.exists(file_path):
            log_error("⚠️ 文件不存在", extra={"file_path": file_path})
            await conv.send_message("⚠️ 文件不存在，无法预览")
            return False
        size = None
        try:
            size = os.path.getsize(file_path)
        except Exception:
            pass
        ents = ensure_entities_objs(list(entities or []), __text_len=len(caption or ""))
        log_debug("🖼️ 媒体预览参数", extra={"media_type": media_type, "file_path": file_path, "bytes": size, "use_entities": bool(ents), "entities_count": len(ents or []), "parse_mode": (None if ents else parse_mode)})
        kwargs: Dict[str, Any] = {"caption": caption, "parse_mode": (None if ents else parse_mode), "formatting_entities": ents if ents else None}
        if media_type == "photo":
            await conv.send_file(file_path, **kwargs)
        elif media_type == "video":
            await conv.send_file(file_path, supports_streaming=True, **kwargs)
        elif media_type == "file":
            await conv.send_file(file_path, force_document=True, **kwargs)
        else:
            await conv.send_file(file_path, **kwargs)
        return True
    except Exception as e:
        log_error(f"❌ 媒体预览失败: {e}")
        try:
            await conv.send_message("❌ 预览失败，请稍后重试")
        except Exception:
            pass
        return False
PRIVATE_LINK_RE = re.compile(r"(?:https?://)?t\.me/c/(?P<int_id>\d+)/(?P<id>\d+)")
def _extract_peer_and_msg_id_from_text(text: str) -> Optional[Tuple[str, int]]:
    text = (text or "").strip()
    # 形如: @channel/123
    if text.startswith("@") and "/" in text:
        try:
            chat, msg_id = text[1:].split("/", 1)
            return chat, int(msg_id)
        except Exception:
            return None

    # 公共频道: t.me/username/123
    m = LINK_RE.search(text)
    if m:
        return m.group("chat"), int(m.group("id"))

    # 私有频道: t.me/c/123456/789  → 返回 id:-100123456
    m2 = PRIVATE_LINK_RE.search(text)
    if m2:
        internal = int(m2.group("int_id"))
        # Telegram 内部 channel/chat id: -100 + internal_id
        return f"id:-100{internal}", int(m2.group("id"))

    return None

async def _collect_album_messages(client: TelegramClient, conv, first_msg) -> List[Any]:
    _trace("_collect_album_messages", conv=conv)
    gid = getattr(first_msg, "grouped_id", None)
    if not gid:
        log_debug("相册收集：单条消息（无 grouped_id）")
        return [first_msg]
    try:
        me = await client.get_me()
        if not bool(getattr(me, "bot", False)):
            msgs = await client.get_messages(conv.chat_id, limit=100)
            same_group = [m for m in msgs if getattr(m, "grouped_id", None) == gid]
            if first_msg not in same_group:
                same_group.append(first_msg)
            same_group.sort(key=lambda x: getattr(x, "id", 0))
            log_debug("相册收集：历史接口", extra={"count": len(same_group)})
            return same_group
    except BotMethodInvalidError:
        log_debug("相册收集：BotMethodInvalid，回退事件流")
    except Exception as e:
        log_warning("相册收集：历史接口异常，回退事件流", extra={"error": str(e)})

    collected = [first_msg]
    deadline = 2.0
    end_ts = __import__("time").monotonic() + deadline
    sender_id = getattr(first_msg, "sender_id", None)

    while True:
        timeout = end_ts - __import__("time").monotonic()
        if timeout <= 0:
            break
        try:
            ev = await conv.wait_event(events.NewMessage(chats=conv.chat_id, from_users=sender_id), timeout=timeout)
            msg = ev.message
            if getattr(msg, "grouped_id", None) == gid:
                collected.append(msg)
                continue
        except __import__("asyncio").TimeoutError:
            break
        except Exception:
            break

    collected.sort(key=lambda x: getattr(x, "id", 0))
    log_debug("相册收集：事件流", extra={"count": len(collected)})
    return collected

async def _safe_preview_album(conv, files: List[str], caption: str, entities: Optional[EntitiesSeq], parse_mode: Optional[ParseMode]) -> bool:
    _trace("_safe_preview_album", conv=conv, files=len(files or []))
    try:
        if not files:
            await conv.send_message("⚠️ 相册为空，无法预览")
            return False
        total_bytes = 0
        try:
            total_bytes = sum(os.path.getsize(p) for p in files if os.path.exists(p))
        except Exception:
            pass
        ents = ensure_entities_objs(list(entities or []), __text_len=len(caption or ""))
        log_debug("🎞️ 相册预览参数", extra={"files": len(files), "bytes": total_bytes, "use_entities": bool(ents), "entities_count": len(ents or []), "parse_mode": (None if ents else parse_mode)})
        await conv.send_file(files, caption=caption, parse_mode=(None if ents else parse_mode), formatting_entities=ents if ents else None, album=True, supports_streaming=True)
        return True
    except Exception as e:
        log_error(f"❌ 相册预览失败: {e}")
        try:
            await conv.send_message("❌ 相册预览失败，请稍后重试")
        except Exception:
            pass
        return False

class MessageBuilder:
    @staticmethod
    async def interactive_build(conv, mtype: MessageType, client: Optional[TelegramClient] = None) -> Optional[MessageContent]:
        client = client or getattr(conv, "client", None) or getattr(conv, "_client", None)
        _trace("interactive_build", conv=conv, client=client, mtype=(getattr(mtype, "name", str(mtype)) if mtype else None))
        log_info("🧭 进入消息构建流程", extra={"message_type": getattr(mtype, "name", str(mtype)), **get_log_context()})

        try:
            if mtype == MessageType.TEXT:
                log_info("构建文本消息", extra={"message_type": "TEXT", **get_log_context()})
                return await MessageBuilder._build_text(conv, client)
            if mtype == MessageType.MEDIA:
                log_info("构建媒体消息", extra={"message_type": "MEDIA", **get_log_context()})
                return await MessageBuilder._build_media(conv, client)
            if mtype == MessageType.FORWARD:
                log_info("构建转发消息", extra={"message_type": "FORWARD", **get_log_context()})
                return await MessageBuilder._build_forward(conv)
            if mtype == MessageType.ALBUM:
                log_info("构建相册消息", extra={"message_type": "ALBUM", **get_log_context()})
                return await MessageBuilder._build_album(conv, client)

            await conv.send_message("❌ 暂不支持该类型")
            log_warning("不支持的消息类型", extra={"mtype": getattr(mtype, "name", str(mtype)), **get_log_context()})
            return None
        except Exception as e:
            log_exception("❌ 消息构建失败", exc=e, extra={"message_type": getattr(mtype, "name", str(mtype)), **get_log_context()})
            try:
                await conv.send_message("❌ 处理消息时出错，请稍后重试。")
            except Exception:
                pass
            return None

    @staticmethod
    async def _build_album(conv, client: TelegramClient) -> Optional[MessageContent]:
        _trace("MessageBuilder._build_album", conv=conv, client=client)
        log_info("🧭 开始构建相册消息", extra={**get_log_context()})

        # 确保保存目录存在
        os.makedirs(MEDIA_DIR, exist_ok=True)

        await conv.send_message("📚 发送相册消息（在下一条消息里一次性选中多张图片/视频；可附带文字作为标题）：")
        mm = await conv.get_response()

        caption = getattr(mm, "text", "") or ""
        parse_mode: Optional[ParseMode] = None
        entities: Optional[EntitiesSeq] = None

        if caption.strip():
            caption, parse_mode, entities, warns = smart_text_parse(
                caption, template_name_map=(emoji_store.name_to_id if emoji_store.loaded else None)
            )
            log_info(
                "🧩 相册标题解析完毕",
                extra={"caption_len": len(caption), "parse_mode": parse_mode or "auto+", "entities_count": len(entities or []),
                    "warnings": len(warns or []), **get_log_context()}
            )

        msgs = await _collect_album_messages(client, conv, mm)
        log_info("🧺 相册消息收集完成", extra={"count": len(msgs or []), **get_log_context()})

        file_paths: List[str] = []
        for m in msgs:
            media = getattr(m, "media", None)
            if not media:
                continue
            p = await client.download_media(media, file=MEDIA_DIR)  # 统一保存目录
            if p and os.path.exists(p):
                file_paths.append(p)

        if not file_paths:
            await conv.send_message("⚠️ 未识别到相册媒体，请重新尝试 `/b`")
            log_warning("相册媒体为空", extra={**get_log_context()})
            return None

        total_bytes = sum(os.path.getsize(p) for p in file_paths if os.path.exists(p))
        log_info("⬇️ 相册媒体下载完成", extra={"files": len(file_paths), "bytes": total_bytes, "save_dir": MEDIA_DIR, **get_log_context()})

        caption2, ents2 = clip_text_entities(caption or "", list(entities or []), limit=CAPTION_LIMIT)
        ok = await _safe_preview_album(conv, file_paths, caption2, ents2, parse_mode)
        if not ok:
            log_warning("相册预览失败", extra={**get_log_context()})
            return None

        mc = MessageContent.build(MessageType.ALBUM, file_paths=file_paths, caption=caption2, parse_mode=parse_mode, entities=ents2)
        mc.emoji_ids = extract_custom_emoji_ids(entities)
        log_info("✅ 已配置相册消息", extra={"files": len(file_paths), "caption_len": len(caption or ""), "parse_mode": parse_mode or "auto+",
            "entities_count": len(entities or []), **get_log_context()})
        return mc

    @staticmethod
    async def _build_text(conv, client: TelegramClient) -> Optional[MessageContent]:
        _trace("MessageBuilder._build_text", conv=conv, client=client)
        log_info("🧭 开始构建文本消息", extra={**get_log_context()})

        await conv.send_message("✏️ 输入群发的文字消息：")
        tm = await conv.get_response()

        raw_input_text = getattr(tm, "text", "") or ""
        text = (raw_input_text or "").strip()
        if not text:
            await conv.send_message("⚠️ 文本不能为空，请重试 `/b`")
            log_warning("空文本输入，取消构建", extra={**get_log_context()})
            return None

        text, parse_mode, entities, warns = smart_text_parse(text, template_name_map=(emoji_store.name_to_id if emoji_store.loaded else None))
        log_info("🧩 文本解析完毕", extra={"text_len": len(text), "parse_mode": parse_mode or "auto+", "entities_count": len(entities or []), "warnings": len(warns or []), **get_log_context()})

        emoji_ids_from_entities = extract_custom_emoji_ids(entities)
        emoji_ids_from_html = extract_tgemoji_ids_from_html(raw_input_text if parse_mode == "html" else "")
        emoji_ids_final = emoji_ids_from_entities or emoji_ids_from_html

        info = []
        if emoji_ids_final:
            ids_str = "\n".join(f"<code>{eid}</code>" for eid in emoji_ids_final)
            info.append(f"✨ 识别到 Premium Emoji（文本）：\n{ids_str}")
        if warns:
            info.append("⚠️ 解析告警：\n" + "\n".join(f"- {w}" for w in warns))
        if info:
            await conv.send_message("\n\n".join(info), parse_mode="html")

        try:
            # 过长文本按官方算法拆分，预览首段，避免 4096 报错
            pm = sanitize_parse_mode(parse_mode or AutoParseMode())
            chunks = list(split_text_for_send(text, pm))
            first_text, first_ents = chunks[0] if chunks else (text, None)
            log_debug("🖼️ 文本预览", extra={"entities_count": len(first_ents or []), **get_log_context()})
            await conv.send_message(first_text, parse_mode=(None if first_ents else (pm or None)), formatting_entities=first_ents or None, link_preview=False)
        except Exception as e:
             log_exception("预览失败（不阻断发送）", exc=e)

        mc = MessageContent.build_text(text, parse_mode=parse_mode, entities=entities, emoji_ids=emoji_ids_final)
        mc.emoji_ids = emoji_ids_final
        log_info("✅ 已配置文本消息", extra={"parse_mode": parse_mode or "auto+", "entities_count": len(entities or []), "preview_len": len(text), "emoji_count": len(emoji_ids_final or []), **get_log_context()})
        await conv.send_message("✅ 文本消息已成功配置", parse_mode="html")
        return mc



    @staticmethod
    async def _build_media(conv, client: TelegramClient) -> Optional[MessageContent]:
        _trace("MessageBuilder._build_media", conv=conv, client=client)
        log_info("🧭 开始构建图文消息", extra={**get_log_context()})

        # 确保保存目录存在
        os.makedirs(MEDIA_DIR, exist_ok=True)

        await conv.send_message("🏞 输入群发的图文消息：")
        mm = await conv.get_response()

        raw_input_caption = getattr(mm, "text", "") or ""
        caption = raw_input_caption
        path = ""

        parse_mode: Optional[ParseMode] = None
        entities: Optional[EntitiesSeq] = None
        emoji_ids_final: List[int] = []

        if caption.strip():
            caption, parse_mode, entities, warns = smart_text_parse(
                caption,
                template_name_map=(emoji_store.name_to_id if emoji_store.loaded else None)
            )
            emoji_ids_from_entities = extract_custom_emoji_ids(entities)
            emoji_ids_from_html = extract_tgemoji_ids_from_html(raw_input_caption if parse_mode == "html" else "")
            emoji_ids_final = emoji_ids_from_entities or emoji_ids_from_html
            log_info(
                "🧩 Caption 解析完毕",
                extra={"caption_len": len(caption), "parse_mode": parse_mode or "none", "entities_count": len(entities or []),
                    "warnings": len(warns or []), "emoji_count": len(emoji_ids_final or []), **get_log_context()}
            )

        if getattr(mm, "media", None):
            path = await client.download_media(mm.media, file=MEDIA_DIR)  # 统一保存目录
            if not path or not os.path.exists(path):
                await conv.send_message("⚠️ 图文消息失败，请重试 `/b`")
                log_warning("媒体下载失败", extra={"path": path, "save_dir": MEDIA_DIR, **get_log_context()})
                return None

            media_type = guess_media_type(mm)
            try:
                fsize = os.path.getsize(path)
            except Exception:
                fsize = None
            log_info("⬇️ 媒体已下载", extra={"media_type": media_type, "file_path": path, "bytes": fsize, "save_dir": MEDIA_DIR, **get_log_context()})

            caption2, ents2 = clip_text_entities(caption or "", list(entities or []), limit=CAPTION_LIMIT)
            ok = await _safe_preview_media(conv, path, media_type, caption2, parse_mode=(parse_mode or None), entities=(ents2 or None))
            if not ok:
                log_warning("媒体预览失败", extra={"media_type": media_type, **get_log_context()})
                return None

        mc = MessageContent.build_media(file_path=path, caption=caption, parse_mode=parse_mode, entities=entities)
        mc.emoji_ids = list(emoji_ids_final or [])
        log_info(
            "✅ 已配置图文消息",
            extra={"file": bool(path), "caption_len": len(caption or ""), "parse_mode": parse_mode or "none",
                "entities_count": len(entities or []), "emoji_count": len(mc.emoji_ids or []), **get_log_context()}
        )
        await conv.send_message("✅ 图文消息已成功配置", parse_mode="html")
        return mc

    @staticmethod
    async def _build_forward(conv) -> Optional[MessageContent]:
        _trace("MessageBuilder._build_forward", conv=conv)
        log_info("🧭 开始构建转发消息", extra={**get_log_context()})

        try:
            await conv.send_message(
                "🔄 请直接转发公开频道中的消息，或发送该消息链接（@username/123、t.me/username/123、t.me/c/ID/123）"
            )
            fm = await conv.get_response()

            from_peer, msg_id = None, None
            fwd_obj = getattr(fm, "forward", None)

            if fwd_obj:
                chat_ent = getattr(fwd_obj, "chat", None)
                chat_username = None
                try:
                    for u in getattr(chat_ent, "usernames", []) or []:
                        if getattr(u, "active", True) and getattr(u, "username", None):
                            chat_username = u.username
                            break
                except Exception:
                    pass
                if not chat_username:
                    chat_username = getattr(chat_ent, "username", None)

                msg_id = getattr(getattr(fwd_obj, "original_fwd", None), "channel_post", None) or getattr(fm, "id", None)
                if chat_username and msg_id:
                    from_peer = f"@{chat_username}"
                    log_info("🔗 成功识别转发频道消息",
                             extra={"from_peer": from_peer, "msg_id": msg_id, "path": "forward_object", **get_log_context()})
                else:
                    log_warning("ℹ️ 仅部分识别转发信息",
                                extra={"has_fwd": True, "from_peer": str(from_peer), "msg_id": msg_id, **get_log_context()})
            else:
                parsed = _extract_peer_and_msg_id_from_text(getattr(fm, "text", ""))
                if parsed:
                    peer_key, msg_id = parsed
                    from_peer = peer_key if str(peer_key).startswith("id:") else f"@{peer_key}"
                    log_info("🔗 成功提取频道链接",
                             extra={"from_peer": from_peer, "msg_id": msg_id, "path": "link_text", **get_log_context()})

            if not (from_peer and msg_id):
                await conv.send_message("❌ 未能识别来源，请确保是公开频道消息，或发送可解析的 t.me 链接。")
                return None

            # 固定为“标准转发+无评论”，具体是否隐藏来源由外层 _run_build_flow 写回 mc.forward_as_copy 决定
            mc = MessageContent.build_forward(
                forward_peer=str(from_peer),
                forward_id=int(msg_id),
                as_copy=False,      # 默认值，随后会被 _run_build_flow 覆盖
                comment="",
                parse_mode=None,
                entities=[],
            )
            mc.emoji_ids = []

            log_info("✅ 已配置转发消息", extra={"from_peer": str(from_peer), "msg_id": int(msg_id), **get_log_context()})
            await conv.send_message("✅ 转发消息已成功配置", parse_mode="html")
            return mc

        except Exception as e:
            log_exception("❌ 构建转发消息失败", exc=e, extra={**get_log_context()})
            try:
                await conv.send_message("❌ 处理转发消息时出错，请稍后重试。")
            except Exception:
                pass
            return None

async def is_self_premium(client: TelegramClient) -> bool:
    me = await client.get_me()
    return bool(getattr(me, "premium", False))

async def premium_guard_strip_entities(client: TelegramClient, entities: Optional[List[Any]]) -> Tuple[Optional[List[Any]], Optional[str]]:
    if not entities:
        return None, None
    try:
        if await is_self_premium(client):
            return entities, None
        stripped = [e for e in entities if not isinstance(e, MessageEntityCustomEmoji)]
        if len(stripped) != len(entities):
            return stripped, "当前账号非 Premium，已移除自定义表情实体（文本保留）。"
        return stripped, None
    except Exception:
        return entities, None
