# typess/message_enums.py
# -*- coding: utf-8 -*-
"""
消息/任务相关的枚举与数据结构
- HealthState / TaskStatus / MessageType：统一状态枚举（带中文名与 emoji）
- MessageContent：消息载体（文本/媒体/相册/转发），兼容 Telethon 实体序列化
- SendTask：发送任务对象（序列化/反序列化向后兼容）
- ensure_entities_objs / ensure_entities_serializable：TLObject <-> dict 转换
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Union, Final, Literal, TypeAlias, Tuple
from datetime import datetime

from pydantic import BaseModel, Field
from telethon.tl import types as t
from telethon.tl.tlobject import TLObject
from telethon.helpers import add_surrogate
from typess.fsm_keys import FSMStage
from typess.link_types import ParsedLink
from unified.logger import log_exception, log_info, log_warning
from unified.trace_context import get_log_context, generate_trace_id, inject_trace_context
from unified.smart_text import smart_text_parse, extract_tgemoji_ids_from_html
from telethon.tl.types import MessageEntityCustomEmoji
__all__ = [
    "TaskStatus",
    "MessageType",
    "MessageContent",
    "SendTask",
    "ensure_entities_objs",
    "ensure_entities_serializable",
]

# ========= 类型别名 & 兼容 =========
# Telethon 某些版本不导出 TypeMessageEntity；回退到 TLObject（不影响 parse/unparse）
try:
    TypeMessageEntity: TypeAlias = t.TypeMessageEntity  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    TypeMessageEntity: TypeAlias = TLObject

EntityDict: TypeAlias = Dict[str, Any]
EntityLike = Union[TypeMessageEntity, EntityDict, TLObject]  # 仍兼容 TLObject/dict
EntitiesSeq = Sequence[EntityLike]
# 与 tg/telethon_aligned.sanitize_parse_mode 对齐（支持 html+/md+）
ParseMode: TypeAlias = Optional[Literal["auto+", "md", "md+", "html", "html+", "mdv2"]]
MediaLike = Union[str, bytes, "Path"]
AlbumLike: TypeAlias = Sequence[MediaLike]


def _utf16_len(s: str) -> int:
    return len(add_surrogate(s or ""))


# ========= 小工具 =========
def _clamp(off: int, length: int, text_len: Optional[int]) -> Tuple[int, int]:
    off = max(0, int(off or 0))
    length = max(0, int(length or 0))
    if text_len is not None:
        off = min(off, text_len)
        if off + length > text_len:
            length = max(0, text_len - off)
    return off, length


def _ensure_https_tme(url: str) -> str:
    """把 @username / t.me/xxx / http://t.me/xxx 统一到 https://t.me/xxx"""
    s = str(url or "").strip()
    if not s:
        return s
    if s.startswith("@"):
        return f"https://t.me/{s[1:]}"
    if s.startswith("t.me/"):
        return f"https://{s}"
    if s.startswith("http://t.me/"):
        return "https://" + s[len("http://") :]
    return s


def _looks_like_peer(s: str) -> bool:
    """
    仅做形态校验（不触网）：
    - @username / t.me/xxx / https://t.me/xxx / tg://user?id=xxx
    - 数字 ID（正/负） / -100 开头频道 ID
    """
    s = str(s or "").strip()
    if not s:
        return False
    if s.startswith("@"):
        return len(s) > 1
    if s.startswith(("t.me/", "http://t.me/", "https://t.me/")):
        return True
    if s.startswith("tg://user?id="):
        return s[len("tg://user?id=") :].isdigit()
    if s.isdigit():
        return True
    if s.startswith("-100") and s[4:].isdigit():
        return True
    return False


# ========= 通用深序列化（日志友好） =========
def deep_serialize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: deep_serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_serialize(i) for i in obj]
    if hasattr(obj, "to_dict"):
        return deep_serialize(obj.to_dict())
    if hasattr(obj, "__dict__"):
        return deep_serialize(vars(obj))
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)


# ========= 实体工具 =========
def ensure_entities_objs(
    entities: Optional[EntitiesSeq] = None,
    __text_len: Optional[int] = None,
) -> List[TypeMessageEntity]:
    """
    dict/TLObject → Telethon 实体对象（UTF-16 下裁剪）
    """
    if not entities:
        return []
    out: List[TypeMessageEntity] = []
    for item in entities:
        # 1) TL 实体直接裁剪（含自定义扩展类型）
        if isinstance(item, TLObject) and item.__class__.__name__.startswith("MessageEntity"):
            try:
                off = int(getattr(item, "offset", 0) or 0) if hasattr(item, "offset") else 0
                length = int(getattr(item, "length", 0) or 0) if hasattr(item, "length") else 0
                off, length = _clamp(off, length, __text_len)
                if length > 0:
                    if hasattr(item, "offset"):
                        setattr(item, "offset", off)
                    if hasattr(item, "length"):
                        setattr(item, "length", length)
                    out.append(item)  # type: ignore
            except Exception as e:
                log_warning("实体对象裁剪失败", extra={"error": str(e), "sample": repr(item)[:120]})
            continue

        # 2) dict → TL
        if not isinstance(item, dict):
            log_warning("未知实体类型（非 dict/TLObject），已忽略", extra={"sample": repr(item)[:120]})
            continue

        typ = str(item.get("type", "")).strip().lower()
        off, length = _clamp(item.get("offset", 0), item.get("length", 0), __text_len)
        if length <= 0:
            continue
        try:
            if typ == "bold":
                out.append(t.MessageEntityBold(offset=off, length=length))
            elif typ == "italic":
                out.append(t.MessageEntityItalic(offset=off, length=length))
            elif typ == "underline":
                out.append(t.MessageEntityUnderline(offset=off, length=length))
            elif typ in ("strike", "s", "del", "strikethrough"):
                out.append(t.MessageEntityStrike(offset=off, length=length))
            elif typ == "code":
                out.append(t.MessageEntityCode(offset=off, length=length))
            elif typ == "pre":
                lang = str(item.get("language", "") or "")
                out.append(t.MessageEntityPre(offset=off, length=length, language=lang))
            elif typ in ("blockquote", "quote"):
                out.append(t.MessageEntityBlockquote(offset=off, length=length))
            elif typ == "spoiler":
                out.append(t.MessageEntitySpoiler(offset=off, length=length))
            elif typ == "url":
                out.append(t.MessageEntityUrl(offset=off, length=length))
            elif typ in ("text_url", "texturl", "link"):
                url = _ensure_https_tme(str(item.get("url", "") or "").strip())
                if url:
                    out.append(t.MessageEntityTextUrl(offset=off, length=length, url=url))
            elif typ == "email":
                out.append(t.MessageEntityEmail(offset=off, length=length))
            elif typ in ("mention", "@"):
                out.append(t.MessageEntityMention(offset=off, length=length))
            elif typ in ("mention_name", "mentionname", "user_mention"):
                user_id = item.get("user_id")
                username = item.get("username")
                if user_id:
                    url = f"tg://user?id={int(user_id)}"
                    out.append(t.MessageEntityTextUrl(offset=off, length=length, url=url))
                elif isinstance(username, str) and username:
                    url = _ensure_https_tme(username if username.startswith("@") else f"@{username}")
                    out.append(t.MessageEntityTextUrl(offset=off, length=length, url=url))
            elif typ in ("custom_emoji", "customemoji", "emoji"):
                doc_id = int(item.get("document_id"))
                out.append(t.MessageEntityCustomEmoji(offset=off, length=length, document_id=doc_id))
            elif typ == "hashtag":
                out.append(t.MessageEntityHashtag(offset=off, length=length))
            elif typ in ("bot_command", "command"):
                out.append(t.MessageEntityBotCommand(offset=off, length=length))
            elif typ == "cashtag":
                out.append(t.MessageEntityCashtag(offset=off, length=length))
            elif typ == "phone":
                out.append(t.MessageEntityPhone(offset=off, length=length))
            elif typ in ("bank_card", "bankcard"):
                out.append(t.MessageEntityBankCard(offset=off, length=length))
            elif typ == "unknown":
                out.append(t.MessageEntityUnknown(offset=off, length=length))
            else:
                log_warning("未知实体类型（已忽略）", extra={"type": typ, "sample": item})
        except Exception as e:
            log_warning("实体构造失败（已忽略）", extra={"error": str(e), "sample": item})
    return out


def ensure_entities_serializable(entities: Optional[EntitiesSeq] = None) -> List[dict]:
    out: List[dict] = []
    for e in entities or []:
        if isinstance(e, dict):
            out.append(e)
            continue
        try:
            base = {
                "offset": int(getattr(e, "offset", 0) or 0),
                "length": int(getattr(e, "length", 0) or 0),
            }
            if isinstance(e, t.MessageEntityBold):
                out.append({"type": "bold", **base})
            elif isinstance(e, t.MessageEntityItalic):
                out.append({"type": "italic", **base})
            elif isinstance(e, t.MessageEntityUnderline):
                out.append({"type": "underline", **base})
            elif isinstance(e, t.MessageEntityStrike):
                out.append({"type": "strike", **base})
            elif isinstance(e, t.MessageEntityCode):
                out.append({"type": "code", **base})
            elif isinstance(e, t.MessageEntityPre):
                out.append({"type": "pre", "language": getattr(e, "language", "") or "", **base})
            elif isinstance(e, t.MessageEntityBlockquote):
                out.append({"type": "blockquote", **base})
            elif isinstance(e, t.MessageEntitySpoiler):
                out.append({"type": "spoiler", **base})
            elif isinstance(e, t.MessageEntityUrl):
                out.append({"type": "url", **base})
            elif isinstance(e, t.MessageEntityTextUrl):
                out.append({"type": "text_url", "url": getattr(e, "url", "") or "", **base})
            elif isinstance(e, t.MessageEntityEmail):
                out.append({"type": "email", **base})
            elif isinstance(e, t.MessageEntityMention):
                out.append({"type": "mention", **base})
            elif isinstance(e, (t.MessageEntityMentionName, t.InputMessageEntityMentionName)):
                # 统一反序列化为 text_url，避免对 MentionName 的耦合
                try:
                    uid = int(getattr(e, "user_id", 0) or 0)
                except Exception:
                    uid = 0
                out.append({"type": "text_url", "url": (f"tg://user?id={uid}" if uid else ""), **base})
            elif isinstance(e, t.MessageEntityCustomEmoji):
                out.append(
                    {
                        "type": "custom_emoji",
                        "document_id": int(getattr(e, "document_id", 0) or 0),
                        **base,
                    }
                )
            elif isinstance(e, t.MessageEntityHashtag):
                out.append({"type": "hashtag", **base})
            elif isinstance(e, t.MessageEntityBotCommand):
                out.append({"type": "bot_command", **base})
            elif isinstance(e, t.MessageEntityCashtag):
                out.append({"type": "cashtag", **base})
            elif isinstance(e, t.MessageEntityPhone):
                out.append({"type": "phone", **base})
            elif isinstance(e, t.MessageEntityBankCard):
                out.append({"type": "bank_card", **base})
            elif isinstance(e, t.MessageEntityUnknown):
                out.append({"type": "unknown", **base})
            else:
                out.append(
                    {"type": e.__class__.__name__.replace("MessageEntity", "").lower(), **base}
                )
        except Exception as ex:
            log_warning("实体序列化失败（已忽略）", extra={"error": str(ex), "sample": repr(e)[:120]})
    return out


# ========= 任务与消息状态 =========
class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"
    PAUSED = "paused"

    def zh_name(self) -> str:
        return {
            TaskStatus.PENDING: "待开始",
            TaskStatus.RUNNING: "进行中",
            TaskStatus.COMPLETED: "已完成",
            TaskStatus.STOPPED: "已终止",
            TaskStatus.FAILED: "失败",
            TaskStatus.PAUSED: "已暂停",
        }.get(self, "未知状态")

    def emoji(self) -> str:
        return {
            TaskStatus.PENDING: "⏳",
            TaskStatus.RUNNING: "🚀",
            TaskStatus.COMPLETED: "✅",
            TaskStatus.STOPPED: "🛑",
            TaskStatus.FAILED: "❌",
            TaskStatus.PAUSED: "⏸️",
        }.get(self, "❓")

    def format(self) -> str:
        return f"{self.emoji()} {self.zh_name()}"


class MessageType(str, Enum):
    TEXT = "text"
    MEDIA = "media"
    FORWARD = "forward"
    ALBUM = "album"

    def zh_name(self) -> str:
        return {
            MessageType.TEXT: "文字消息",
            MessageType.MEDIA: "图文消息",
            MessageType.FORWARD: "转发消息",
            MessageType.ALBUM: "相册消息",
        }.get(self, "未知消息")

    def emoji(self) -> str:
        return {
            MessageType.TEXT: "📝",
            MessageType.MEDIA: "🖼️",
            MessageType.FORWARD: "🔁",
            MessageType.ALBUM: "📚",
        }.get(self, "❓")


# ========= 构造器/校验器注册 =========
_BUILDERS: Final[Dict["MessageType", Callable[..., "MessageContent"]]] = {}
_VALIDATORS: Final[List[Callable[["MessageContent"], bool]]] = []


def register_builder(type_: "MessageType", func: Callable[..., "MessageContent"]) -> None:
    if type_ in _BUILDERS and _BUILDERS[type_] is not func:
        log_warning("覆盖已注册的 MessageType 构造器", extra={"type": type_.value})
    _BUILDERS[type_] = func


def register_validator(func: Callable[["MessageContent"], bool]) -> None:
    if func not in _VALIDATORS:
        _VALIDATORS.append(func)


# ========= parse_mode 归一化 =========
def normalize_parse_mode(mode: Any, *, default: Optional[str] = "auto+") -> Optional[str]:
    """
    将多样输入统一为内部字符串存储（发送层由 tg/telethon_aligned 处理 parse/unparse）
    """
    if mode is None:
        return None
    s = str(mode).strip().lower()
    if s in {"", "none", "plain", "off", "false", "text"}:
        return None
    if s in {"html", "htm", "html+"}:
        return s if s.endswith("+") else "html"
    if s in {"md", "markdown", "md+", "markdown+"}:
        return "md" if s in {"md", "markdown"} else "md+"
    if s in {"markdownv2", "markdown2", "md2", "mdv2", "markdown_2"}:
        return "mdv2"
    if s in {"auto", "auto+"}:
        return "auto+"
    if mode is True or s in {"true", "on"}:
        return "html"
    return default


# ========= 消息模型 =========
class MessageContent(BaseModel):
    # ===== 核心类型 =====
    type: MessageType

    # 文本相关
    content: Optional[str] = None          # 纯文本 or forward 的可选评论
    caption: Optional[str] = None          # 媒体/相册的说明文字

    # 媒体
    media: Optional[MediaLike] = None
    media_group: Optional[List[MediaLike]] = None

    # 转发
    forward_peer: Optional[str] = None     # 源 peer：@user / t.me/c/.. / tg://user?id=..
    forward_id: Optional[int] = None       # 源消息 ID
    # —— Telethon 主推：drop_author 语义（无引用转发 = True）
    drop_author: Optional[bool] = None

    # —— 兼容旧字段（仍支持读写；最终统一映射到 drop_author）
    forward_show_author: Optional[bool] = False
    forward_as_copy: Optional[bool] = None

    # 兼容历史键位（链接型源，交由发送器解析）
    forward_link: Optional[str] = None

    # 富文本
    parse_mode: ParseMode = "auto+"
    entities: Optional[EntitiesSeq] = None

    # ===== 发送选项（对齐 Telethon Message 行为）=====
    reply_to_msg_id: Optional[int] = None  # 作为回复发送
    link_preview: Optional[bool] = None    # 是否展示链接预览（仅文本）
    silent: Optional[bool] = None          # 静默发送（不打扰）
    schedule_date: Optional[datetime] = None  # 定时发送
    noforwards: Optional[bool] = None      # 禁止此消息被转发（可用即透传）

    # 额外信息
    extra: Optional[dict] = None
    trace_id: str = ""
    emoji_ids: List[int] = Field(default_factory=list)
    timestamp: int = Field(default_factory=lambda: int(time.time()))

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

    # ---- 便捷只读属性 ----
    @property
    def text(self) -> Optional[str]:
        if self.type == MessageType.TEXT:
            return self.content
        if self.type in {MessageType.MEDIA, MessageType.ALBUM}:
            return self.caption
        if self.type == MessageType.FORWARD:
            return self.content
        return None

    # ---- 兼容：统一计算最终的 drop_author（权重：drop_author > forward_as_copy > forward_show_author）----
    def forward_drop_author(self) -> Optional[bool]:
        if self.drop_author is not None:
            return bool(self.drop_author)
        if self.forward_as_copy is not None:
            # as_copy=True 表示“无引用转发”，等价 drop_author=True
            return bool(self.forward_as_copy)
        if self.forward_show_author is not None:
            return not bool(self.forward_show_author)
        return None

    # ---- 合法性校验 ----
    def is_ready(self) -> bool:
        # 兼容你项目里的自定义校验器链
        from .message_enums import _VALIDATORS
        return all(v(self) for v in _VALIDATORS) if _VALIDATORS else True

    def validate(self) -> None:
        if self.type == MessageType.MEDIA and not self.media:
            raise ValueError("MEDIA 消息必须提供非空的 media")
        if self.type == MessageType.ALBUM and (not self.media_group or not list(self.media_group)):
            raise ValueError("ALBUM 消息必须提供非空的 media_group 列表")
        if self.type == MessageType.FORWARD and (not self.forward_peer or not self.forward_id):
            raise ValueError("FORWARD 消息必须包含 forward_peer 和 forward_id")

    # ---- 统一导出发送选项（给发送器透传）----
    def to_send_options(self) -> dict:
        """
        返回 Telethon 友好的可选参数字典：
        - 文本/媒体：reply_to、link_preview、silent、schedule、noforwards
        - 转发：drop_author 由 forward_drop_author() 给出
        """
        opts = {}
        if self.reply_to_msg_id:
            opts["reply_to"] = int(self.reply_to_msg_id)
        if self.link_preview is not None and self.type == MessageType.TEXT:
            opts["link_preview"] = bool(self.link_preview)
        if self.silent is not None:
            opts["silent"] = bool(self.silent)
        if self.schedule_date is not None:
            opts["schedule"] = self.schedule_date
        if self.noforwards is not None:
            opts["noforwards"] = bool(self.noforwards)
        # forward 的 drop_author 单独在 forward_params() 里返回
        return opts

    def forward_params(self) -> dict:
        """
        仅当 type=FORWARD 时使用。
        返回 { "from_peer": forward_peer, "message_id": forward_id, "drop_author": Optional[bool] }
        （其中 from_peer 为原始字符串，解析放在发送器里做）
        """
        if self.type != MessageType.FORWARD:
            return {}
        return {
            "from_peer": self.forward_peer,
            "message_id": int(self.forward_id) if self.forward_id is not None else None,
            "drop_author": self.forward_drop_author(),
        }

    # ---- 构造器（推荐） ----
    @classmethod
    def build_text(
        cls,
        text: str,
        *,
        entities: Optional[EntitiesSeq] = None,
        parse_mode: Optional[ParseMode | str] = None,
        trace_id: str = "",
        emoji_ids: Optional[List[int]] = None,
        reply_to: Optional[int] = None,
        link_preview: Optional[bool] = None,
        silent: Optional[bool] = None,
        schedule_date: Optional[datetime] = None,
        noforwards: Optional[bool] = None,
    ) -> "MessageContent":
        # 前置解析：仅当未提供 entities 时
        _pm = normalize_parse_mode(parse_mode) or "auto+"
        _ents = list(entities or [])
        if not _ents:
            t2, pm2, ents2, _warns = smart_text_parse(text)
            text = t2
            if pm2:
                _pm = pm2
            if ents2:
                _ents = ents2

        mc = cls(
            type=MessageType.TEXT,
            content=text,
            parse_mode=_pm,
            entities=_ents,
            trace_id=trace_id,
            emoji_ids=list(emoji_ids or []),
            reply_to_msg_id=reply_to,
            link_preview=link_preview,
            silent=silent,
            schedule_date=schedule_date,
            noforwards=noforwards,
        )
        # 自动识别自定义 emoji（如 HTML 模式 <tg-emoji>）
        if not mc.emoji_ids:
            try:
                em_from_ents = [int(getattr(e, "document_id", 0)) for e in (_ents or []) if isinstance(e, MessageEntityCustomEmoji)]
                em_from_html = extract_tgemoji_ids_from_html(text if (_pm == "html") else "")
                mc.emoji_ids = em_from_ents or em_from_html or []
            except Exception:
                pass
        return mc

    @classmethod
    def build_media(
        cls,
        file_path: MediaLike,
        *,
        caption: Optional[str] = "",
        entities: Optional[EntitiesSeq] = None,
        parse_mode: Optional[ParseMode | str] = None,
        trace_id: str = "",
        reply_to: Optional[int] = None,
        silent: Optional[bool] = None,
        schedule_date: Optional[datetime] = None,
        noforwards: Optional[bool] = None,
        emoji_ids: Optional[List[int]] = None,
    ) -> "MessageContent":
        _pm = normalize_parse_mode(parse_mode) or "auto+"
        _ents = list(entities or [])
        cap = caption or ""
        if not _ents and (cap or "").strip():
            c2, pm2, ents2, _warns = smart_text_parse(cap)
            cap = c2
            if pm2:
                _pm = pm2
            if ents2:
                _ents = ents2
        mc = cls(
            type=MessageType.MEDIA,
            media=file_path,
            caption=cap,
            content=cap,
            parse_mode=_pm,
            entities=_ents,
            trace_id=trace_id,
            reply_to_msg_id=reply_to,
            silent=silent,
            schedule_date=schedule_date,
            noforwards=noforwards,
        )
        if emoji_ids:
            mc.emoji_ids = list(emoji_ids)
        return mc

    @classmethod
    def build_album(
        cls,
        file_paths: Sequence[MediaLike],
        *,
        caption: Optional[str] = "",
        format: str = "html",
        entities: Optional[EntitiesSeq] = None,
        parse_mode: Optional[ParseMode | str] = None,
        trace_id: str = "",
        reply_to: Optional[int] = None,
        silent: Optional[bool] = None,
        schedule_date: Optional[datetime] = None,
        noforwards: Optional[bool] = None,
        emoji_ids: Optional[List[int]] = None,
    ) -> "MessageContent":
        if not file_paths:
            raise ValueError("ALBUM 需要至少 1 个文件（建议 2~10）")
        _pm = normalize_parse_mode(parse_mode) or "auto+"
        _ents = list(entities or [])
        cap = caption or ""
        if not _ents and (cap or "").strip():
            c2, pm2, ents2, _warns = smart_text_parse(cap)
            cap = c2
            if pm2:
                _pm = pm2
            if ents2:
                _ents = ents2
        mc = cls(
            type=MessageType.ALBUM,
            media_group=[p for p in file_paths],
            caption=cap,
            content=cap,
            parse_mode=_pm,
            entities=_ents,
            trace_id=trace_id,
            reply_to_msg_id=reply_to,
            silent=silent,
            schedule_date=schedule_date,
            noforwards=noforwards,
        )
        if emoji_ids:
            mc.emoji_ids = list(emoji_ids)
        return mc

    @classmethod
    def build_forward(
        cls,
        *,
        forward_peer: str,
        forward_id: int,
        # —— 新推荐：直接给 drop_author（等价“无引用/复制式转发”）
        drop_author: Optional[bool] = None,
        # —— 兼容旧参（若设置将被映射为 drop_author）：
        show_author: Optional[bool] = None,
        as_copy: Optional[bool] = None,
        comment: Optional[str] = None,
        entities: Optional[EntitiesSeq] = None,
        parse_mode: Optional[ParseMode | str] = None,
        trace_id: str = "",
        reply_to: Optional[int] = None,
        silent: Optional[bool] = None,
        schedule_date: Optional[datetime] = None,
        noforwards: Optional[bool] = None,
        forward_link: Optional[str] = None,
    ) -> "MessageContent":
        pm = normalize_parse_mode(parse_mode) or "auto+"
        # 折衷合并：优先使用 drop_author；否则从 as_copy / show_author 推导
        da = drop_author
        if da is None and as_copy is not None:
            da = bool(as_copy)
        if da is None and show_author is not None:
            da = not bool(show_author)

        return cls(
            type=MessageType.FORWARD,
            forward_peer=str(forward_peer),
            forward_id=int(forward_id),
            drop_author=da,
            # 兼容字段也保留写入，便于回显 & 向后保存
            forward_show_author=bool(show_author) if show_author is not None else False,
            forward_as_copy=bool(as_copy) if as_copy is not None else None,
            content=comment or "",
            parse_mode=pm,
            entities=entities or [],
            trace_id=trace_id,
            reply_to_msg_id=reply_to,
            silent=silent,
            schedule_date=schedule_date,
            noforwards=noforwards,
            forward_link=forward_link,
        )

    @classmethod
    def build(cls, type_: MessageType, **kwargs) -> "MessageContent":
        from .message_enums import _BUILDERS  # 避免循环导入
        fn = _BUILDERS.get(type_)
        if fn:
            return fn(**kwargs)

        parse_mode = normalize_parse_mode(kwargs.get("parse_mode", kwargs.get("format"))) or "auto+"
        entities: Optional[EntitiesSeq] = kwargs.get("entities") or kwargs.get("formatting_entities")

        common_opts = dict(
            trace_id=kwargs.get("trace_id", ""),
        )

        if type_ == MessageType.TEXT:
            return cls.build_text(
                text=str(kwargs.get("content") or kwargs.get("text") or ""),
                parse_mode=parse_mode,
                entities=entities,
                emoji_ids=kwargs.get("emoji_ids"),
                reply_to=kwargs.get("reply_to") or kwargs.get("reply_to_msg_id"),
                link_preview=kwargs.get("link_preview"),
                silent=kwargs.get("silent"),
                schedule_date=kwargs.get("schedule") or kwargs.get("schedule_date"),
                noforwards=kwargs.get("noforwards"),
                **common_opts,
            )

        if type_ == MessageType.MEDIA:
            fp = kwargs.get("file_path") or kwargs.get("media")
            if not fp:
                raise ValueError("MEDIA 需要提供 file_path/media")
            return cls.build_media(
                file_path=fp,
                caption=str(kwargs.get("caption", "")),
                parse_mode=parse_mode,
                entities=entities,
                reply_to=kwargs.get("reply_to") or kwargs.get("reply_to_msg_id"),
                silent=kwargs.get("silent"),
                schedule_date=kwargs.get("schedule") or kwargs.get("schedule_date"),
                noforwards=kwargs.get("noforwards"),
                emoji_ids=kwargs.get("emoji_ids"),
                **common_opts,
            )

        if type_ == MessageType.ALBUM:
            fps = kwargs.get("file_paths") or kwargs.get("media_group") or []
            if not isinstance(fps, (list, tuple)) or not fps:
                raise ValueError("ALBUM 需要提供非空的 file_paths/media_group 列表")
            return cls.build_album(
                file_paths=fps,
                caption=str(kwargs.get("caption", "")),
                parse_mode=parse_mode,
                entities=entities,
                reply_to=kwargs.get("reply_to") or kwargs.get("reply_to_msg_id"),
                silent=kwargs.get("silent"),
                schedule_date=kwargs.get("schedule") or kwargs.get("schedule_date"),
                noforwards=kwargs.get("noforwards"),
                emoji_ids=kwargs.get("emoji_ids"),
                **common_opts,
            )

        if type_ == MessageType.FORWARD:
            peer = kwargs.get("peer") or kwargs.get("forward_peer")
            mid = kwargs.get("msg_id") or kwargs.get("forward_id")
            if not peer or not mid:
                raise ValueError("FORWARD 需要提供 peer/msg_id（或 forward_peer/forward_id）")
            return cls.build_forward(
                forward_peer=str(peer),
                forward_id=int(mid),
                drop_author=kwargs.get("drop_author"),
                show_author=kwargs.get("show_author", kwargs.get("forward_show_author")),
                as_copy=kwargs.get("as_copy", kwargs.get("forward_as_copy")),
                comment=str(kwargs.get("comment") or kwargs.get("content") or ""),
                parse_mode=parse_mode,
                entities=entities,
                reply_to=kwargs.get("reply_to") or kwargs.get("reply_to_msg_id"),
                silent=kwargs.get("silent"),
                schedule_date=kwargs.get("schedule") or kwargs.get("schedule_date"),
                noforwards=kwargs.get("noforwards"),
                forward_link=kwargs.get("forward_link"),
                **common_opts,
            )

        raise ValueError(f"未注册构造器且不支持的消息类型: {type_}")

    # ---- 序列化 / 反序列化 ----
    def to_dict(self) -> dict:
        """裁剪 entities → 可存储 dict（与 from_dict 对称）"""
        d = self.model_dump()
        # 依据“有效文本”裁剪实体（UTF-16）
        txt = self.caption if self.type in {MessageType.MEDIA, MessageType.ALBUM} else (self.content or "")
        if self.entities:
            try:
                cropped = ensure_entities_objs(self.entities, __text_len=_utf16_len(txt))
                d["entities"] = ensure_entities_serializable(cropped)
            except Exception:
                d["entities"] = ensure_entities_serializable(self.entities)
        d["type"] = self.type.value
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "MessageContent":
        d = dict(data or {})
        # 类型回退保护
        try:
            raw_type = d.get("type", "text")
            d["type"] = raw_type if isinstance(raw_type, MessageType) else MessageType(str(raw_type))
        except Exception:
            log_warning("MessageContent.from_dict: 非法 type，已回退为 text", extra={"type": data.get("type")})
            d["type"] = MessageType.TEXT

        d["parse_mode"] = normalize_parse_mode(d.get("parse_mode", d.get("format"))) or "auto+"

        # 旧字段 → drop_author 兼容折叠
        if d.get("drop_author") is None:
            if d.get("forward_as_copy") is not None:
                d["drop_author"] = bool(d["forward_as_copy"])
            elif d.get("forward_show_author") is not None:
                d["drop_author"] = not bool(d["forward_show_author"])

        ents = d.get("entities")
        if ents:
            try:
                kind = d["type"].value if isinstance(d["type"], MessageType) else str(d.get("type", "text"))
                txt = str(d.get("caption") if kind in ("media", "album") else d.get("content") or "")
                d["entities"] = ensure_entities_objs(ents, __text_len=_utf16_len(txt))
            except Exception:
                d["entities"] = None
        return cls(**d)

    def __repr__(self) -> str:
        from textwrap import shorten
        return f"<MessageContent type={self.type} text={shorten(self.text or '', 40)} media={self.media}>"


# ========= 默认构造器 =========
def _build_text_default(**kwargs) -> "MessageContent":
    return MessageContent.build_text(
        text=kwargs.get("content", ""),
        parse_mode=kwargs.get("parse_mode", kwargs.get("format", "auto+")),
        entities=kwargs.get("entities"),
        emoji_ids=kwargs.get("emoji_ids"),
    )


def _build_media_default(**kwargs) -> "MessageContent":
    return MessageContent.build_media(
        file_path=kwargs.get("file_path") or kwargs.get("media"),
        caption=kwargs.get("caption", ""),
        parse_mode=kwargs.get("parse_mode", kwargs.get("format", "auto+")),
        entities=kwargs.get("entities"),
        emoji_ids=kwargs.get("emoji_ids"),
    )


def _build_album_default(**kwargs) -> "MessageContent":
    return MessageContent.build_album(
        file_paths=kwargs.get("file_paths") or kwargs.get("media_group"),
        caption=kwargs.get("caption", ""),
        parse_mode=kwargs.get("parse_mode", kwargs.get("format", "auto+")),
        entities=kwargs.get("entities"),
        emoji_ids=kwargs.get("emoji_ids"),
    )


def _build_forward_default(**kwargs) -> "MessageContent":
    peer = kwargs.get("peer") or kwargs.get("forward_peer")
    fid  = kwargs.get("msg_id") or kwargs.get("forward_id")
    if not peer or not fid:
        raise ValueError("FORWARD 需要提供 peer/msg_id（或 forward_peer/forward_id）")

    # 兼容旧参数名 → 统一折叠为 drop_author / as_copy / show_author
    as_copy_val   = kwargs.get("as_copy", kwargs.get("forward_as_copy"))
    show_author   = kwargs.get("show_author", kwargs.get("forward_show_author"))
    drop_author   = kwargs.get("drop_author")

    if drop_author is None:
        if as_copy_val is not None:
            drop_author = bool(as_copy_val)           # as_copy=True => 无引用转发 => drop_author=True
        elif show_author is not None:
            drop_author = (not bool(show_author))     # 显示来源=False => drop_author=True

    return MessageContent.build_forward(
        forward_peer=str(peer),
        forward_id=int(fid),
        drop_author=drop_author,
        show_author=show_author,
        as_copy=as_copy_val,
        comment=(kwargs.get("comment") or kwargs.get("content") or ""),
        parse_mode=kwargs.get("parse_mode", kwargs.get("format", "auto+")),
        entities=kwargs.get("entities"),
    )


# 注册默认构造器
register_builder(MessageType.TEXT, _build_text_default)
register_builder(MessageType.MEDIA, _build_media_default)
register_builder(MessageType.ALBUM, _build_album_default)
register_builder(MessageType.FORWARD, _build_forward_default)


# ========= 任务模型 =========
@dataclass
class SendTask:
    task_id: str
    user_id: int
    group_list: List[str]
    message: MessageContent
    interval: int = 60
    status: TaskStatus = TaskStatus.PENDING
    stats: Dict[str, Any] = field(default_factory=dict)
    whitelist: List[int] = field(default_factory=list)
    username: str = ""
    stage: FSMStage = FSMStage.INIT

    links: List[ParsedLink] = field(default_factory=list)
    meta: Optional[Dict[str, Any]] = None
    trace_id: str = ""  # 显式 trace_id

    def iter_targets(self) -> List[ParsedLink]:
        return list(self.links or [])

    def _log_task(self):
        """统一日志记录功能"""
        log_info(
            "SendTask operation",
            extra={"task_id": self.task_id, "user_id": self.user_id, "group_list_len": len(self.group_list)},
        )

    def get_trace(self) -> dict:
        ctx = get_log_context() or {}
        tid = ctx.get("trace_id") or (self.trace_id if getattr(self, "trace_id", None) else None) or generate_trace_id()
        try:
            self.trace_id = tid
        except Exception:
            pass
        return {"trace_id": tid}

    def to_json(self) -> str:
        """序列化 SendTask → JSON，兼容老版本 status/stage/trace_id"""
        self.get_trace()  # 确保 trace_id 注入
        links_json = [pl.to_dict() for pl in self.links if pl]
        try:
            return json.dumps(
                {
                    "task_id": self.task_id,
                    "user_id": self.user_id,
                    "group_list": self.group_list,
                    "links": links_json,
                    "message": self.message.to_dict(),
                    "interval": self.interval,
                    "status": self.status.value if isinstance(self.status, TaskStatus) else str(self.status),
                    "stats": self.stats,
                    "whitelist": self.whitelist,
                    "username": self.username,
                    "stage": self.stage.value if isinstance(self.stage, FSMStage) else str(self.stage),
                    "trace_id": self.trace_id,
                },
                ensure_ascii=False,
            )
        except Exception as e:
            log_exception("❌ SendTask.to_json failed", exc=e, extra={"task_id": self.task_id, "user_id": self.user_id})
            raise

    @staticmethod
    def from_json(data: str) -> "SendTask":
        """反序列化 JSON → SendTask，兼容旧字段缺失/非法值"""
        try:
            raw = json.loads(data)
        except Exception as e:
            log_exception("❌ SendTask.from_json JSON 解析失败", exc=e)
            raise

        inject_trace_context(func_name="SendTask.from_json")

        # message 恢复
        try:
            message = MessageContent.from_dict(raw.get("message", {}))
        except Exception as e:
            log_exception("SendTask.from_json: 恢复 message 失败，使用空文本", exc=e)
            message = MessageContent.build_text("")

        # status 恢复
        try:
            status_raw = raw.get("status", TaskStatus.PENDING.value)
            status = status_raw if isinstance(status_raw, TaskStatus) else TaskStatus(str(status_raw))
        except Exception:
            status = TaskStatus.PENDING

        # stage 恢复（带容错）
        try:
            stage_raw = raw.get("stage", FSMStage.INIT.value)
            if isinstance(stage_raw, FSMStage):
                stage = stage_raw
            else:
                try:
                    stage = FSMStage(str(stage_raw))
                except ValueError:
                    log_warning(
                        "SendTask.from_json: 非法 stage，已回退 INIT",
                        extra={"stage_raw": stage_raw, "task_id": raw.get("task_id")},
                    )
                    stage = FSMStage.INIT
        except Exception:
            stage = FSMStage.INIT

        # links 恢复
        links = []
        for d in raw.get("links", []) or []:
            try:
                if isinstance(d, dict):
                    links.append(ParsedLink.from_dict(d))
            except Exception:
                continue

        return SendTask(
            task_id=raw.get("task_id", str(uuid.uuid4())),
            user_id=int(raw.get("user_id", 0)),
            group_list=list(raw.get("group_list", [])),
            message=message,
            interval=int(raw.get("interval", 60) or 60),
            status=status,
            stats=dict(raw.get("stats", {})),
            whitelist=[int(x) for x in raw.get("whitelist", []) if str(x).isdigit()],
            username=str(raw.get("username", "")),
            stage=stage,
            links=links,
            meta=raw.get("meta"),
            trace_id=str(raw.get("trace_id") or ""),
        )

    def get_links(self) -> List[ParsedLink]:
        from tg.link_utils import standardize_and_dedup_links

        parsed = standardize_and_dedup_links(self.group_list)
        inject_trace_context(func_name="SendTask.get_links", user_id=self.user_id, task_id=self.task_id)

        valid_links = [pl for _, pl in parsed if getattr(pl, "is_valid", lambda: True)()]
        invalid_links = [raw for raw, pl in parsed if not getattr(pl, "is_valid", lambda: True)()]

        if invalid_links:
            log_warning(
                "⚠️ 群链接解析有失败项",
                extra={"user_id": self.user_id, "task_id": self.task_id, "invalid_links": invalid_links[:5]},
            )
        else:
            log_info("✅ 群链接解析成功", extra={"user_id": self.user_id, "task_id": self.task_id})

        return valid_links
