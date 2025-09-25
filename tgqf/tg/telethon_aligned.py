# tg/telethon_aligned.py
# -*- coding: utf-8 -*-
"""
基于 Telethon 官方解析器的轻量封装：
- 优先使用官方 parse/unparse（telethon.extensions.html / markdown）
- 提供 AutoParseMode（自动在 HTML/Markdown 间选择）
- sanitize_parse_mode 仅在接收到 'auto'/'auto+' 时返回 AutoParseMode，其余委托给官方
- normalize_for_send：统一三元组 (text, entities or None, parse_mode or None)
  - 若显式给了 entities，则裁剪后直接使用（不再走 parse）
  - 否则根据 parse_mode（若为 'auto' 则使用 AutoParseMode）解析
  - 若解析有实体 → 返回 (parsed_text, entities, None)
  - 若解析无实体 → 返回 (text, None, parse_mode 对象)，交给 Telethon 渲染
"""
from __future__ import annotations

import re
from typing import Any, Iterable, List, Optional, Tuple, TypeAlias, Union

from telethon import utils as tg_utils
from telethon.extensions import html
from telethon.extensions import markdown as _md
from telethon.helpers import add_surrogate
from telethon.tl import types as t
from telethon.tl.tlobject import TLObject
from telethon.utils import sanitize_parse_mode as _sanitize_mode, split_text as _split_text
from typess.message_enums import ParseMode, ensure_entities_objs  # 复用项目内实体裁剪

__all__ = [
    "TypeMessageEntity",
    "AutoParseMode",
    "sanitize_parse_mode",
    "looks_like_html",
    "split_text_for_send",
    "clip_text_entities",
    "normalize_for_send",
]

# —— TypeMessageEntity 兜底（某些环境里可能缺失该别名）
try:
    TypeMessageEntity: TypeAlias = t.TypeMessageEntity  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    TypeMessageEntity: TypeAlias = TLObject

# —— 简单 HTML 标签检测（用于 Auto 模式的启发式选择）
_HTML_TAG_RE = re.compile(
    r"</?(a|b|strong|i|em|u|s|strike|del|code|pre|blockquote)\b[^>]*>",
    re.I,
)


class AutoParseMode:
    """
    Auto 解析模式（最小自定义）：
    - 如果文本看起来像 HTML → 使用 telethon.extensions.html.parse
    - 否则 → 使用 telethon.extensions.markdown.parse
    - 反解统一走 html.unparse（官方实现）
    """
    @staticmethod
    def parse(text: str) -> Tuple[str, List[TypeMessageEntity]]:
        text = text or ""
        if looks_like_html(text):
            return html.parse(text)
        return _md.parse(text)

    @staticmethod
    def unparse(text: str, entities: Iterable[TypeMessageEntity]) -> str:
        return html.unparse(text or "", list(entities or []))


def sanitize_parse_mode(mode: Union[ParseMode, str, Any]) -> Optional[Any]:
    if mode is None:
        return None
    if isinstance(mode, str) and mode.strip().lower() in {"auto", "auto+"}:
        return AutoParseMode()
    return _sanitize_mode(mode)

MESSAGE_LIMIT = 4096
CAPTION_LIMIT = 1024


def _pick_limit(is_caption: bool | None, limit: Optional[int]) -> int:
    if isinstance(limit, int) and limit > 0:
        return limit
    return CAPTION_LIMIT if is_caption else MESSAGE_LIMIT


def clip_text_entities(text: str, entities: Optional[List[TypeMessageEntity]], *, limit: int) -> Tuple[str, Optional[List[TypeMessageEntity]]]:
    """UTF-16 对齐安全裁剪文本与实体。"""
    text = text or ""
    if limit and limit > 0 and len(text) > limit:
        text = text[:limit]
    ents = ensure_entities_objs(list(entities or []), __text_len=len(add_surrogate(text)))
    return text, (ents or None)


def split_text_for_send(
    text: str,
    parse_mode: Optional[Any] = None,
    entities: Optional[List[TypeMessageEntity]] = None,
    *,
    is_caption: bool = False,
    limit: Optional[int] = None,
):
    """
    安全分片：
    - 若传入 entities → 直接按实体分片（不再重新 parse）
    - 否则：若 parse_mode 解析得到实体 → 官方 split_text 保格式
             若无实体 → 朴素切片
    """
    text = text or ""
    use_limit = _pick_limit(is_caption, limit)

    # 1) 显式实体 → 直接分片
    if entities:
        fixed_text, fixed_ents = clip_text_entities(text, entities, limit=use_limit)
        for part_text, part_ents in _split_text(fixed_text, fixed_ents or [], limit=use_limit):
            yield part_text, part_ents
        return

    # 2) 解析模式
    pm = sanitize_parse_mode(parse_mode or AutoParseMode())
    if pm:
        parsed_text, parsed_ents = pm.parse(text)
        parsed_ents = [e for e in (parsed_ents or []) if int(getattr(e, "length", 0) or 0) > 0]
        if parsed_ents:
            # 有实体 → 官方安全分片
            for part_text, part_ents in _split_text(parsed_text, parsed_ents, limit=use_limit):
                yield part_text, part_ents
            return
        # 无实体 → 朴素切片
        s = parsed_text
    else:
        s = text

    i = 0
    while i < len(s):
        yield s[i:i + use_limit], None
        i += use_limit


def looks_like_html(text: str) -> bool:
    return bool(_HTML_TAG_RE.search(text or ""))


def normalize_for_send(
    text: str,
    parse_mode: Optional[ParseMode | Any],
    entities: Optional[Iterable] = None,
    *,
    is_caption: bool = False,
    limit: Optional[int] = None,
) -> Tuple[str, Optional[List[TypeMessageEntity]], Optional[Any]]:
    """
    返回 (text, entities_or_None, parse_mode_obj_or_None)

    规则：
    1) 若调用方传入 entities → 优先使用，并按消息类型自动限长裁剪（UTF-16 对齐），parse_mode 置空。
    2) 否则根据 parse_mode 解析：
       - 若解析产出实体 → 返回 (parsed_text, entities, None) 并按限长裁剪
       - 若解析无实体 → 返回 (裁剪后的原文, None, parse_mode 对象)，交给 Telethon 渲染
    """
    text = text or ""
    use_limit = _pick_limit(is_caption, limit)

    # 1) 显式实体路径
    if entities:
        fixed_text, fixed_ents = clip_text_entities(text, list(entities), limit=use_limit)
        return fixed_text, fixed_ents, None

    # 2) 解析路径
    pm = sanitize_parse_mode(parse_mode or AutoParseMode())
    if pm:
        parsed_text, parsed_entities = pm.parse(text)
        parsed_entities = [e for e in (parsed_entities or []) if int(getattr(e, "length", 0) or 0) > 0]
        if parsed_entities:
            fixed_text, fixed_ents = clip_text_entities(parsed_text, parsed_entities, limit=use_limit)
            return fixed_text, fixed_ents, None
        # 无实体 → 按限长裁剪原文并交给 parse_mode 渲染
        fixed_text, _ = clip_text_entities(text, None, limit=use_limit)
        return fixed_text, None, pm

    # 3) 无解析/无实体 → 朴素裁剪
    fixed_text, _ = clip_text_entities(text, None, limit=use_limit)
    return fixed_text, None, None
