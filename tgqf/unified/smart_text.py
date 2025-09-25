# unified/smart_text.py
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple
from telethon.tl.types import MessageEntityCustomEmoji
from unified.text_utils import PLACEHOLDER_PATTERN, utf16_index

_TG_EMOJI_RE = re.compile(r'<tg-emoji\s+emoji-id="(?P<id>\d+)"[^>]*>.*?</tg-emoji>', re.I | re.S)
_HTML_TAG_RE = re.compile(r"</?(b|i|u|s|strike|em|strong|a|code|pre|span|blockquote|tg-spoiler|u|tg-emoji)\b[^>]*>", re.I)
_MD_SIGNAL_RE = re.compile(r"(\*\*.+?\*\*|__.+?__|~~.+?~~|```[\s\S]+?```|`.+?`)", re.S)
_MD_QUOTE_RE = re.compile(r"^>\s?.+", re.M)
_MD_SPOILER_RE = re.compile(r"\|\|.+?\|\|", re.S)

PLACEHOLDER_DEFAULT_CHAR = "•"

def _looks_like_html(text: str) -> bool:
    return bool(_HTML_TAG_RE.search(text or ""))

def _looks_like_md(text: str) -> bool:
    t = text or ""
    return bool(_MD_SIGNAL_RE.search(t) or _MD_QUOTE_RE.search(t) or _MD_SPOILER_RE.search(t))

def extract_tgemoji_ids_from_html(text: str) -> List[int]:
    if not text:
        return []
    try:
        ids = [int(m.group("id")) for m in _TG_EMOJI_RE.finditer(text)]
        seen, out = set(), []
        for i in ids:
            if i not in seen:
                seen.add(i); out.append(i)
        return out
    except Exception:
        return []

def build_text_entities_from_template(
    text: str,
    *,
    seq_doc_ids: Optional[Iterable[int]] = None,
    name_to_doc: Optional[Dict[str, int]] = None,
    placeholder_char: str = PLACEHOLDER_DEFAULT_CHAR,
    strict: bool = False,
) -> Tuple[str, List[MessageEntityCustomEmoji], List[str]]:
    warnings: List[str] = []
    result_text = ""
    entities: List[MessageEntityCustomEmoji] = []
    seq_ids = list(seq_doc_ids or [])
    name_map = dict(name_to_doc or {})
    emoji_count = 0
    pos = 0
    for match in PLACEHOLDER_PATTERN.finditer(text or ""):
        start, end = match.span()
        result_text += text[pos:start]
        name = (match.group(1) or "").strip() if match.group(1) else ""
        doc_id: Optional[int] = None
        if name:
            doc_id = name_map.get(name)
            if not doc_id:
                msg = f"⚠️ 表情名称未找到: {name}"
                if strict: raise ValueError(msg)
                warnings.append(msg)
        else:
            if emoji_count < len(seq_ids):
                doc_id = seq_ids[emoji_count]
            else:
                msg = f"⚠️ 顺序表情 ID 用尽 (已用 {emoji_count})"
                if strict: raise ValueError(msg)
                warnings.append(msg)
        insert_text = placeholder_char
        insert_pos = len(result_text)
        result_text += insert_text
        if doc_id:
            # UTF-16 下的 offset/length
            offset = utf16_index(result_text, insert_pos)
            length = utf16_index(result_text, insert_pos + len(insert_text)) - offset
            entities.append(MessageEntityCustomEmoji(offset=offset, length=length, document_id=int(doc_id)))
        emoji_count += 1
        pos = end
    result_text += (text or "")[pos:]
    return result_text, entities, warnings

def smart_text_parse(
    text: str,
    entities: Optional[List[Any]] = None,
    *,
    template_seq_ids: Optional[Iterable[int]] = None,
    template_name_map: Optional[Dict[str, int]] = None,
    placeholder_char: str = PLACEHOLDER_DEFAULT_CHAR,
    strict_template: bool = False,
) -> Tuple[str, Optional[str], Optional[List[Any]], List[str]]:
    """返回: (text, parse_mode, entities, warnings)
    - 若已有 entities：直接返回
    - 识别模板 {{emoji}} 并构造 custom_emoji 实体
    - 否则根据内容猜测 parse_mode: html/md/None
    """
    if entities:
        return text, None, entities, []
    if PLACEHOLDER_PATTERN.search(text or ""):
        try:
            ttext, ents, warns = build_text_entities_from_template(
                text,
                seq_doc_ids=template_seq_ids,
                name_to_doc=template_name_map,
                placeholder_char=placeholder_char,
                strict=strict_template,
            )
            return ttext, None, ents, warns
        except Exception as e:
            return text, None, None, [f"模板解析失败：{e}"]
    if _looks_like_html(text):
        return text, "html", None, []
    if _looks_like_md(text):
        return text, "md", None, []
    return text, None, None, []

def extract_custom_emoji_ids(entities: Optional[List[Any]]) -> List[int]:
    if not entities:
        return []
    return [int(getattr(e, "document_id", 0)) for e in entities if isinstance(e, MessageEntityCustomEmoji)]