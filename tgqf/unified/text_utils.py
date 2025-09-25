# -*- coding: utf-8 -*-
# unified/text_utils.py
from __future__ import annotations
import re
from typing import Any, List, Optional, Sequence, Tuple

try:
    from telethon.tl import types as t
except Exception:
    t = None  # 允许无 Telethon 环境做静态检查

__all__ = [
    "PLACEHOLDER_PATTERN",
    "utf16_index",
    "utf16_len",
    "py_index_from_utf16",
    "inject_emoji_fallback_inline",
    "is_custom_emoji_entity",
    "replace_emoji_placeholders_visible",
]

# 兼容两种写法：
# 1) {{emoji}} / {{emoji:name}}（旧）
# 2) {{name}}（新）
# 捕获组1为名称（可能为空）
PLACEHOLDER_PATTERN = re.compile(
    r"\{\{\s*(?:emoji\s*:\s*)?([^\s{}]*)\s*\}\}",
    re.I,
)

def replace_emoji_placeholders_visible(
    text: str,
    *,
    name_to_char: Optional[dict] = None,
    default_char: str = "•",
) -> Tuple[str, List[str]]:
    """把 {{emoji}} / {{emoji:name}} / {{name}} 替换为**可见字符**（不会创建实体）。
    - 无名称/不可解析 → 用 default_char 填充。
    - name_to_char 需提供“名字→单字符 emoji”的映射。
    """
    warnings: List[str] = []
    name_to_char = name_to_char or {}

    def _repl(m: re.Match) -> str:
        raw = (m.group(1) or "").strip()
        if not raw:
            return default_char
        ch = name_to_char.get(raw)
        if isinstance(ch, str) and len(ch) == 1:
            return ch
        warnings.append(f"未识别的 emoji 名称：{raw}，已使用默认字符。")
        return default_char

    return PLACEHOLDER_PATTERN.sub(_repl, text or ""), warnings

def utf16_index(text: str, pos: int) -> int:
    """返回 text[:pos] 的 UTF-16 code unit 数量（不含 BOM）。"""
    if not isinstance(text, str):
        text = str(text or "")
    n = len(text)
    if pos < 0:
        pos = 0
    elif pos > n:
        pos = n
    return len(text[:pos].encode("utf-16-le")) // 2

def utf16_len(text: str) -> int:
    if not isinstance(text, str):
        text = str(text or "")
    return len(text.encode("utf-16-le")) // 2

def py_index_from_utf16(text: str, off16: int) -> int:
    """UTF-16 码元数 → Python 索引（二分）。"""
    if off16 <= 0:
        return 0
    py2u16 = [0] * (len(text) + 1)
    acc = 0
    for i, ch in enumerate(text, 1):
        acc += 2 if ord(ch) > 0xFFFF else 1
        py2u16[i] = acc
    if off16 >= py2u16[-1]:
        return len(text)
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi) // 2
        if py2u16[mid] < off16:
            lo = mid + 1
        else:
            hi = mid
    return lo

def inject_emoji_fallback_inline(
    text: str,
    entities: Optional[Sequence[Any]],
    emoji_id_to_char: dict[int, str],
    *,
    prefer_html_tag: bool = False,
) -> str:
    """把 MessageEntityCustomEmoji 在原位置用可见字符（或 <tg-emoji>）内联替换。"""
    if not text or not entities:
        return text

    targets: List[Tuple[int, int, str]] = []
    for e in entities:
        if not is_custom_emoji_entity(e):
            continue
        try:
            eid = int(getattr(e, "document_id", 0) or 0)
            off16 = int(getattr(e, "offset", 0) or 0)
            len16 = int(getattr(e, "length", 0) or 0)
        except Exception:
            continue
        ch = emoji_id_to_char.get(eid, "")
        if not ch:
            continue
        repl = f'<tg-emoji emoji-id="{eid}">{ch}</tg-emoji>' if prefer_html_tag else ch
        targets.append((off16, len16, repl))
    if not targets:
        return text

    # 倒序替换，避免后移影响前面的 offset
    targets.sort(key=lambda x: (x[0], x[1]), reverse=True)
    buf = text
    for off16, len16, repl in targets:
        start_py = py_index_from_utf16(buf, off16)
        end_py = py_index_from_utf16(buf, off16 + len16)
        buf = buf[:start_py] + repl + buf[end_py:]
    return buf

def is_custom_emoji_entity(ent: Any) -> bool:
    try:
        return t is not None and isinstance(ent, t.MessageEntityCustomEmoji)
    except Exception:
        return getattr(ent, "CLASS_NAME", "") == "MessageEntityCustomEmoji"
