# -*- coding: utf-8 -*-
# unified/ui_kit.py
from __future__ import annotations

import time
import html
from typing import Any, Iterable, Optional, Dict

from unified.logger import log_debug, log_info
from unified.trace_context import get_log_context


def _now_ms() -> int:
    return int(time.time() * 1000)


def _elapsed_ms(ts: int) -> int:
    return max(0, _now_ms() - ts)


def _h(s: Any) -> str:
    return html.escape(str(s or ""), quote=False)


class UiKit:
    """
    极简、稳定的通知渲染工具：
      - bullet: 项目符号
      - kv: 键值行，加粗 key
      - card: 简单卡片 HTML（支持分隔线、上下文页脚）
    """

    @staticmethod
    def bullet(text: Any) -> str:
        s = _h(text)
        return f"• {s}" if s else "• -"

    @staticmethod
    def kv(key: Any, val: Any) -> str:
        return f"<b>{_h(key)}</b>：{_h(val)}"

    @staticmethod
    def _level_tag(level: str) -> str:
        table = {
            "ok": "✅",
            "start": "🚀",
            "warn": "⚠️",
            "progress": "⏳",
            "error": "🛑",
            "info": "ℹ️",
        }
        return table.get((level or "").lower(), "ℹ️")

    @staticmethod
    def card(
        title: str,
        lines: Iterable[str] | None = None,
        *,
        level: str = "info",
        with_rule: bool = False,
        ctx_footer: bool = False,
    ) -> str:
        ts = _now_ms()
        tag = UiKit._level_tag(level)
        title_html = f"{tag} {_h(title or '-')}"
        body_lines = [str(x) for x in (lines or []) if str(x or "").strip()]
        body = "<br/>".join(body_lines) if body_lines else ""
        parts = [f"<b>{title_html}</b>"]
        if body:
            parts.append(body)
        if with_rule:
            parts.append("───────────────")
        if ctx_footer:
            ctx: Dict[str, Any] = get_log_context() or {}
            ctx_kv = []
            if "user_id" in ctx:
                ctx_kv.append(f"UID:{_h(ctx.get('user_id'))}")
            if "trace_id" in ctx:
                ctx_kv.append(f"trace:{_h(ctx.get('trace_id'))}")
            if ctx_kv:
                parts.append(f"<i>{' · '.join(ctx_kv)}</i>")
        out = "\n".join(parts)

        # 结构化日志（便于定位具体 UI 输出）
        log_info(
            "uikit_card_rendered",
            extra={
                "level": level,
                "title_len": len(title or ""),
                "lines": len(body_lines),
                "elapsed_ms": _elapsed_ms(ts),
            },
        )
        return out
