# ui/accounts_health.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List

from unified.logger import log_info, log_warning
from unified.context import get_scheduler
from core.defaults.bot_utils import BotUtils
from unified.accounts_service import AccountsService


async def preview_send_for_selected(event, user_id: int, selected_phones: List[str]) -> dict:
    """
    面板复用入口：体检所选 → 统一选号（SEND）→ 把结果发回到面板
    - event 可为 None：此时只返回数据给上层渲染
    返回：{"ready": [...], "stats": {...}, "excluded": {...}}
    """
    svc = AccountsService()
    ready, detail = await svc.check_selected_and_preview_send(user_id, selected_phones)
    stats = (detail or {}).get("stats", {})
    excluded = (detail or {}).get("excluded", {})

    if event:
        try:
            ok_n = len(ready or [])
            total = int((stats or {}).get("total", len(selected_phones)))
            cd_n = int((stats or {}).get("cooldown_hit", 0))
            ex_n = int((stats or {}).get("excluded", 0))
            title = "🩺 体检完成 · 可群发预览（SEND）\n"
            lines = []
            lines.append(f"• 选择账号：{total}")
            lines.append(f"• 可群发就绪：{ok_n}")
            lines.append(f"• 冷却命中：{cd_n}")
            lines.append(f"• 其它排除：{ex_n}")
            if ready:
                # 只展示前 50，避免过长
                sample = ready[:50]
                lines.append("• 可发账号（样本）：\n" + "\n".join(f"  · <code>{p}</code>" for p in sample))
                if len(ready) > 50:
                    lines.append(f"  ……共 {len(ready)} 个")
            text = title + "\n".join(lines)
            await BotUtils.safe_respond(event, event.client, text, parse_mode="html", link_preview=False)
        except Exception as e:
            log_warning("preview_send_for_selected: UI 回显失败（忽略）", extra={"user_id": user_id, "err": str(e)})

    return {"ready": ready, "stats": stats, "excluded": excluded}
