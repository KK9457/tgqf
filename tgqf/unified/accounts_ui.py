# unified/accounts_ui.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from html import escape
from typing import Callable, Dict, List, Optional, Set, Tuple
from core.defaults.bot_utils import BotUtils
from telethon import Button, types
from unified.callback_proto import ensure_cb_len
from unified.logger import log_info, log_exception, log_debug
from unified.trace_context import inject_trace_context, get_log_context
from typess.health_types import HealthInfo, HealthState
from ui.constants import ACCOUNTS_OPEN, TASK_SET_GROUPS, TASK_SET_MESSAGE, TASK_SET_INTERVAL, TASK_START, TASK_STOP, TASKMENU_OPEN

def html_code(s: str) -> str:
    return f"<code>{escape(s or '-')}</code>"

def _inline(text: str, data: bytes | None, *, label: str = "cb") -> "types.KeyboardButtonCallback":
    try:
        safe = ensure_cb_len(data or b"", label=label)
        return Button.inline(text, data=safe)
    except ValueError as e:
        log_exception("构造 Inline 按钮失败（data 超限或非法）", exc=e, extra={"text": text, "label": label, "data_len": len(data or b'')})
        return Button.inline(f"⚠️ {text[:10]}(失效)", data=b"_:noop")

def count_states(phones: List[str], health_info: Dict[str, HealthInfo]) -> Dict[str, int]:
    total = len(phones or [])
    counts = {
        "total": total, "ok": 0, "limited": 0, "auth_expired": 0,
        "flood_wait": 0, "network_error": 0, "banned": 0, "unknown": 0,
    }

    def _extract_state(hi) -> HealthState:
        try:
            if isinstance(hi, HealthState):
                return hi
            if isinstance(hi, dict):
                v = hi.get("state", None)
            else:
                v = getattr(hi, "state", None)
            return HealthState.parse(v)
        except Exception:
            return HealthState.UNKNOWN

    map_key = {
        HealthState.OK: "ok",
        HealthState.LIMITED: "limited",
        HealthState.AUTH_EXPIRED: "auth_expired",
        HealthState.FLOOD_WAIT: "flood_wait",
        HealthState.NETWORK_ERROR: "network_error",
        HealthState.BANNED: "banned",
        HealthState.UNKNOWN: "unknown",
    }

    for p in phones or []:
        hi = (health_info or {}).get(p)
        st = _extract_state(hi)
        counts[map_key.get(st, "unknown")] += 1

    counts["abnormal"] = (
        counts["limited"] + counts["auth_expired"] + counts["flood_wait"] +
        counts["network_error"] + counts["banned"]
    )
    return counts

def build_accounts_text(
    *,
    title: str,
    phones: List[str],
    names: Dict[str, str],
    usernames: Dict[str, str],  # ← 新增
    health_info: Dict[str, HealthInfo],
    page: int,
    total_pages: int,
    selected: Optional[Set[str]] = None,
    page_size: int = 10,
    show_checkbox: bool = False,
    show_online_hint: bool = False,
) -> str:
    selected = set(selected or set())
    total_pages = max(1, int(total_pages or 1))
    page = max(1, min(int(page or 1), total_pages))

    stat = count_states(phones, health_info)
    header = [
        f"<b>{escape(title)}</b>",
        f"• 账号数：<b>{stat['total']}</b> | 健康：<b>{stat['ok']}</b> | 异常：<b>{stat['abnormal']}</b> | 未知：<b>{stat['unknown']}</b>",
    ]

    if not phones:
        body = ["<i>暂无账号</i>"]
    else:
        idx_start = max(0, (page - 1) * page_size)
        idx_end = min(idx_start + page_size, len(phones))
        rows: List[str] = []

        def _get(o, key: str, default):
            try:
                if isinstance(o, dict):
                    v = o.get(key, default)
                else:
                    v = getattr(o, key, default)
                return v if v is not None else default
            except Exception:
                return default

        for phone in phones[idx_start:idx_end]:
            name = names.get(phone, "-")
            uname = usernames.get(phone, "-")
            hi = (health_info or {}).get(phone)
            emoji = _get(hi, "emoji", "❔")
            online_str = str(_get(hi, "online", "未知")).strip() or "未知"

            mark = "☑ " if (show_checkbox and phone in selected) else ("☐ " if show_checkbox else "")
            tail = f" · {escape(online_str)}" if (show_online_hint and online_str in {'在线','离线'}) else ""
            rows.append(
                f"{mark}{emoji} 账号：{html_code(phone)} 名字：{html_code(name)} 用户名：{html_code(uname)}{tail}"
            )
        body = rows

    footer = f"<i>第 {page}/{total_pages} 页</i>"
    inject_trace_context("build_accounts_text", phase="uikit", signal="build", module=__name__)
    log_debug("🧱 账号文本构造完成", extra={"page": page, "total_pages": total_pages, "page_size": page_size, "phones_total": len(phones or []), "selected_count": len(selected)})
    return "\n".join(header) + "\n\n" + "\n".join(body) + "\n\n" + footer



def build_accounts_manage_buttons(
    *,
    owner_uid: int,
    phones: List[str],
    page: int,
    total_pages: int,
    selected: Set[str],
    page_size: int,
    pack_fn: Callable[..., bytes],
    upload_open: bytes,
) -> List[List[Button]]:
    rows: List[List[Button]] = [[Button.inline("📥 上传账号", data=upload_open)]]

    total_pages = max(1, int(total_pages or 1))
    page = max(1, min(int(page or 1), total_pages))
    idx_start = max(0, (page - 1) * page_size)
    idx_end = min(idx_start + page_size, len(phones))
    phones_page = phones[idx_start:idx_end]

    for phone in phones_page:
        mark = "☑" if phone in selected else "☐"
        try:
            rows.append([
                _inline(f"{mark} {phone}", pack_fn("toggle", owner_uid, phone, page), label="acct.toggle"),
            ])
        except Exception as e:
            log_exception("构造账号切换按钮失败", exc=e, extra={"owner_uid": owner_uid, "page": page, "phone": phone})

    nav = []
    if page > 1:
        nav.append(_inline("⬅️ 上一页", pack_fn("page", owner_uid, page - 1), label="acct.page"))
    if page < total_pages:
        nav.append(_inline("➡️ 下一页", pack_fn("page", owner_uid, page + 1), label="acct.page"))
    if nav:
        rows.append(nav)

    if phones_page:
        all_selected = all(p in selected for p in phones_page)
        toggle_label = "🔁 反选当前页" if all_selected else "☑ 全选当前页"
        toggle_action = "sel_invert" if all_selected else "sel_all"
        rows.append([_inline(toggle_label, pack_fn(toggle_action, owner_uid, page), label="acct.toggle_page")])

    rows.append([
        _inline(f"🗑 删除所选({len(selected)})", pack_fn("confirm_del", owner_uid, page), label="acct.confirm_del"),
        _inline("🧹 清空全部", pack_fn("confirm_clear", owner_uid, page), label="acct.confirm_clear"),
    ])

    rows.append([
        _inline("🔄 刷新", pack_fn("refresh", owner_uid, page), label="acct.refresh"),
        _inline(f"🩺 体检所选({len(selected)})", pack_fn("health_bulk", owner_uid, page), label="acct.health_bulk"),
        Button.inline("📤 任务菜单", data=TASKMENU_OPEN),
    ])

    inject_trace_context("build_accounts_manage_buttons", phase="uikit", signal="build", module=__name__)
    log_debug("🧩 账号管理按钮构造完成", extra={
        "owner_uid": owner_uid, "page": page, "total_pages": total_pages,
        "page_size": page_size, "phones_on_page": len(phones_page), "selected_count": len(selected or set())
    })
    return rows


def build_accounts_page(
    *,
    owner_uid: int,
    phones: List[str],
    names: Dict[str, str],
    usernames: Dict[str, str],     # ← 新增
    health_info: Dict[str, HealthInfo],
    page: int,
    total_pages: int,
    selected: Set[str],
    page_size: int,
    pack_fn: Callable[..., bytes],
    upload_open: bytes,
    show_online_hint: bool = True,
) -> Tuple[str, List[List[Button]]]:
    text = build_accounts_text(
        title="👥 账号管理",
        phones=phones,
        names=names,
        usernames=usernames,        # ← 新增
        health_info=health_info,
        page=page,
        total_pages=total_pages,
        selected=selected,
        page_size=page_size,
        show_checkbox=True,
        show_online_hint=show_online_hint,
    )
    buttons = build_accounts_manage_buttons(
        owner_uid=owner_uid,
        phones=phones,
        page=page,
        total_pages=total_pages,
        selected=selected,
        page_size=page_size,
        pack_fn=pack_fn,
        upload_open=upload_open,
    )
    inject_trace_context("build_accounts_page", phase="uikit", signal="build", module=__name__, user_id=owner_uid)
    log_info("📄 账号管理页面已构造", extra={"owner_uid": owner_uid, "page": page, "total_pages": total_pages, "page_size": page_size, "buttons_rows": len(buttons)})
    return text, buttons


def accounts_entry_button() -> Button:
    inject_trace_context("accounts_entry_button", phase="uikit", signal="build", module=__name__)
    log_debug("🔘 构造入口按钮：账号管理", extra={"constant": "ACCOUNTS_OPEN"})
    return Button.inline("👥 账号管理", data=ACCOUNTS_OPEN)


def build_task_menu(accounts_open: bytes) -> Tuple[str, List[List[Button]]]:
    text = (
        "📤 <b>任务配置菜单</b>\n\n"
        "首次启动任务前请依次完成：\n\n"
        "1️⃣ 设置 <b>设置目标群组</b>\n"
        "2️⃣ 设置 <b>设置群发内容</b>\n"
        "3️⃣ 设置 <b>设置间隔时间</b>\n"
    )
    from handlers.commands.group_command import GROUPS_ENTRY
    buttons = [
        [Button.inline("🎯 设置目标群组", data=GROUPS_ENTRY)],

        [Button.inline("🧾 设置群发内容", data=TASK_SET_MESSAGE)],
        [Button.inline("⏱ 设置间隔时间", data=TASK_SET_INTERVAL)],
        [Button.inline("🚀 启动任务",   data=TASK_START)],
        [Button.inline("🛑 停止任务",   data=TASK_STOP)],
        [Button.inline("👥 账号管理", data=accounts_open)],
    ]
    inject_trace_context("build_task_menu", phase="uikit", signal="build", module=__name__)
    log_info("🧭 群发配置菜单构造完成", extra={
        "include_accounts_entry": (accounts_open == ACCOUNTS_OPEN),
        "buttons_rows": len(buttons)
    })
    return text, buttons

async def send_task_menu(event, accounts_open: bytes) -> None:
    try:
        if hasattr(event, "answer"):
            await event.answer()
    except Exception:
        pass

    text, buttons = build_task_menu(accounts_open)
    try:
        markup = event.client.build_reply_markup(buttons)
    except Exception:
        markup = None

    try:
        await event.edit(text, buttons=markup, link_preview=False)
        return
    except Exception:
        pass

    inject_trace_context("send_task_menu", user_id=getattr(event, "sender_id", None), phase="uikit", signal="respond", module=__name__)
    log_info("📤 群发配置菜单已发送", extra={"user_id": getattr(event, "sender_id", None)})
    await BotUtils.safe_respond(event, event.client, text, buttons=markup,parse_mode="html",  link_preview=False)

async def on_taskmenu_open_cb(event) -> None:
    try:
        if hasattr(event, "answer"):
            await event.answer(cache_time=0)
    except Exception:
        pass

    user_id = getattr(event, "sender_id", None)
    inject_trace_context("on_taskmenu_open_cb", user_id=user_id, phase="uikit", signal="callback", module=__name__)
    log_info("📤 群发配置菜单回调触发", extra=get_log_context())

    try:
        await send_task_menu(event, ACCOUNTS_OPEN)
    except Exception as e:
        log_exception("❌ 打开群发配置菜单失败", exc=e, extra=get_log_context())
        try:
            await BotUtils.safe_respond(event, event.client, "⚠️ 打开群发配置菜单失败，请稍后重试。",parse_mode="html",  link_preview=False)
        except Exception:
            pass
