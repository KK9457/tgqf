# unified/white_panel.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from telethon import Button
from typess.fsm_keys import USERNAME
from unified.config import get_admin_ids, PAGE_SIZE
from unified.context import get_fsm
from ui.constants import NS_WHITE

def _pack_white(action: str, *parts: int | str) -> bytes:
    # white:<action>:<arg1>:<arg2>...
    tail = b":".join(str(p).encode() for p in parts if p is not None)
    return b"%s%s:%s" % (NS_WHITE, action.encode(), tail if tail else b"")

def _unpack_white(data: bytes) -> tuple[str, list[str]]:
    s = (data or b"").decode(errors="ignore")
    if not s.startswith("white:"):
        return "", []
    parts = s.split(":")
    if len(parts) < 2:
        return "", []
    return parts[1], parts[2:]  # action, args

async def list_white(event, page: int = 1, *, edit=False):
    fsm = get_fsm()
    ids = await fsm.get_whitelist_all()
    total = len(ids)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, pages))

    start, end = (page - 1) * PAGE_SIZE, page * PAGE_SIZE
    sliced = ids[start:end]

    lines = [f"⭐️ 当前白名单（共 <b>{total}</b> 人，第 <b>{page}/{pages}</b> 页）：\n"]
    for uid in sliced:
        uname = await fsm.get_data(uid, USERNAME)
        display = uname or "(未记录)"
        lines.append(f"🔹 <code>{uid}</code> {display}")

    buttons = [[Button.inline(f"❌ 移除 {uid}", data=_pack_white("remove", uid, page))] for uid in sliced]

    nav = []
    if page > 1:
        nav.append(Button.inline("⬅️ 上一页", data=_pack_white("see", page - 1)))
    if page < pages:
        nav.append(Button.inline("➡️ 下一页", data=_pack_white("see", page + 1)))
    if nav:
        buttons.append(nav)

    msg_text = "\n".join(lines)
    if edit:
        await event.edit(msg_text, parse_mode="html", buttons=buttons or None)
    else:
        await event.respond(msg_text, parse_mode="html", buttons=buttons or None)

async def handle_white_callback(event):
    fsm = get_fsm()
    if not fsm:
        await event.answer("⚠️ 系统未初始化", alert=True)
        return

    sender_id = event.sender_id
    admins = set(get_admin_ids() or [])
    whitelist = set(await fsm.get_whitelist_all())

    # 权限：允许管理员或白名单用户操作
    if sender_id not in admins and sender_id not in whitelist:
        await event.answer("⛔ 权限不足", alert=True)
        return

    action, args = _unpack_white(event.data or b"")
    if not action:
        return

    if action == "see":
        page = int(args[0]) if args else 1
        await list_white(event, page=page, edit=True)
        await event.answer("✅ 页面已更新", alert=False)
        return

    if action == "remove":
        if len(args) < 2:
            await event.answer("参数缺失", alert=True)
            return
        uid = int(args[0]); page = int(args[1])
        success = await fsm.remove_from_whitelist(0, uid)
        await event.answer("✅ 已移除" if success else "⚠️ 不在白名单", alert=True)

        # 若白名单已变更 → 检查是否需要跳页
        ids = await fsm.get_whitelist_all()
        if (page - 1) * PAGE_SIZE >= len(ids):
            page = max(1, page - 1)
        await list_white(event, page=page, edit=True)
        return
