# -*- coding: utf-8 -*-
# core/runtime_events.py
from __future__ import annotations

import asyncio
from typing import Any, Tuple

from telethon import events, functions
from telethon.errors import RPCError
from telethon.tl import types as t

from unified.context import get_client_manager
from core.event_bus import bus
from core.task_status_reporter import TaskStatusReporter
from unified.logger import log_info, log_warning, log_exception
from tg.utils_username import display_name_from_me, username_from_me


async def _make_reporter(scheduler) -> TaskStatusReporter:
    """
    监听器环境下没有调度上下文，这里用 scheduler.fsm.redis 构造 Reporter 以便发通知 & 登记。
    """
    try:
        redis = getattr(getattr(scheduler, "fsm", None), "redis", None)
    except Exception:
        redis = None
    r = TaskStatusReporter(redis_client=redis)
    return r


def attach_runtime_event_listeners(client, user_id: int, scheduler=None) -> None:
    """
    对每个已注册 TelegramClient 挂载监听：
      - ChatAction: 自己入群成功/被踢出/离开
      - Raw: 频道/群权限变化（写禁言等）
    并通过 EventBus 统一派发结构化事件，通过 Reporter 发送 UI 卡片与模板通知。
    """
    me_id = getattr(client, "self_id", None)
    phone = getattr(client, "phone", "-")

    async def _me_info() -> Tuple[str, str]:
        try:
            me = await client.get_me()
            return display_name_from_me(me), (username_from_me(me, with_at=True) or "-")
        except Exception:
            return "-", "-"

    async def _send_template(uid: int, text: str) -> None:
        try:
            rep = await _make_reporter(scheduler)
            rep.set_user(uid)
            await rep._send(text, to_user_id=uid, parse_mode="html")
        except Exception as e:
            log_warning("send_template_failed", extra={"user_id": uid, "err": str(e)})

    async def _ensure_me_id():
        nonlocal me_id
        if not me_id:
            try:
                me = await client.get_me()
                me_id = int(getattr(me, "id", 0) or 0) or None
                setattr(client, "self_id", me_id)  # 缓存
            except Exception:
                me_id = None

    # 启动时异步确认 self_id，避免前几条事件无法过滤“自己”
    asyncio.create_task(_ensure_me_id())

    # ---- ChatAction：入群成功 / 被踢出 / 离开 ----
    @client.on(events.ChatAction)
    async def _on_action(e: events.ChatAction.Event):  # type: ignore
        try:
            # 只处理“自己”的事件
            if not me_id:
                await _ensure_me_id()
            if me_id is None:
                return
            if getattr(e, "user_id", None) != me_id:
                return

            chat = await e.get_chat()
            title = getattr(chat, "title", "") or getattr(chat, "username", "") or str(getattr(chat, "id", "-"))
            username = getattr(chat, "username", None)
            link = f"https://t.me/{username}" if username else "-"
            payload = {
                "type": "JOIN_APPROVED" if (e.user_joined or e.user_added) else ("KICKED" if (e.user_kicked or e.user_left) else "UNKNOWN"),
                "user_id": int(user_id),
                "phone": phone,
                "chat_id": getattr(chat, "id", None),
                "chat_title": title,
                "chat_link": link,
            }

            if e.user_joined or e.user_added:
                # 事件派发
                await bus.dispatch("JOIN_APPROVED", payload)

                # UI 通知 & 取消“待审批”
                rep = await _make_reporter(scheduler)
                rep.set_user(user_id)
                await rep.mark_approval_done(user_id, link or str(getattr(chat, "id", "")))
                await rep.notify_join_approved(user_id, title, link)

                # 如任务被暂停，尝试恢复（交由 HealthChecker/Scheduler 的既有逻辑拉起）
                try:
                    if scheduler and hasattr(scheduler, "start_user_task"):
                        asyncio.create_task(scheduler.start_user_task(user_id))
                except Exception:
                    pass

                log_info("🎉 join approved", extra={"user_id": user_id, "chat": title, "phone": phone})

                # 回群成功通知模板
                try:
                    name, uname = await _me_info()
                    uname_disp = (uname or "-").lstrip("@")
                    safe_title = title or "-"
                    safe_link = link or "-"
                    text = f"⭐️ 顶替进群 →  📱{phone} 👤{name}  @{uname_disp} → 💃🏼{safe_title} {safe_link}"
                    await _send_template(user_id, text)
                except Exception:
                    pass

            elif e.user_kicked or e.user_left:
                # 事件派发
                await bus.dispatch("KICKED", payload)

                rep = await _make_reporter(scheduler)
                rep.set_user(user_id)
                await rep.notify_kicked(user_id, title, link)

                # 尝试快速 rejoin：优先用户名高阶 API（使用函数请求）
                ok = False
                if username:
                    try:
                        entity = await client.get_input_entity(username)
                        await client(functions.channels.JoinChannelRequest(channel=entity))
                        ok = True
                    except RPCError as ex:
                        log_warning("rejoin by username failed", extra={"chat": username, "err": str(ex)})
                    except Exception as ex:
                        log_warning("rejoin username resolve failed", extra={"chat": username, "err": str(ex)})
 
 
                if not ok and link and link != "-" and ("/+" in link or "joinchat" in link):
                    # 处理 t.me/+abcdef 或 t.me/joinchat/abcdef
                    try:
                        invite_hash = link.rsplit("/", 1)[-1].replace("+", "")
                        await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
                        ok = True
                    except RPCError as ex:
                        log_warning("rejoin by invite failed", extra={"link": link, "err": str(ex)})
                    except Exception as ex:
                        log_warning("rejoin invite parse failed", extra={"link": link, "err": str(ex)})

                if not ok:
                    # 用 chat 实体兜底（有时能直接加入可见的公开频道）
                    try:
                        entity = await client.get_input_entity(chat)
                        await client(functions.channels.JoinChannelRequest(channel=entity))
                        ok = True
                    except Exception:
                        pass

                if ok:
                    await rep.notify_rejoin_result(user_id, title, link, success=True)
                    # 继续发送：让调度器按照现有任务状态推进
                    try:
                        if scheduler and hasattr(scheduler, "start_user_task"):
                            asyncio.create_task(scheduler.start_user_task(user_id))
                    except Exception:
                        pass
                else:
                    # 无法回群：登记“待回群”并标记“待分配”
                    ref = link or str(getattr(chat, "id", ""))
                    await rep.mark_rejoin_pending(user_id, ref)
                    await rep.mark_reassign_pending(user_id, ref)

                # 被踢出群组通知模板（兜底）
                try:
                    name, uname = await _me_info()
                    uname_disp = (uname or "-").lstrip("@")
                    safe_title = title or "-"
                    safe_link = link or "-"
                    text = f"👿 被踢群 → 📱{phone} 👤{name}  @{uname_disp} → 💃🏼{safe_title} {safe_link}"
                    await _send_template(user_id, text)
                except Exception:
                    pass

            # 其它事件类型忽略
        except Exception as ex:
            log_exception("runtime ChatAction handler error", exc=ex)

    # ---- Raw：权限变化（写禁言/限制） ----
    @client.on(events.Raw)
    async def _on_raw(update: Any):  # type: ignore
        try:
            if isinstance(update, t.UpdateChannelParticipant):
                part = update.new_participant or update.prev_participant
                chat_id = getattr(update, "channel_id", None)

                # 只有“自己”的权限变化才关心
                is_self = False
                if isinstance(part, t.ChannelParticipantSelf):
                    is_self = True
                elif isinstance(part, t.ChannelParticipant):
                    is_self = (getattr(part, "user_id", None) == me_id)
                elif isinstance(part, t.ChannelParticipantBanned):
                    is_self = (getattr(part, "peer", None)
                               and getattr(getattr(part, "peer", None), "user_id", None) == me_id)
                elif isinstance(part, t.ChannelParticipantRestricted):
                    is_self = (getattr(part, "peer", None)
                               and getattr(getattr(part, "peer", None), "user_id", None) == me_id)
                if not is_self:
                    return

                # 检测禁言（永久/临时）
                write_blocked = (
                    isinstance(part, t.ChannelParticipantBanned)
                    and getattr(part, "banned_rights", None)
                    and getattr(part.banned_rights, "send_messages", False)
                ) or (
                    isinstance(part, t.ChannelParticipantRestricted)
                    and getattr(part, "banned_rights", None)
                    and getattr(part.banned_rights, "send_messages", False)
                )

                if write_blocked:
                    payload = {
                        "type": "WRITE_FORBIDDEN",
                        "user_id": int(user_id),
                        "phone": phone,
                        "chat_id": chat_id,
                    }
                    await bus.dispatch("WRITE_FORBIDDEN", payload)

                    rep = await _make_reporter(scheduler)
                    rep.set_user(user_id)

                    # 取标题（best-effort）
                    try:
                        chat = await client.get_entity(chat_id)
                        title = (
                            getattr(chat, "title", "")
                            or getattr(chat, "username", "")
                            or str(chat_id)
                        )
                        link = f"https://t.me/{getattr(chat, 'username')}" if getattr(chat, "username", None) else "-"
                    except Exception:
                        title, link = str(chat_id), "-"

                    await rep.notify_write_forbidden(user_id, title, link)

                    # 被禁言通知模板（兜底）
                    try:
                        name, uname = await _me_info()
                        uname_disp = (uname or "-").lstrip("@")
                        safe_title = title or "-"
                        safe_link = link or "-"
                        text = f"👿 禁言 → 📱{phone} 👤{name}  @{uname_disp} → 💃🏼{safe_title} {safe_link}"
                        await _send_template(user_id, text)
                    except Exception:
                        pass
        except Exception as ex:
            log_exception("runtime Raw handler error", exc=ex)

    # ---- 订阅总线事件：FLOOD_WAIT / BANNED / RECOVERED → 模板通知 ----
    async def _on_bus(evt: dict) -> None:
        try:
            if not isinstance(evt, dict):
                return
            et = (evt.get("type") or "").upper()
            phone_evt = str(evt.get("phone") or phone or "-")
            user_ids = list(evt.get("user_ids") or ([user_id] if user_id else []))

            # 为了拿到 name / username，尽量取对应 user 的 client
            async def _name_uname_for(uid: int) -> Tuple[str, str]:
                try:
                    cm = get_client_manager()
                    c = cm.get_client(uid, phone_evt)
                    if c:
                        me = await c.get_me()
                        return display_name_from_me(me), (username_from_me(me, with_at=True) or "-")
                except Exception:
                    pass
                return "-", "-"

            if et == "FLOOD_WAIT":
                for uid in user_ids:
                    name_u, uname_u = await _name_uname_for(uid)
                    uname_disp = (uname_u or "-").lstrip("@")
                    text = f"👿 限流 → 📱{phone_evt} 👤{name_u}  @{uname_disp} → 💃🏼- -"
                    await _send_template(int(uid), text)
            elif et == "BANNED":
                for uid in user_ids:
                    name_u, uname_u = await _name_uname_for(uid)
                    uname_disp = (uname_u or "-").lstrip("@")
                    text = f"☠️ 封号 → 📱{phone_evt} 👤{name_u}  @{uname_disp} → 💃🏼- -"
                    await _send_template(int(uid), text)
            elif et == "RECOVERED":
                for uid in user_ids:
                    name_u, uname_u = await _name_uname_for(uid)
                    uname_disp = (uname_u or "-").lstrip("@")
                    text = f"⭐️ 恢复 → 📱{phone_evt} 👤{name_u}  @{uname_disp} → 💃🏼- -"
                    await _send_template(int(uid), text)
        except Exception as e:
            log_warning("bus_template_handler_failed", extra={"err": str(e)})

    # —— 关键：同步注册，立即生效，避免启动竞态丢事件 —— #
    try:
        bus.register_many(["FLOOD_WAIT", "BANNED", "RECOVERED"], _on_bus)
    except Exception:
        pass
