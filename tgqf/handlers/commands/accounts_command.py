# handlers/commands/accounts_command.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Optional, Set, Tuple, Dict
import math
import hashlib
import asyncio
import inspect

from telethon import errors
from telethon.errors.rpcerrorlist import MessageNotModifiedError
from redis.asyncio import Redis

from unified.config import FLOOD_AUTOSLEEP_THRESHOLD
from core.decorators import super_command, command_safe
from core.defaults.bot_utils import BotUtils
from router.command_router import main_router
from core.telethon_errors import classify_telethon_error
from typess.health_types import HealthState
from core.event_bus import bus
from unified.context import get_client_manager, get_redis, get_white_user
from unified.logger import log_info, log_warning, log_debug, log_exception
from unified.trace_context import set_log_context, get_log_context, generate_trace_id
from unified.accounts_service import AccountsService
from unified.accounts_ui import build_accounts_page, on_taskmenu_open_cb, send_task_menu
from unified.callback_proto import pack as cb_pack, unpack as cb_unpack, ensure_cb_len
from ui.constants import (
    ACCOUNTS_OPEN, PATTERN_ACCT, UPLOAD_OPEN,
    TASKMENU_OPEN, TASK_SET_GROUPS, TASK_STOP, TASK_START, TASK_SET_INTERVAL, TASK_SET_MESSAGE, PATTERN_TASK
)

# ---------------- Redis Keys & TTL ----------------
K_PAGE     = "ui:acct:{uid}:page"
K_SELECTED = "ui:acct:{uid}:selected"  # redis set(String phone)
EXPIRE_S   = 7200  # 2h


# ========= 权限兜底（管理员/白名单放行） =========
async def _is_admin(uid: int) -> bool:
    try:
        from unified.config import get_admin_ids
        admin_ids = get_admin_ids() or set()
        return int(uid) in {int(x) for x in admin_ids}
    except Exception:
        return False


async def _is_white(uid: int) -> bool:
    """
    兼容两种白名单接口：
      1) obj.is_white(user_id) -> bool
      2) obj.is_in_whitelist(owner_id, user_id) -> bool
    优先 owner_id=0；失败再回退到任一管理员ID。
    """
    try:
        wu = get_white_user()
        if not wu:
            return False

        if hasattr(wu, "is_white"):
            try:
                return bool(await wu.is_white(int(uid)))
            except Exception:
                pass

        if hasattr(wu, "is_in_whitelist"):
            try:
                ok0 = await wu.is_in_whitelist(0, int(uid))
                if ok0:
                    return True
            except Exception:
                pass

            try:
                from unified.config import get_admin_ids
                admin_ids = get_admin_ids() or set()
                for owner in admin_ids:
                    try:
                        if await wu.is_in_whitelist(int(owner), int(uid)):
                            return True
                    except Exception:
                        continue
            except Exception:
                pass

        return False
    except Exception:
        return False


async def _allow(event) -> bool:
    """
    权限判断：
    - 管理员始终允许
    - 其余用户需在白名单中
    """
    uid = getattr(event, "sender_id", None)
    try:
        uid = int(uid)
    except Exception:
        return False

    try:
        if await _is_admin(uid):
            return True
        if await _is_white(uid):
            return True
    except Exception as e:
        log_exception("❌ _allow: 权限检查失败", exc=e, extra={"uid": uid})
        return False
    return False


class AccountsCommand:
    def __init__(self, redis: Optional[Redis] = None, page_size: int = 10) -> None:
        if redis is None:
            try:
                redis = get_redis()
            except Exception:
                redis = None
        self.redis = redis
        self.page_size = max(1, int(page_size or 10))
        self.svc = AccountsService()
        self._mem_selected: dict[int, set[str]] = {}
        self._mem_page: dict[int, int] = {}
        self._sid_mem: dict[int, dict[str, str]] = {}  # {uid: {sid: phone}}

    # ---- 兼容 client_manager 健康状态接口 (get/update) ----
    @staticmethod
    def _cm_update_status(cm, user_id: int, phone: str, state) -> None:
        """
        优先走 update_status(phone, state)，不兼容则回退 update_status(user_id, phone, state)
        """
        try:
            return cm.update_status(phone, state)
        except TypeError:
            try:
                return cm.update_status(user_id, phone, state)
            except Exception:
                pass

    @staticmethod
    def _cm_get_health_state(cm, user_id: int, phone: str):
        """
        优先走 get_health_state(phone)，不兼容则回退 get_health_state(user_id, phone)
        """
        try:
            return cm.get_health_state(phone)
        except TypeError:
            try:
                return cm.get_health_state(user_id, phone)
            except Exception:
                return None

    # ---------------- SID <-> phone 映射 ----------------
    def _make_sid(self, phone: str) -> str:
        import hashlib as _hashlib
        return _hashlib.sha1(phone.encode("utf-8")).hexdigest()[:8]

    async def _sid_set(self, uid: int, phone: str) -> str:
        sid = self._make_sid(phone)
        if not self.redis:
            self._sid_mem.setdefault(uid, {})[sid] = phone
            return sid
        key = f"ui:acct:{uid}:sid:{sid}"
        try:
            await self.redis.set(key, phone, ex=EXPIRE_S)
        except Exception:
            self._sid_mem.setdefault(uid, {})[sid] = phone
        return sid

    async def _sid_get(self, uid: int, sid: str) -> Optional[str]:
        if not sid:
            return None
        if self.redis:
            key = f"ui:acct:{uid}:sid:{sid}"
            try:
                v = await self.redis.get(key)
                if v:
                    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
            except Exception:
                pass
        return (self._sid_mem.get(uid, {}) or {}).get(sid)

    # ---------------- 基础工具 ----------------
    async def _ack(self, event) -> None:
        try:
            if hasattr(event, "answer"):
                await event.answer(cache_time=0)
        except Exception:
            pass

    async def _send_or_edit(self, event, text, buttons=None, *, link_preview: bool = False):
        """优先编辑，失败则发送新消息；统一带上 buttons/link_preview。"""
        try:
            await event.edit(text, buttons=buttons, link_preview=link_preview)
        except MessageNotModifiedError:
            log_debug("refresh: MessageNotModified → skip fallback", extra=get_log_context())
            return
        except Exception:
            await BotUtils.safe_respond(event, event.client, text, buttons=buttons, parse_mode="html", link_preview=link_preview)

    async def _unpack_accounts_view4(self, user_id: int) -> Tuple[List[str], Dict[str, str], Dict[str, str], Dict]:
        """
        兼容 AccountsService.get_accounts_view 返回 (phones,names,health) 或 (phones,names,usernames,health)。
        始终返回四元组：(phones, names, usernames, health)
        """
        try:
            res = await self.svc.get_accounts_view(user_id)
        except Exception:
            return [], {}, {}, {}

        if isinstance(res, tuple):
            if len(res) == 4:
                phones, names, usernames, health = res
                return list(phones or []), dict(names or {}), dict(usernames or {}), dict(health or {})
            if len(res) == 3:
                phones, names, health = res
                phones = list(phones or [])
                names = dict(names or {})
                usernames = {ph: "-" for ph in phones}
                return phones, names, usernames, dict(health or {})
        return [], {}, {}, {}

    def _build_accounts_page_args(
        self,
        *,
        owner_uid: int,
        phones,
        names,
        usernames,
        health_info,
        page: int,
        total_pages: int,
        selected: Set[str],
        page_size: int,
        pack_fn,
        upload_open,
        show_online_hint: bool
    ):
        """
        兼容 unified.accounts_ui.build_accounts_page 是否支持 usernames 参数。
        """
        base_kwargs = dict(
            owner_uid=owner_uid,
            phones=phones,
            names=names,
            health_info=health_info,
            page=page,
            total_pages=total_pages,
            selected=selected,
            page_size=page_size,
            pack_fn=pack_fn,
            upload_open=upload_open,
            show_online_hint=show_online_hint,
        )
        try:
            sig = inspect.signature(build_accounts_page)
            if "usernames" in sig.parameters:
                base_kwargs["usernames"] = usernames
        except Exception:
            pass
        return base_kwargs

    async def _build_accounts_page_for(self, user_id: int, page: int):
        """
        只构建文本与按钮，不发送/编辑消息。
        返回 (text, buttons, page, total_pages)。
        """
        phones, names, usernames, health = await self._unpack_accounts_view4(user_id)
        total_pages = max(1, math.ceil(len(phones) / self.page_size))
        page = max(1, min(int(page or 1), total_pages))
        await self._set_page(user_id, page)

        idx_start = max(0, (page - 1) * self.page_size)
        idx_end = min(idx_start + self.page_size, len(phones))
        phones_page = phones[idx_start:idx_end]
        sid_map: dict[str, str] = {}
        for ph in phones_page:
            sid_map[ph] = await self._sid_set(user_id, ph)

        selected = await self._get_selected(user_id)

        def pack_wrapper(action: str, owner_uid: int, *args: str) -> bytes:
            if action in {"toggle", "health"} and args:
                ph = str(args[0])
                pg = str(args[1]) if len(args) >= 2 else str(page)
                sid = sid_map.get(ph) or self._make_sid(ph)
                return self._pack_acct(action, owner_uid, sid, pg)
            return self._pack_acct(action, owner_uid, *[str(a) for a in args])

        kwargs = self._build_accounts_page_args(
            owner_uid=user_id,
            phones=phones,
            names=names,
            usernames=usernames,
            health_info=health,
            page=page,
            total_pages=total_pages,
            selected=selected,
            page_size=self.page_size,
            pack_fn=pack_wrapper,
            upload_open=UPLOAD_OPEN,
            show_online_hint=True,
        )
        text, buttons = build_accounts_page(**kwargs)
        return text, buttons, page, total_pages

    async def _edit_only_if_changed(self, event, text, buttons, *, link_preview: bool = False):
        """
        仅在内容发生变化时进行编辑；不回退到发送新消息。
        """
        try:
            markup = event.client.build_reply_markup(buttons)
        except Exception:
            markup = None

        try:
            cur_msg = getattr(event, "message", None)
            cur_text = getattr(cur_msg, "message", None) or getattr(cur_msg, "text", None) or ""
        except Exception:
            cur_text = ""

        new_text = text or ""
        if cur_text != new_text:
            try:
                await event.edit(new_text, buttons=markup, link_preview=link_preview)
                return True
            except MessageNotModifiedError:
                log_debug("refresh(diff): MessageNotModified → skip", extra=get_log_context())
                return False
            except Exception as e:
                log_warning("refresh: edit failed; skip sending new", extra={"err": str(e), **(get_log_context() or {})})
                return False

        log_debug("refresh: no diff; skip edit", extra=get_log_context())
        return False

    # ---------------- 持久化 UI 状态 ----------------
    async def _get_page(self, uid: int) -> int:
        if not self.redis:
            return self._mem_page.get(uid, 1)
        raw = await self.redis.get(K_PAGE.format(uid=uid))
        try:
            v = int(raw) if raw is not None else 1
            return max(1, v)
        except Exception:
            return 1

    async def _set_page(self, uid: int, page: int) -> None:
        p = max(1, int(page))
        if not self.redis:
            self._mem_page[uid] = p
            return
        k = K_PAGE.format(uid=uid)
        await self.redis.set(k, p)
        try:
            await self.redis.expire(k, EXPIRE_S)
        except Exception:
            pass

    async def _get_selected(self, uid: int) -> Set[str]:
        if not self.redis:
            return self._mem_selected.setdefault(uid, set())
        try:
            members = await self.redis.smembers(K_SELECTED.format(uid=uid))
            out = set()
            for m in (members or []):
                out.add(m.decode("utf-8") if isinstance(m, (bytes, bytearray)) else str(m))
            # 活跃读取时刷新 TTL
            try:
                await self.redis.expire(K_SELECTED.format(uid=uid), EXPIRE_S)
            except Exception:
                pass
            return out
        except Exception:
            return set()
    # 统一：把“已选集合”与“有效账号集合”取交集，移除失效项
    async def _normalize_selected(self, uid: int, valid_phones: list[str]) -> Set[str]:
        selected = await self._get_selected(uid)
        if not selected:
            return set()
        valid = set(map(str, valid_phones or []))
        invalid = [ph for ph in selected if ph not in valid]
        if invalid:
            await self._sel_remove_many(uid, invalid)
        return set(ph for ph in selected if ph in valid)

    async def _sel_add_many(self, uid: int, phones: List[str]) -> None:
        if not phones:
            return
        if not self.redis:
            self._mem_selected.setdefault(uid, set()).update(phones)
            return
        try:
            k = K_SELECTED.format(uid=uid)
            await self.redis.sadd(k, *phones)
            await self.redis.expire(k, EXPIRE_S)
        except Exception:
            pass

    async def _sel_remove_many(self, uid: int, phones: List[str]) -> None:
        if not phones:
            return
        if not self.redis:
            self._mem_selected.setdefault(uid, set()).difference_update(phones)
            return
        try:
            k = K_SELECTED.format(uid=uid)
            await self.redis.srem(k, *phones)
            await self.redis.expire(k, EXPIRE_S)
        except Exception:
            pass

    async def _sel_clear(self, uid: int) -> None:
        if not self.redis:
            self._mem_selected.pop(uid, None)
            return
        try:
            await self.redis.delete(K_SELECTED.format(uid=uid))
        except Exception:
            pass

    # ---------------- 回调打包 ----------------
    @staticmethod
    def _pack_acct(action: str, owner_uid: int, *args: str) -> bytes:
        token = cb_pack("acct", action, int(owner_uid), *[str(a) for a in args])
        return ensure_cb_len(b"a:" + token, label="accounts_cb")

    # ---------------- 渲染 ----------------
    async def _render(self, event, user_id: int, *, goto_page: Optional[int] = None) -> None:
        await self._ack(event)
        set_log_context({"user_id": user_id, "phase": "handler", "function": "_render", "module": __name__, "signal": "render"})
        page = int(goto_page or await self._get_page(user_id) or 1)

        phones, names, usernames, health = await self._unpack_accounts_view4(user_id)
        total_pages = max(1, math.ceil(len(phones) / self.page_size))
        page = max(1, min(page, total_pages))
        await self._set_page(user_id, page)

        # 渲染前规范化选择集（与“当前用户有效账号全集 phones”对齐）
        selected = await self._normalize_selected(user_id, phones)
        log_info("🧩 _render: data ready",
                 extra={"user_id": user_id, "page": page, "total_pages": total_pages, "phones": len(phones), "selected": len(selected)})

        idx_start = max(0, (page - 1) * self.page_size)
        idx_end = min(idx_start + self.page_size, len(phones))
        phones_page = phones[idx_start:idx_end]
        sid_map: dict[str, str] = {}
        for ph in phones_page:
            sid_map[ph] = await self._sid_set(user_id, ph)

        def pack_wrapper(action: str, owner_uid: int, *args: str) -> bytes:
            if action in {"toggle", "health"} and args:
                ph = str(args[0])
                pg = str(args[1]) if len(args) >= 2 else str(page)
                sid = sid_map.get(ph) or self._make_sid(ph)
                return self._pack_acct(action, owner_uid, sid, pg)
            return self._pack_acct(action, owner_uid, *[str(a) for a in args])

        kwargs = self._build_accounts_page_args(
            owner_uid=user_id,
            phones=phones,
            names=names,
            usernames=usernames,
            health_info=health,
            page=page,
            total_pages=total_pages,
            selected=selected,
            page_size=self.page_size,
            pack_fn=pack_wrapper,
            upload_open=UPLOAD_OPEN,
            show_online_hint=True,
        )
        text, buttons = build_accounts_page(**kwargs)
        await self._send_or_edit(event, text, buttons)

    # ---------------- 健康检查（精简 & 限流交给 Telethon） ----------------
    async def _do_health_check(self, user_id: int, phone: str) -> None:
        cm = get_client_manager()
        hc = getattr(cm, "health_checker", None)

        try:
            cli = await cm.get_client_or_connect(user_id, phone)
            if not cli:
                self._cm_update_status(cm, user_id, phone, HealthState.NETWORK_ERROR)
                return

            # 统一把短 Flood 交给 Telethon 自动 sleep，长 Flood 才抛异常
            try:
                cli.flood_sleep_threshold = FLOOD_AUTOSLEEP_THRESHOLD
            except Exception:
                pass

            try:
                me = await asyncio.wait_for(cli.get_me(), timeout=8)
                if not me:
                    self._cm_update_status(cm, user_id, phone, HealthState.AUTH_EXPIRED)
                    return

                if hc:
                    hc._set_state(phone, HealthState.OK, origin="manual_check", notify=False)
                else:
                    self._cm_update_status(cm, user_id, phone, HealthState.OK)
                return

            except errors.FloodWaitError as e:
                secs = int(getattr(e, "seconds", 0) or 0)
                self._cm_update_status(cm, user_id, phone, HealthState.FLOOD_WAIT)
                if hc:
                    hc._set_state(phone, HealthState.FLOOD_WAIT)
                    await hc._try_set_cooldown(phone, max(1, secs))
                    await hc._broadcast_floodwait(phone, max(0, secs))
                else:
                    try:
                        await bus.dispatch("FLOOD_WAIT", {"type": "FLOOD_WAIT", "phone": phone, "seconds": secs, "user_ids": [user_id]})
                    except Exception:
                        pass
                return

            except (errors.UserDeactivatedBanError, errors.AccountDeactivatedError):
                self._cm_update_status(cm, user_id, phone, HealthState.BANNED)
                if hc:
                    hc._set_state(phone, HealthState.BANNED)
                    await hc._broadcast_banned(phone)
                else:
                    try:
                        await bus.dispatch("BANNED", {"type": "BANNED", "phone": phone, "user_ids": [user_id]})
                    except Exception:
                        pass
                return

            except (errors.AuthKeyUnregisteredError, errors.UserDeactivatedError):
                self._cm_update_status(cm, user_id, phone, HealthState.AUTH_EXPIRED)
                return

            except errors.RPCError as e:
                # 优先 Telethon 的错误，保留 classify_telethon_error 作为补充映射
                state = None
                meta = {}
                try:
                    dec = classify_telethon_error(e)
                    if isinstance(dec, tuple) and len(dec) >= 1:
                        state, meta = dec[0], (dec[1] if len(dec) >= 2 else {})
                    else:
                        # Decision 对象 → HealthState
                        from core.health import _decision_to_health_state
                        state = _decision_to_health_state(dec)
                        meta = {"seconds": getattr(dec, "seconds", None)}
                except Exception:
                    state, meta = (HealthState.NETWORK_ERROR, {})

                self._cm_update_status(cm, user_id, phone, state or HealthState.NETWORK_ERROR)

                if state == HealthState.FLOOD_WAIT:
                    secs = int((meta or {}).get("seconds", 0) or getattr(e, "seconds", 0) or 0)
                    if hc:
                        hc._set_state(phone, HealthState.FLOOD_WAIT)
                        await hc._try_set_cooldown(phone, max(1, secs))
                        await hc._broadcast_floodwait(phone, max(0, secs))
                    else:
                        try:
                            await bus.dispatch("FLOOD_WAIT", {"type": "FLOOD_WAIT", "phone": phone, "seconds": secs, "user_ids": [user_id]})
                        except Exception:
                            pass
                elif state == HealthState.BANNED:
                    if hc:
                        hc._set_state(phone, HealthState.BANNED)
                        await hc._broadcast_banned(phone)
                    else:
                        try:
                            await bus.dispatch("BANNED", {"type": "BANNED", "phone": phone, "user_ids": [user_id]})
                        except Exception:
                            pass
                return

        except Exception:
            try:
                self._cm_update_status(cm, user_id, phone, HealthState.NETWORK_ERROR)
            except Exception:
                pass
            return

    # ---------- /d（账号管理） ----------
    @super_command(trace_action="账号管理", white_only=False)
    async def on_accounts_command(self, event) -> None:
        if not await _allow(event):
            try:
                await BotUtils.safe_respond(event, event.client, "⚠️ 你没有权限访问该功能。")
            except Exception:
                pass
            return

        user_id = int(event.sender_id)
        set_log_context({"user_id": user_id, "phase": "handler", "function": "on_accounts_command", "module": __name__, "signal": "command", "trace_id": generate_trace_id()})
        log_info("➡️ ENTER on_accounts_command", extra={"user_id": user_id, "raw_text": (event.raw_text or "")[:64], "command": "/d"})
        try:
            parts = (event.raw_text or "").strip().split()
            goto = int(parts[1]) if len(parts) >= 2 else 1
        except Exception:
            goto = 1
        await self._set_page(user_id, max(1, goto))
        await self._sel_clear(user_id)
        await self._render(event, user_id, goto_page=goto)

    # ---------- “账号管理”入口按钮 ----------
    @command_safe(white_only=False)
    async def on_accounts_open_cb(self, event) -> None:
        user_id = int(event.sender_id)
        set_log_context({"user_id": user_id, "phase": "handler", "function": "on_accounts_open_cb", "module": __name__, "signal": "callback", "trace_id": generate_trace_id()})
        log_info("➡️ ENTER on_accounts_open_cb", extra={"user_id": user_id, "data": "ACCOUNTS_OPEN"})
        try:
            await self._set_page(user_id, 1)
            await self._render(event, user_id, goto_page=1)
            log_info("✅ EXIT on_accounts_open_cb (render scheduled)", extra={"user_id": user_id})
        except Exception as e:
            log_exception("❌ on_accounts_open_cb 异常", exc=e, extra={"user_id": user_id})

    # ---------- 主回调（a:<token>）----------
    @command_safe(white_only=False)
    async def on_accounts_cb(self, event) -> None:
        user_id = int(getattr(event, "sender_id", 0) or 0)
        set_log_context({"user_id": user_id, "phase": "handler", "function": "on_accounts_cb", "module": __name__, "signal": "callback"})
        data: bytes = event.data or b""
        if not (data and data.startswith(b"a:")):
            log_debug("on_accounts_cb: skip non a:* callback", extra={"user_id": user_id, "data_prefix": data[:32]})
            return

        payload = data[2:]
        parsed = cb_unpack(payload)
        if not parsed:
            log_warning("accounts_cb: token 验签失败", extra=get_log_context()); return
        ns, action, owner_uid, args = parsed
        log_info("on_accounts_cb: parsed", extra={"ns": ns, "action": action, "owner": int(owner_uid), "args": args})
        if ns != "acct":
            return
        if int(owner_uid) != user_id:
            log_warning("accounts_cb: 非所有者触发，忽略", extra={"owner": owner_uid, "sender": user_id, **(get_log_context() or {})})
            return

        cur_page = await self._get_page(user_id)
        phones, names, usernames, health = await self._unpack_accounts_view4(user_id)
        total_pages = max(1, math.ceil(len(phones) / self.page_size))
        cur_page = max(1, min(cur_page, total_pages))

        idx_start = max(0, (cur_page - 1) * self.page_size)
        idx_end = min(idx_start + self.page_size, len(phones))

        selected = await self._get_selected(user_id)

        # 翻页
        if action == "page":
            log_debug("on_accounts_cb: action=page", extra={"args": args})
            try:
                goto = max(1, int(args[0])) if args else cur_page
            except Exception:
                goto = cur_page
            await self._set_page(user_id, goto)
            await self._render(event, user_id, goto_page=goto)
            return

        # 页级多选
        if action in {"sel_all", "sel_none", "sel_invert"}:
            try:
                goto = max(1, int(args[0])) if args else cur_page
            except Exception:
                goto = cur_page

            idx_start2 = max(0, (goto - 1) * self.page_size)
            idx_end2 = min(idx_start2 + self.page_size, len(phones))
            page_list2 = phones[idx_start2:idx_end2]

            # 先规范化（防止对不可见/已隔离号码操作）
            await self._normalize_selected(user_id, phones)

            if action == "sel_all":
                await self._sel_add_many(user_id, page_list2)
            elif action == "sel_none":
                await self._sel_remove_many(user_id, page_list2)
            else:
                cur_sel = await self._get_selected(user_id)
                to_add, to_remove = [], []
                for ph in page_list2:
                    (to_remove if ph in cur_sel else to_add).append(ph)
                if to_add:
                    await self._sel_add_many(user_id, to_add)
                if to_remove:
                    await self._sel_remove_many(user_id, to_remove)

            await self._render(event, user_id, goto_page=goto)
            return

        # 单个切换
        if action == "toggle":
            log_debug("on_accounts_cb: action=toggle", extra={"args": args})
            sid_or_phone = (args[0] if len(args) >= 1 else "")
            try:
                goto = max(1, int(args[1])) if len(args) >= 2 else cur_page
            except Exception:
                goto = cur_page

            phone = None
            if sid_or_phone:
                phone = await self._sid_get(user_id, sid_or_phone) or sid_or_phone

            if phone:
                if phone in selected:
                    await self._sel_remove_many(user_id, [phone])
                else:
                    await self._sel_add_many(user_id, [phone])
            await self._render(event, user_id, goto_page=goto)
            return

        # 批量体检
        if action == "health_bulk":
            log_debug("on_accounts_cb: action=health_bulk", extra={"args": args})
            try:
                goto = max(1, int(args[0])) if args else cur_page
            except Exception:
                goto = cur_page

            # 规范化后再取已选，避免对失效账号体检
            await self._normalize_selected(user_id, phones)
            sel = [ph for ph in await self._get_selected(user_id) if ph in set(phones)]
            if not sel:
                try:
                    await event.answer("请先在列表中勾选要体检的账号", alert=True)
                except Exception:
                    pass
                return

            sem = asyncio.Semaphore(5)

            async def _run_one(ph: str):
                async with sem:
                    _ = await self._do_health_check(user_id, ph)

            try:
                # 不因单个失败而中断全部
                await asyncio.gather(*(_run_one(ph) for ph in sel), return_exceptions=True)
            except Exception as e:
                log_exception("批量体检执行异常", exc=e, extra={"user_id": user_id, "count": len(sel)})

            try:
                cm = get_client_manager()
                ok_cnt = abnormal_cnt = unknown_cnt = 0
                for ph in sel:
                    st = self._cm_get_health_state(cm, user_id, ph)
                    st = HealthState.parse(st)
                    if st == HealthState.OK:
                        ok_cnt += 1
                    elif st == HealthState.UNKNOWN:
                        unknown_cnt += 1
                    else:
                        abnormal_cnt += 1
                summary = f"🩺 批量体检完成：共 {len(sel)} 个\n✅ 正常：{ok_cnt} | ⚠️ 异常：{abnormal_cnt} | ❔ 未知：{unknown_cnt}"
            except Exception:
                summary = f"🩺 批量体检完成：共 {len(sel)} 个"

            try:
                await event.answer(summary, alert=True)
            except Exception:
                await self._send_or_edit(event, summary, buttons=None, link_preview=False)

            await self._render(event, user_id, goto_page=goto)
            return

        # 单项体检
        if action == "health":
            log_debug("on_accounts_cb: action=health", extra={"args": args})
            sid_or_phone = (args[0] if len(args) >= 1 else "")
            try:
                goto = max(1, int(args[1])) if len(args) >= 2 else cur_page
            except Exception:
                goto = cur_page

            phone = None
            if sid_or_phone:
                phone = await self._sid_get(user_id, sid_or_phone) or sid_or_phone

            if phone:
                try:
                    await self._do_health_check(user_id, phone)
                    await event.answer("✅ 已体检该账号", alert=False)
                except Exception as e:
                    log_exception("单项体检异常", exc=e, extra={"user_id": user_id, "phone": phone})
                    try:
                        await event.answer("⚠️ 该账号体检失败", alert=True)
                    except Exception:
                        pass
            await self._render(event, user_id, goto_page=goto)
            return

        # 刷新
        if action == "refresh":
            log_debug("on_accounts_cb: action=refresh")
            try:
                await event.answer(cache_time=0)
            except Exception:
                pass

            goto_page = cur_page
            try:
                text, buttons, page_fixed, total_pages = await self._build_accounts_page_for(user_id, goto_page)
                await self._edit_only_if_changed(event, text, buttons, link_preview=False)
            except Exception as e:
                log_exception("refresh: build/edit failed", exc=e, extra={"user_id": user_id})

            try:
                await event.answer("✅ 已完成刷新", alert=True)
            except Exception:
                pass
            return

        # 关闭
        if action == "close":
            log_info("on_accounts_cb: action=close")
            await self._ack(event)
            try:
                await event.edit("已关闭。", buttons=None, link_preview=False)
            except Exception:
                await BotUtils.safe_respond(event, event.client, "已关闭。")
            return

        # 删除/清空确认
        if action in {"confirm_del", "confirm_clear"}:
            log_info("on_accounts_cb: action=confirm_*", extra={"selected_count": len(selected)})
            from telethon import Button
            if action == "confirm_del":
                if not selected:
                    try:
                        await event.answer("请先选择账号", alert=True)
                    except Exception:
                        pass
                    return
                title = "⚠️ 确认删除所选账号？"
                ok_action = "do_del"
            else:
                title = "⚠️ 确认清空全部账号？"
                ok_action = "do_clear"

            btns = [[
                Button.inline("✅ 确认", data=self._pack_acct(ok_action, user_id)),
                Button.inline("❌ 取消", data=self._pack_acct("refresh", user_id, str(cur_page))),
            ]]
            await self._ack(event)
            await self._send_or_edit(event, title, btns)
            return

        if action == "do_del":
            log_info("on_accounts_cb: action=do_del")
            sel = list(await self._get_selected(user_id))
            if sel:
                await self.svc.delete_accounts(user_id, sel)
            await self._sel_clear(user_id)
            await self._set_page(user_id, 1)
            await self._render(event, user_id, goto_page=1)
            return

        if action == "do_clear":
            log_info("on_accounts_cb: action=do_clear")
            await self._sel_clear(user_id)
            await self.svc.clear_accounts(user_id)
            await self._set_page(user_id, 1)
            await self._render(event, user_id, goto_page=1)
            return

        # 默认兜底
        await self._render(event, user_id, goto_page=cur_page)

    def register_to_client(self, client) -> None:
        register_callbacks(client)
        log_info("accounts_callbacks_registered(client) - via register_callbacks()")


# ---------- 群发命名空间：task:* 分发 ----------
@command_safe(white_only=True)
async def on_task_ns_cb(event):
    try:
        await event.answer()
    except Exception:
        pass

    data: bytes = event.data or b""
    uid = getattr(event, "sender_id", None)
    set_log_context({"user_id": uid, "phase": "task_ns", "data": (data[:32] if isinstance(data, (bytes, bytearray)) else str(data))})

    try:
        if data == TASK_SET_GROUPS:
            from handlers.commands.group_command import handle_group_input
            await handle_group_input(event); return
        if data == TASK_SET_MESSAGE:
            from handlers.commands.message_command import handle_message
            await handle_message(event); return
        if data == TASK_SET_INTERVAL:
            from handlers.commands.interval_command import handle_interval_input
            await handle_interval_input(event); return
        if data == TASK_START:
            from handlers.commands.control_command import start_task
            await start_task(event); return
        if data == TASK_STOP:
            from handlers.commands.control_command import stop_task
            await stop_task(event); return

        await send_task_menu(event, ACCOUNTS_OPEN)
    except Exception as e:
        log_exception("❌ task 命名空间回调异常", exc=e, extra=get_log_context())
        await BotUtils.safe_respond(event, event.client, "⚠️ 操作失败，请稍后重试。")


# ====== 模块级单例 ======
_ACCOUNTS = AccountsCommand()


def register_commands():
    # 命令入口权限由 _allow 统一校验
    main_router.command("/d", trace_name="账号管理", white_only=False)(_ACCOUNTS.on_accounts_command)
    return True


# ---------- 调试探针 & BYPASS ----------
async def _debug_probe_accounts_open(event):
    try:
        data = event.data or b""
        log_info("🔎 probe: 收到 accounts:open", extra={
            "sender_id": getattr(event, "sender_id", None),
            "chat_id": getattr(event, "chat_id", None),
            "data": data,
        })
        try:
            await event.answer()
        except Exception:
            pass
    except Exception as e:
        log_exception("probe: accounts_open 记录失败", exc=e)


async def _bypass_accounts_open(event):
    uid = getattr(event, "sender_id", None)
    log_info("🟢 BYPASS accounts:open 命中（无白名单校验）", extra={"user_id": uid})
    try:
        await _ACCOUNTS._set_page(int(uid), 1)
        await _ACCOUNTS._render(event, int(uid), goto_page=1)
        log_info("🟢 BYPASS 渲染完成", extra={"user_id": uid})
    except Exception as e:
        log_exception("❌ BYPASS 渲染异常", exc=e, extra={"user_id": uid})


async def _bypass_accounts_ns(event):
    uid = getattr(event, "sender_id", None)
    data: bytes = event.data or b""
    if not (data and data.startswith(b"a:")):
        return
    log_info("🟢 BYPASS a:* 命中（无白名单校验）", extra={"user_id": uid, "data_prefix": data[:32]})
    try:
        await _ACCOUNTS.on_accounts_cb.__wrapped__(_ACCOUNTS, event)
    except AttributeError:
        try:
            await _ACCOUNTS._render(event, int(uid), goto_page=await _ACCOUNTS._get_page(int(uid)))
        except Exception as e:
            log_exception("❌ BYPASS a:* 兜底渲染异常", exc=e, extra={"user_id": uid})
    except Exception as e:
        log_exception("❌ BYPASS a:* 处理异常", exc=e, extra={"user_id": uid})


def register_callbacks(bot=None):
    from unified.context import get_bot
    from core.registry_guard import mark_callbacks_registered, safe_add_event_handler
    from telethon import events

    bot = bot or get_bot()
    if not bot:
        log_warning("⚠️ accounts.register_callbacks: bot 未注入，回调未注册")
        return False

    if not mark_callbacks_registered(bot, "accounts_module"):
        log_debug("⚠️ accounts.register_callbacks: 命名空间已存在（accounts_module），本次跳过")
        return False

    safe_add_event_handler(bot, _ACCOUNTS.on_accounts_open_cb, events.CallbackQuery(data=ACCOUNTS_OPEN), tag="accounts:open")
    safe_add_event_handler(bot, _ACCOUNTS.on_accounts_cb,       events.CallbackQuery(pattern=PATTERN_ACCT), tag="accounts:ns")

    safe_add_event_handler(bot, on_taskmenu_open_cb, events.CallbackQuery(data=TASKMENU_OPEN), tag="taskmenu:open")
    safe_add_event_handler(bot, on_task_ns_cb,       events.CallbackQuery(pattern=PATTERN_TASK), tag="taskmenu:ns")

    try:
        from unified.config import DEBUG_BYPASS_CALLBACKS as _DBG_BYPASS
    except Exception:
        _DBG_BYPASS = True

    if _DBG_BYPASS:
        safe_add_event_handler(bot, _debug_probe_accounts_open, events.CallbackQuery(data=ACCOUNTS_OPEN), tag="debug:accounts_open")
        safe_add_event_handler(bot, _bypass_accounts_open,      events.CallbackQuery(data=ACCOUNTS_OPEN), tag="accounts:open:bypass")
        safe_add_event_handler(bot, _bypass_accounts_ns,        events.CallbackQuery(pattern=PATTERN_ACCT), tag="accounts:ns:bypass")
        log_info("🟡 DEBUG: accounts:open & a:* BYPASS 已挂载")

    return True
