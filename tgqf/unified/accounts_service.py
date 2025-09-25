# unified/accounts_service.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Any
import json
import os

from typess.health_types import HealthInfo, HealthState, make_health_info
from unified.context import get_client_manager, get_scheduler
from unified.config import get_phone_json
from unified.logger import log_debug, log_warning

class AccountsService:
    """
    面向账号管理 UI 的聚合服务：
      - 拉取用户账号列表、名称、健康信息（HealthInfo）
      - 删除选中 / 清空全部
    """

    def _read_name_from_json(self, user_id: int, phone: str) -> Optional[str]:
        try:
            p = get_phone_json(user_id, phone)
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for k in ("name", "nickname", "display_name"):
                    v = data.get(k)
                    if v: return str(v)
        except Exception:
            pass
        return None

    def _read_username_from_json(self, user_id: int, phone: str) -> Optional[str]:
        try:
            p = get_phone_json(user_id, phone)
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                v = data.get("username")
                if v: return "@" + str(v).lstrip("@")
        except Exception:
            pass
        return None


    async def get_accounts_view(
        self, user_id: int
    ) -> Tuple[List[str], Dict[str, str], Dict[str, str], Dict[str, HealthInfo]]:
        """
        返回：phones, names, usernames, health_info
        """
        cm = get_client_manager()
        if cm is None:
            log_warning("AccountsService: client_manager 未注入，返回空集", extra={"user_id": user_id})
            return [], {}, {}, {}

        try:
            pairs = cm.list_user_phones(user_id) or []
            phones = sorted({str(p) for p, _ in pairs})
        except Exception:
            phones = []

        names: Dict[str, str] = {}
        usernames: Dict[str, str] = {}
        health: Dict[str, HealthInfo] = {}

        mapping = {}
        try:
            mapping = cm.get_clients_for_user(user_id) or {}
        except Exception:
            mapping = {}

        for ph in phones:
            # 名称
            name = None
            try:
                if hasattr(cm, "get_account_name"):
                    name = cm.get_account_name(user_id, ph)
            except Exception:
                pass
            name = name or self._read_name_from_json(user_id, ph) or "-"

            # 用户名
            uname = self._read_username_from_json(user_id, ph) or "-"
            cli = mapping.get(ph) if isinstance(mapping, dict) else None
            if cli:
                try:
                    # 借助 SessionClientManager 的缓存友好方法
                    me = await cm.get_cached_me(cli)
                    if me and getattr(me, "username", None):
                        uname = "@" + str(me.username).lstrip("@")
                except Exception:
                    pass

            # 在线/健康
            try:
                is_conn = bool(cli.is_connected()) if cli else False
            except Exception:
                is_conn = False

            state_raw = None
            try:
                if hasattr(cm, "get_health_state"):
                    state_raw = cm.get_health_state(user_id, ph)
            except Exception:
                state_raw = None
            state = (
                HealthState.parse(state_raw)
                if state_raw is not None
                else (HealthState.OK if is_conn else HealthState.NETWORK_ERROR)
            )

            names[ph] = str(name)
            usernames[ph] = str(uname)
            health[ph] = make_health_info(state, online=is_conn)

        log_debug("accounts_service_view", extra={"user_id": user_id, "count": len(phones)})
        return phones, names, usernames, health

    async def delete_accounts(self, user_id: int, phones: List[str]) -> Dict[str, int]:
        from unified.logger import log_info, log_warning
        if not phones:
            return {"ok": 0, "fail": 0}
        cm = get_client_manager()
        if cm is None:
            log_warning("accounts_delete_failed: client_manager 未注入", extra={"user_id": user_id})
            return {"ok": 0, "fail": len(phones)}
        try:
            if hasattr(cm, "remove_accounts"):
                res = await cm.remove_accounts(user_id, phones)
            else:
                res = {"ok": 0, "fail": len(phones)}
            log_info("accounts_deleted", extra={"user_id": user_id, **(res or {})})
            return res or {"ok": 0, "fail": 0}
        except Exception as e:
            log_warning("accounts_delete_failed", extra={"user_id": user_id, "err": str(e)})
            return {"ok": 0, "fail": len(phones)}
    # =========================
    # 体检所选 → 可发预览（SEND）
    # =========================
    async def check_selected_and_preview_send(
        self,
        user_id: int,
        selected_phones: List[str],
    ) -> Tuple[List[str], Dict[str, Any]]:
        """
        步骤：
          1) 仅为所选手机号做最小准备（prepare_clients_for_user: phones_whitelist）
          2) 对所选逐号 perform_health_check（或跳过若无健康检查器）
          3) 统一选号口径：SEND（Health==OK 且 不在冷却期）
        返回：
          (sendable_phones, {"stats": stats, "excluded": excluded})
        """
        cm = get_client_manager()
        if cm is None:
            log_warning("check_selected_and_preview_send: client_manager 未注入", extra={"user_id": user_id})
            return [], {"stats": {}, "excluded": {}}

        scheduler = None
        try:
            scheduler = get_scheduler()
        except Exception:
            scheduler = None
        validator = getattr(scheduler, "validator", None) if scheduler else None

        phones = [str(p) for p in (selected_phones or []) if p]
        if not phones:
            return [], {"stats": {}, "excluded": {}}

        # ① 最小准备：仅子集（避免全量扫描/断连）
        try:
            clients = await cm.prepare_clients_for_user(
                user_id,
                release_existing=False,
                cleanup_journals=True,
                validator=validator,
                set_cooldown=False,
                log_prefix="accounts_health_check(subset)",
                phones_whitelist=phones,
            )
        except Exception:
            # 兜底：直接取内存已有
            clients = {p: cm.get_client(user_id, p) for p in phones if cm.get_client(user_id, p)}

        # ② 逐号体检（优先单号，而不是全量 check_all，避免误伤其他用户）
        try:
            hc = getattr(cm, "health_checker", None)
            if hc:
                for ph in phones:
                    cli = clients.get(ph) or cm.get_client(user_id, ph)
                    if cli:
                        try:
                            await hc.perform_health_check(ph, cli)
                        except Exception:
                            continue
            else:
                log_warning("accounts_health_check: 未注入 HealthChecker，跳过主动体检", extra={"user_id": user_id})
        except Exception as e:
            log_warning("accounts_health_check 执行异常（忽略）", extra={"user_id": user_id, "err": str(e)})

        # ③ 统一选号：SEND（Health==OK 且不在冷却期）
        #    兼容不同项目目录结构，做一次宽松导入
        try:
            from unified.selection import unified_prepare_and_select_clients  # preferred path
        except Exception:
            try:
                from unified.selection import unified_prepare_and_select_clients  # fallback
            except Exception as _e:
                log_warning("accounts_health_check: 选号函数未导入", extra={"user_id": user_id, "err": str(_e)})
                return [], {"stats": {}, "excluded": {}}
        sendable, stats, excluded = await unified_prepare_and_select_clients(
            user_id,
            phones_whitelist=phones,
            intent="SEND",
            validator=validator,
        )
        return list(sendable.keys()), {"stats": stats, "excluded": excluded}

    async def clear_accounts(self, user_id: int) -> Dict[str, int]:
        from unified.logger import log_info, log_warning
        cm = get_client_manager()
        if cm is None:
            log_warning("accounts_clear_failed: client_manager 未注入", extra={"user_id": user_id})
            return {"ok": 0, "fail": 0}
        try:
            if hasattr(cm, "clear_all_accounts"):
                res = await cm.clear_all_accounts(user_id)
            elif hasattr(cm, "remove_accounts"):
                pairs = cm.list_user_phones(user_id) or []
                phones = [p for p, _ in pairs]
                res = await cm.remove_accounts(user_id, phones)
            else:
                res = {"ok": 0, "fail": 0}
            log_info("accounts_cleared", extra={"user_id": user_id, **(res or {})})
            return res or {"ok": 0, "fail": 0}
        except Exception as e:
            log_warning("accounts_clear_failed", extra={"user_id": user_id, "err": str(e)})
            return {"ok": 0, "fail": 0}
