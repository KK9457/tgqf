# unified/auth.py
from __future__ import annotations

from unified.config import get_admin_ids
from unified.context import get_fsm

async def is_user_whitelisted(user_id: int) -> bool:
    """
    统一判断：管理员恒等同于白名单；否则查全局白名单集合。
    """
    try:
        if user_id in get_admin_ids():
            return True
    except Exception:
        pass
    fsm = get_fsm()
    if not fsm:
        return False
    return bool(await fsm.is_in_whitelist(0, user_id))
