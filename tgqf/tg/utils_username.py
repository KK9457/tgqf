# -*- coding: utf-8 -*-
# tg/utils_username.py
"""
频道/超级群用户名设置工具：
- ensure_unique_username：先检测可用性，不可用则一次性追加3位 [a-z0-9] 后缀再设置。
"""

import random
import string
from typing import Tuple
from telethon import utils
from telethon.tl.functions.channels import CheckUsernameRequest, UpdateUsernameRequest
from telethon.tl.types import InputChannel
from telethon.errors import (
    UsernameOccupiedError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    RPCError,
)

_SUFFIX_POOL = string.ascii_lowercase + string.digits

def _suffix(name: str) -> str:
    """生成 3 位 [a-z0-9] 后缀；最多保留前 29 个字符"""
    base = (name or "").strip().lower()
    return base[:29] + "".join(random.choices(_SUFFIX_POOL, k=3))

async def ensure_unique_username(client, entity: InputChannel, username: str) -> Tuple[str, bool]:
    """
    确保唯一的用户名：
    - 检测用户名是否可用；
    - 若不可用，添加后缀再尝试设置。
    """
    # 清理并处理输入的用户名
    uname = (username or "").strip().lstrip("@").lower()
    if not uname:
        new_uname = _suffix("")
        return await _try_update_username(client, entity, new_uname)

    # 尝试直接设置用户名
    if await _check_and_set_username(client, entity, uname):
        return uname, True

    # 尝试加后缀生成新的用户名
    new_uname = _suffix(uname)
    return await _try_update_username(client, entity, new_uname)

async def _check_and_set_username(client, entity: InputChannel, username: str) -> bool:
    try:
        ok = await client(CheckUsernameRequest(channel=entity, username=username))
        # Telethon 直接返回 Python bool，不再有 tl.types.Bool
        if bool(ok):
            await client(UpdateUsernameRequest(channel=entity, username=username))
            return True
        return False
    except (UsernameInvalidError, UsernameOccupiedError, RPCError):
        return False

async def _try_update_username(client, entity: InputChannel, username: str) -> Tuple[str, bool]:
    """
    尝试更新用户名，如果失败则返回失败的原因。
    """
    try:
        # 尝试更新用户名
        await client(UpdateUsernameRequest(channel=entity, username=username))
        return username, True
    except (UsernameInvalidError, UsernameOccupiedError, RPCError):
        return username, False


def display_name_from_me(me) -> str:
    dn = utils.get_display_name(me) or ""  # 官方
    if dn.strip():
        return dn.strip()
    uname = (getattr(me, "username", "") or "").strip()
    return f"@{uname}" if uname else "-"

def username_from_me(me, *, with_at: bool = True) -> str:
    uname = (getattr(me, "username", "") or "").strip()
    if not uname:
        return "-"
    return f"@{uname}" if with_at and not uname.startswith("@") else uname