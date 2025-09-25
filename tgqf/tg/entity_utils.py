# tg/entity_utils.py
# -*- coding: utf-8 -*-

"""
封装 get_entity 的类型判断、安全获取、通用属性提取等。
支持对 ParsedLink 的智能识别和链接行为统一调用。
"""
from __future__ import annotations
from telethon.tl.types import TypeInputPeer
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Union

from telethon import TelegramClient, functions, types, utils
from telethon.errors import RPCError
from telethon.tl.types import (
    Channel,
    ChannelFull,
    Chat,
    InputPeerChannel,
    InputPeerChat,
    InputPeerUser,
    User,
)
from telethon.utils import get_input_peer
import json
from redis.asyncio import Redis
from telethon.tl import types as t
from typess.fsm_keys import K_PEER_CACHE
from unified.context import get_redis

def resolve_marked_peer(marked_id: int):
    """
    将带标记 ID（user: 正；chat: -id；channel: -100id）解析为 Peer* 实例。
    """
    real_id, peer_type = utils.resolve_id(marked_id)
    return peer_type(real_id)

# ---------- 通用封装函数 ----------
async def ensure_entity_seen(client: TelegramClient, dialogs_limit: int = 300) -> None:
    """确保获取对话列表，确保缓存已更新"""
    try:
        await client.get_dialogs(limit=dialogs_limit)
    except Exception:
        pass

async def get_input_entity_strict(client: TelegramClient, entity_like) -> types.TypeInputPeer:
    """
    严格获取 InputPeer：首先尝试缓存，未命中则预热 dialogs 一次再试。
    """
    try:
        return await client.get_input_entity(entity_like)
    except ValueError:
        await ensure_entity_seen(client)
        return await client.get_input_entity(entity_like)

# ---------- /c/<cid> 的 InputPeer 构造 ----------
async def get_input_peer_for_c(client: TelegramClient, cid_short: int) -> Optional[TypeInputPeer]:
    """
    将 /c/<cid_short> 标准化为 InputPeer（通常为 InputPeerChannel），未加入/不可见返回 None。
    """
    try:
        peer_id = int(f"-100{cid_short}")
        return await client.get_input_entity(peer_id)
    except (ValueError, TypeError, RPCError):
        return None

# ---------- 类型判断 ----------
def is_user(entity: Any) -> bool:
    """判断是否为 User 类型"""
    return isinstance(entity, User)


def is_channel(entity: Any) -> bool:
    """判断是否为 Channel 类型（支持广播、群组等）"""
    return isinstance(entity, Channel) and getattr(entity, "broadcast", False)

def is_bot(entity: Any) -> bool:
    """判断是否为 Bot 用户类型"""
    return isinstance(entity, User) and getattr(entity, "bot", False)

def get_standard_input_peer(entity: Union[User, Chat, Channel]) -> Union[InputPeerUser, InputPeerChat, InputPeerChannel]:
    """获取标准的 InputPeer 类型实例"""
    return get_input_peer(entity)

# ---------- 频道与用户名支持 ----------
def get_primary_username(obj: Union[Channel, Chat, User]) -> Optional[str]:
    """获取频道、聊天或用户的主用户名"""
    uname = getattr(obj, "username", None)
    if uname:
        return uname
    # 兼容多个用户名
    for x in getattr(obj, "usernames", []):
        u, active = getattr(x, "username", None), getattr(x, "active", True)
        if u and active:
            return u
    return None

# ---------- 频道标记分类 ----------
def classify_channel_flags(ch: Channel) -> Dict[str, bool | str]:
    """分类并返回频道的不同标记状态"""
    if not isinstance(ch, Channel):
        return {"kind": "chat", "is_forum": False}

    kind = "chat"
    if getattr(ch, "megagroup", False): kind = "megagroup"
    if getattr(ch, "broadcast", False): kind = "broadcast"
    if getattr(ch, "gigagroup", False): kind = "gigagroup"

    return {
        "kind": kind,
        "is_forum": bool(getattr(ch, "forum", False)),
        "noforwards": bool(getattr(ch, "noforwards", False)),
        "join_to_send": bool(getattr(ch, "join_to_send", False)),
        "join_request": bool(getattr(ch, "join_request", False)),
        "slowmode_enabled": bool(getattr(ch, "slowmode_enabled", False)),
    }

# ---------- ChannelFull 缓存 ----------
_CF_CACHE: dict[int, tuple[float, ChannelFull]] = {}
_CF_TTL = 120.0  # 缓存过期时间

async def get_full_channel_safe(client: TelegramClient, peer: Union[Channel, InputPeerChannel, int, str]) -> Optional[ChannelFull]:
    """
    获取频道的完整信息（使用缓存，避免频繁请求）
    """
    try:
        cid = utils.get_peer_id(peer)
        cached = _CF_CACHE.get(cid)
        if cached and (time.time() - cached[0] <= _CF_TTL):
            return cached[1]
        
        full = await client(functions.channels.GetFullChannel(channel=peer))
        cf = getattr(full, "full_chat", None)
        if isinstance(cf, ChannelFull):
            _CF_CACHE[cid] = (time.time(), cf)
            return cf
    except RPCError:
        return None
    except Exception:
        return None

# ---------- 频道是否有付费限制 ----------
def channel_has_paid_constraints(ch: Optional[Channel], full: Optional[ChannelFull]) -> bool:
    """检查频道是否有付费相关限制"""
    if ch and getattr(ch, "send_paid_messages_stars", None):
        return True
    if full:
        for field in ("paid_messages_available", "paid_media_allowed", "can_view_stars_revenue"):
            if bool(getattr(full, field, False)):
                return True
    return False

# ===== 新增：Peer 序列化 / 反序列化 =====
def _peer_to_dict(peer: t.Type) -> dict:
    if isinstance(peer, t.InputPeerChannel):
        return {"kind": "channel", "channel_id": peer.channel_id, "access_hash": peer.access_hash}
    if isinstance(peer, t.InputPeerChat):
        return {"kind": "chat", "chat_id": peer.chat_id}
    if isinstance(peer, t.InputPeerUser):
        return {"kind": "user", "user_id": peer.user_id, "access_hash": peer.access_hash}
    if isinstance(peer, t.PeerChannel):
        return {"kind": "peer_channel", "channel_id": peer.channel_id}
    if isinstance(peer, t.PeerChat):
        return {"kind": "peer_chat", "chat_id": peer.chat_id}
    if isinstance(peer, t.PeerUser):
        return {"kind": "peer_user", "user_id": peer.user_id}
    return {"kind": "unknown"}

def _dict_to_input_peer(d: dict) -> t.Type | None:
    k = d.get("kind")
    try:
        if k == "channel":
            return t.InputPeerChannel(channel_id=d["channel_id"], access_hash=d["access_hash"])
        if k == "chat":
            return t.InputPeerChat(chat_id=d["chat_id"])
        if k == "user":
            return t.InputPeerUser(user_id=d["user_id"], access_hash=d["access_hash"])
        if k == "peer_channel":
            return t.InputPeerChannel(channel_id=d["channel_id"], access_hash=0)
        if k == "peer_chat":
            return t.InputPeerChat(chat_id=d["chat_id"])
        if k == "peer_user":
            return t.InputPeerUser(user_id=d["user_id"], access_hash=0)
    except Exception:
        return None
    return None

# ===== 新增：缓存读写 =====
async def cache_peer(target_key: str, peer: t.Type, uid: int) -> None:
    redis = get_redis()
    if not redis:
        return
    try:
        await redis.hset(K_PEER_CACHE.format(uid=uid), target_key, json.dumps(_peer_to_dict(peer), ensure_ascii=False))
    except Exception:
        pass

async def get_cached_peer(target_key: str, uid: int) -> t.Type | None:
    redis = get_redis()
    if not redis:
        return None
    try:
        raw = await redis.hget(K_PEER_CACHE.format(uid=uid), target_key)
        return _dict_to_input_peer(json.loads(raw)) if raw else None
    except Exception:
        return None

# ===== 新增：统一获取（缓存 → hint → get_input_entity 严格）=====
async def get_or_resolve_peer(
    client: TelegramClient,
    target_key: str,
    uid: int,
    peer_hint: t.Type | None = None,
):
    p = await get_cached_peer(target_key, uid)
    if p:
        return p
    if isinstance(peer_hint, (t.InputPeerChannel, t.InputPeerChat, t.InputPeerUser)):
        await cache_peer(target_key, peer_hint, uid)
        return peer_hint
    ent = await get_input_entity_strict(client, target_key)
    await cache_peer(target_key, ent, uid)
    return ent