# -*- coding: utf-8 -*-
# core/registry_guard.py
from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Optional, Set
from telethon import TelegramClient
from unified.logger import log_exception, log_debug, log_warning

# —— 全局标记：防重复注册 ——
_CLIENT_MARK: Set[int] = set()         # 哪些 client 做过整体注册
_COMMAND_NS: Dict[int, Set[str]] = {}  # 每个 client 下哪些“命名空间”命令已注册
_CALLBACK_NS: Dict[int, Set[str]] = {} # 每个 client 下哪些“命名空间”回调已注册
_EVENT_KEYS: Dict[int, Set[str]] = {}  # 每个 client 下已注册的事件 key 集
_LOCK = threading.RLock()

def mark_client_registered(client: TelegramClient) -> bool:
    cid = id(client)
    with _LOCK:
        if cid in _CLIENT_MARK:
            log_warning("register_all: 本 client 已注册过（幂等跳过）")
            return False
        _CLIENT_MARK.add(cid)
    log_debug("register_all: 首次为该 client 执行注册")
    return True

def mark_commands_registered(client: TelegramClient, namespace: str) -> bool:
    cid = id(client)
    with _LOCK:
        bucket = _COMMAND_NS.setdefault(cid, set())
        if namespace in bucket:
            log_warning(f"命令命名空间已注册过：{namespace}（幂等跳过）")
            return False
        bucket.add(namespace)
    log_debug(f"命令命名空间已标记：{namespace}")
    return True

def mark_callbacks_registered(client: TelegramClient, namespace: str) -> bool:
    cid = id(client)
    with _LOCK:
        bucket = _CALLBACK_NS.setdefault(cid, set())
        if namespace in bucket:
            log_warning(f"回调命名空间已注册过：{namespace}（幂等跳过）")
            return False
        bucket.add(namespace)
    log_debug(f"回调命名空间已标记：{namespace}")
    return True

def _build_event_key(handler: Any, builder: Any, key: Optional[str]) -> str:
    if key:
        return key
    h_mod = getattr(handler, "__module__", "")
    h_name = getattr(handler, "__name__", repr(handler))
    b_cls = getattr(builder, "__class__", type(builder)).__name__
    return f"{h_mod}.{h_name}|{b_cls}"

def safe_add_event_handler(
    client: TelegramClient,
    handler: Callable[..., Any],
    builder: Any,
    *,
    key: Optional[str] = None,
    tag: Optional[str] = None,
    **_: Any,
) -> bool:
    """
    幂等注册 Telethon 事件处理器：
      - 同一 client + key 只会注册一次
      - 兼容 `tag=`（等价于 key）
    返回 True 表示本次完成注册；False 表示已存在、跳过。
    """
    try:
        cid = id(client)
        the_key = (tag or key) or _build_event_key(handler, builder, None)
        with _LOCK:
            bucket = _EVENT_KEYS.setdefault(cid, set())
            if the_key in bucket:
                log_warning("事件已注册，跳过", extra={"key": the_key})
                return False
            # 把 add_event_handler 放到锁内可以避免极端并发下重复注册
            client.add_event_handler(handler, builder)
            bucket.add(the_key)
        return True
    except Exception as e:
        log_exception("safe_add_event_handler 注册失败", exc=e)
        return False
