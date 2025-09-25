# unified/emoji_store.py 
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import os
from typing import Dict, List, Optional

from telethon import TelegramClient

__all__ = [
    "EmojiStore",
    "get_shared_emoji_store",
    "set_shared_emoji_store",
    "ensure_emoji_store_loaded",
]

_DEFAULT_CACHE_PATH = os.getenv("EMOJI_CACHE_PATH", "data/emoji_cache.json")


class EmojiStore:
    """
    自定义表情映射缓存
    id_to_emoji: document_id -> "😎"
    name_to_id : "😎" -> document_id
    """

    def __init__(self, cache_path: str = _DEFAULT_CACHE_PATH):
        self.id_to_emoji: Dict[int, str] = {}
        self.name_to_id: Dict[str, int] = {}
        self.loaded: bool = False
        self.cache_path = cache_path

    async def load_from_client(
        self, client: TelegramClient, save_cache: bool = True
    ) -> None:
        stickers = await client.get_custom_emoji_stickers()
        for s in stickers:
            try:
                if hasattr(s, "document") and hasattr(s, "alt"):
                    doc_id = int(s.document.id)
                    emoji_char = s.alt or ""
                    if emoji_char:
                        self.id_to_emoji[doc_id] = emoji_char
                        self.name_to_id[emoji_char] = doc_id
            except Exception:
                # 单条异常不影响整体
                continue
        self.loaded = True
        if save_cache:
            self.save_to_file()

    def save_to_file(self, path: Optional[str] = None) -> None:
        path = path or self.cache_path
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {str(k): v for k, v in self.id_to_emoji.items()},
                    f,
                    ensure_ascii=False,
                )
        except Exception:
            # 持久化失败不应中断主流程
            pass

    def load_from_file(self, path: Optional[str] = None) -> None:
        path = path or self.cache_path
        try:
            if not os.path.exists(path):
                return
            with open(path, "r", encoding="utf-8") as f:
                raw: Dict[str, str] = json.load(f)
            # 还原映射
            self.id_to_emoji = {int(k): v for k, v in (raw or {}).items() if v}
            self.name_to_id = {v: int(k) for k, v in self.id_to_emoji.items()}
            self.loaded = True if self.id_to_emoji else False
        except Exception:
            # 读取失败不致命
            pass

    # --------- 查询工具 --------- #
    def get_emoji(self, doc_id: int) -> Optional[str]:
        return self.id_to_emoji.get(doc_id)

    def get_id(self, emoji_char: str) -> Optional[int]:
        return self.name_to_id.get(emoji_char)

    def get_bulk_emojis(self, ids: List[int]) -> List[str]:
        return [self.id_to_emoji.get(i, "") for i in ids]

    def get_visible_suffix(self, emoji_ids: List[int]) -> str:
        vis = " ".join(ch for ch in self.get_bulk_emojis(emoji_ids) if ch)
        return ("\n\n" + vis) if vis else ""

    def get_html_fallback_suffix(self, emoji_ids: List[int]) -> str:
        tags: List[str] = []
        for eid in emoji_ids:
            ch = self.get_emoji(eid)
            if ch:
                tags.append(f'<tg-emoji emoji-id="{eid}">{ch}</tg-emoji>')
        return ("<br><br>" + " ".join(tags)) if tags else ""


# ---------------- 单例与懒加载封装（供全局调用） ---------------- #

_shared_store: Optional[EmojiStore] = None
_shared_lock = asyncio.Lock()


def set_shared_emoji_store(store: EmojiStore) -> None:
    """允许外部替换单例（测试/自定义路径）。"""
    global _shared_store
    _shared_store = store


def get_shared_emoji_store(cache_path: Optional[str] = None) -> EmojiStore:
    """
    返回单例 EmojiStore（未必已经加载映射；配合 ensure_emoji_store_loaded 使用）。
    """
    global _shared_store
    if _shared_store is None:
        _shared_store = EmojiStore(cache_path or _DEFAULT_CACHE_PATH)
        # 先尝试从磁盘预加载（不阻塞网络）
        _shared_store.load_from_file()
    return _shared_store


async def ensure_emoji_store_loaded(client: Optional[TelegramClient]) -> EmojiStore:
    """
    确保单例已加载映射：
      - 若磁盘已有缓存且 loaded=True，直接返回
      - 否则在拿到 client 时，去远端拉一次并落盘
      - 多协程并发时用锁防抖
    """
    store = get_shared_emoji_store()
    if store.loaded:
        return store
    if client is None:
        # 没有 client 也先返回（调用方后续可再次调用确保加载）
        return store
    async with _shared_lock:
        if not store.loaded:
            store.load_from_file()
            if not store.loaded:
                await store.load_from_client(client, save_cache=True)
    return store
