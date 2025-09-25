# typess/ttl_types.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Tuple

def _intval(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _floatval(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _csv_to_ints(name: str, default: Tuple[int, ...]) -> Tuple[int, ...]:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        arr = [int(x.strip()) for x in raw.split(",") if x.strip()]
        return tuple([x for x in arr if x >= 0]) or default
    except Exception:
        return default
@dataclass(frozen=True)
class TTLConfig:
    # —— Redis/锁/缓存 —— #
    SENDLOCK_SECONDS: int              = _intval("TTL_SENDLOCK_SECONDS", 300)         # tg:sendlock
    ENTITY_CACHE_TTL_SECONDS: int      = _intval("TTL_ENTITY_CACHE_TTL_SECONDS", 86400)
    PEER_CACHE_TTL_SECONDS: int        = _intval("TTL_PEER_CACHE_TTL_SECONDS", 604800)  # 7d
    BLACKLIST_TTL_SECONDS: int         = _intval("TTL_BLACKLIST_TTL_SECONDS", 7 * 86400)

    # —— 业务相关 TTL（供 FSM/策略层使用） —— #
    PAYMENT_BLOCK_SECONDS: int       = _intval("TTL_PAYMENT_BLOCK_SECONDS", 24 * 3600)
    USERNAME_INVALID_SECONDS: int    = _intval("TTL_USERNAME_INVALID_SECONDS", 24 * 3600)
    WRITE_FORBIDDEN_HOURS: int       = _intval("TTL_WRITE_FORBIDDEN_HOURS", 24)
    BANNED_IN_CHANNEL_HOURS: int     = _intval("TTL_BANNED_IN_CHANNEL_HOURS", 24)
    
    # —— Flood/Slowmode 附加 padding —— #
    SLOWMODE_PAD_SECONDS: int          = _intval("TTL_SLOWMODE_PAD_SECONDS", 2)
    FLOOD_PAD_SECONDS: int             = _intval("TTL_FLOOD_PAD_SECONDS", 5)

    # —— 解析/重试 —— #
    RESOLVE_RETRY_SECONDS: int         = _intval("TTL_RESOLVE_RETRY_SECONDS", 3)
    RESOLVE_RETRY_MAX: int             = _intval("TTL_RESOLVE_RETRY_MAX", 2)

    # —— 发送重试/退避 —— #
    SENDER_MAX_RETRIES: int            = _intval("TTL_SENDER_MAX_RETRIES", 3)
    RETRY_BASE_DELAY_SECONDS: float    = _floatval("TTL_RETRY_BASE_DELAY_SECONDS", 2.0)
    RETRY_BACKOFF_FACTOR: float        = _floatval("TTL_RETRY_BACKOFF_FACTOR", 1.7)
    RETRY_MAX_DELAY_SECONDS: float     = _floatval("TTL_RETRY_MAX_DELAY_SECONDS", 90.0)
    SEND_TIMEOUT_SECONDS: int          = _intval("TTL_SEND_TIMEOUT_SECONDS", 25)

    # —— 入群/预热 —— #
    DIALOGS_WARMUP_LIMIT: int          = _intval("TTL_DIALOGS_WARMUP_LIMIT", 200)
    DIALOGS_WARMUP_CONCURRENCY: int    = _intval("TTL_DIALOGS_WARMUP_CONCURRENCY", 8)

    # —— 其他（按需扩展） —— #
    JOIN_INTERVAL_BASE_SECONDS: int    = _intval("TTL_JOIN_INTERVAL_BASE_SECONDS", 30)
    JOIN_INTERVAL_JITTER_SECONDS: int  = _intval("TTL_JOIN_INTERVAL_JITTER_SECONDS", 5)
    # ✅ 账号内串行 JOIN 冷却序列（单位秒；仅“真正触发 join/import”时才计入节奏）
    # 默认：第1次3s，第2次30s，第3次90s，第4次300s，第5次600s，之后900s
    JOIN_COOLDOWN_SEQUENCE: Tuple[int, ...] = _csv_to_ints(
        "TTL_JOIN_COOLDOWN_SEQUENCE", (3, 30, 90, 300, 600, 900)
    )

    # 慢速模式：未给出 seconds 时的默认等待
    SLOWMODE_DEFAULT_SECONDS: int     = _intval("STRAT_SLOWMODE_DEFAULT_SECONDS", 10)
    # PeerFlood（对单个 phone+chat 的冷却小时数）
    PEER_FLOOD_COOLDOWN_HOURS: int    = _intval("STRAT_PEER_FLOOD_COOLDOWN_HOURS", 12)
    # 入群数量过多触发时，对该账号的冷却小时数
    CHANNELS_TOO_MUCH_HOURS: int      = _intval("STRAT_CHANNELS_TOO_MUCH_HOURS", 6)
    
TTL = TTLConfig()

def next_join_cooldown(ttl: TTLConfig, join_attempt_index: int) -> int:
    if join_attempt_index <= 0:
        return 0
    seq = ttl.JOIN_COOLDOWN_SEQUENCE
    idx = min(join_attempt_index - 1, len(seq) - 1)
    return seq[idx]
