# -*- coding: utf-8 -*-
# unified/selection.py
from __future__ import annotations

import random
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from telethon import TelegramClient

from unified.logger import log_info, log_warning, log_exception, log_debug
from unified.trace_context import generate_trace_id, set_log_context, get_log_context
from typess.health_types import HealthState

__all__ = [
    "select_clients_by_health",
    "unified_prepare_and_select_clients",
]

# ---------------- 健康态优先级（数值越小优先级越高） ----------------
# 说明：
#  1) OK 绝对优先
#  1.5) LIMITED 可用但降级（受限/慢速/写入受限等），优先级仅次于 OK
#  2) UNKNOWN 作为回退（很多场景首次加载/未探测）
#  3) NETWORK_ERROR 次之（偶发/临时）
#  4) FLOOD_WAIT 再后（短时不可用；通常不用于首选）
#  5) AUTH_EXPIRED / BANNED 均视为不可用
_HEALTH_RANK = {
    HealthState.OK: 0,
    HealthState.LIMITED: 1,
    HealthState.UNKNOWN: 2,
    HealthState.NETWORK_ERROR: 3,
    HealthState.FLOOD_WAIT: 4,
    HealthState.AUTH_EXPIRED: 90,
    HealthState.BANNED: 99,
}
_BAD_STATES = {HealthState.AUTH_EXPIRED, HealthState.BANNED}

def _rank_of(state: Any) -> int:
    try:
        s = HealthState.parse(state)
    except Exception:
        s = HealthState.UNKNOWN
    return _HEALTH_RANK.get(s, 50)

def _safe_is_connected(cli: Optional[TelegramClient]) -> bool:
    try:
        return bool(cli and cli.is_connected())
    except Exception:
        return False

def _get_state_any(cm, user_id: int, phone: str) -> HealthState:
    """
    同时兼容：
      - get_health_state(user_id, phone)
      - get_health_state(phone)
      - get_health_state_legacy(phone)
    优先返回第一个非 UNKNOWN 的状态。
    """
    # 新签名
    try:
        st = cm.get_health_state(user_id, phone)  # type: ignore
        st = HealthState.parse(st)
        if st != HealthState.UNKNOWN:
            return st
    except TypeError:
        pass
    except Exception:
        pass

    # 旧签名（仅 phone）
    for meth in ("get_health_state", "get_health_state_legacy"):
        fn = getattr(cm, meth, None)
        if callable(fn):
            try:
                st = fn(phone)  # type: ignore
                st = HealthState.parse(st)
                if st != HealthState.UNKNOWN:
                    return st
            except Exception:
                continue

    # 都拿不到 → UNKNOWN
    return HealthState.UNKNOWN

# ---------------- 选择策略 ----------------
def select_clients_by_health(
    user_id: int,
    clients: Dict[str, TelegramClient],
    cm,
    *,
    prefer_ok: bool = True,
    allow_unknown_fallback: bool = True,
    shuffle_within_tier: bool = True,
    exclude_bad: bool = True,
    max_pick: Optional[int] = None,
) -> Dict[str, TelegramClient]:
    """
    根据健康态为该用户筛选/排序账号（返回按优先级有序的 dict）。
    - prefer_ok=True：OK 优先级最高（默认）
    - allow_unknown_fallback=True：没有 OK 时允许 UNKNOWN 回退
    - exclude_bad=True：剔除 AUTH_EXPIRED/BANNED
    - shuffle_within_tier=True：同优先级内做一次洗牌，避免总是命中同一批
    - max_pick：限制最终返回的账号数量（None/<=0 表示不限制）
    """
    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "func": "select_clients_by_health", "trace_id": trace_id})

    if not clients:
        log_debug("select_clients_by_health: 输入为空", extra=get_log_context()); return {}

    scored: List[Tuple[int, int, str, TelegramClient, HealthState]] = []  # (rank, conn_penalty, phone, client, state)

    ok_count = unknown_count = limited_count = flood_count = dropped_bad = 0

    for phone, cli in list(clients.items()):
        st = _get_state_any(cm, user_id, phone)
        if exclude_bad and st in _BAD_STATES:
            dropped_bad += 1
            continue
        r = _rank_of(st)
        # 连接性作为轻微加权：断连给 +1 惩罚，已连给 0
        conn_penalty = 0 if _safe_is_connected(cli) else 1
        scored.append((r, conn_penalty, phone, cli, st))
        if st == HealthState.OK:
            ok_count += 1
        if st == HealthState.LIMITED:
            limited_count += 1
        if st == HealthState.UNKNOWN:
            unknown_count += 1
        if st == HealthState.FLOOD_WAIT:
            flood_count += 1

    # ← 修复：分桶必须在收集完 scored 之后
    buckets: Dict[int, List[Tuple[int, int, str, TelegramClient, HealthState]]] = {}
    for row in scored:
        buckets.setdefault(row[0], []).append(row)


    ordered_rows: List[Tuple[int, int, str, TelegramClient, HealthState]] = []
    for rank in sorted(buckets.keys()):
        tier = buckets[rank]
        if shuffle_within_tier:
            random.shuffle(tier)
        # 同 tier 内再按 conn_penalty 升序（已连优先）
        tier.sort(key=lambda x: x[1])
        ordered_rows.extend(tier)

    # 可选：只取前 max_pick 个（控制并发/资源）
    if isinstance(max_pick, int) and max_pick > 0:
        ordered_rows = ordered_rows[:max_pick]

    # 构建有序字典返回（phone -> client）
    selected: "OrderedDict[str, TelegramClient]" = OrderedDict()
    for _rank, _pen, phone, cli, _st in ordered_rows:
        selected[phone] = cli

    log_debug(
        "select_clients_by_health: summary",
        extra={
            "user_id": user_id,
            "ok": ok_count, "limited": limited_count, "unknown": unknown_count, "flood": flood_count,
            "dropped_bad": dropped_bad, "picked": len(selected),
        },
    )
    log_info(
        "select_clients_by_health: 排序完成",
        extra={
            "user_id": user_id,
            "total_in": len(clients),
            "total_out": len(selected),
            "ok": ok_count,
            "limited": limited_count,
            "unknown": unknown_count,
            "flood_wait": flood_count,
            "dropped_bad": dropped_bad,
            "top_preview": [f"{p}:{_get_state_any(cm, user_id, p).name}" for p in list(selected.keys())[:8]],
        },
    )

    # 如果不允许 UNKNOWN 回退且全部都是 UNKNOWN，则清空
    if not allow_unknown_fallback:
        all_unknown = all((_get_state_any(cm, user_id, p) == HealthState.UNKNOWN) for p in selected.keys())
        if all_unknown:
            log_warning("select_clients_by_health: 禁止 UNKNOWN 回退且全为 UNKNOWN → 空集", extra=get_log_context())
            return {}

    # 如果 prefer_ok 且存在 OK，则把首段 OK 保持在最前（目前已满足；此处仅作为显式保证）
    if prefer_ok:
        oks = [(p, selected[p]) for p in selected.keys() if _get_state_any(cm, user_id, p) == HealthState.OK]
        if oks:
            # 将 OK 放前面（其它顺序保持）
            others = [(p, selected[p]) for p in selected.keys() if p not in dict(oks)]
            selected = OrderedDict(oks + others)

    return selected


# ---------------- 统一入口（prepare + select） ----------------
async def unified_prepare_and_select_clients(
    user_id: int,
    client_manager,
    *,
    validator=None,
    phones_whitelist: Optional[Iterable[str]] = None,
    deep_scan: bool = True,
    prefer_ok: bool = True,
    allow_unknown_fallback: bool = True,
    exclude_bad: bool = True,
    shuffle_within_tier: bool = True,
    max_pick: Optional[int] = None,
    log_prefix: str = "selection",
) -> Dict[str, TelegramClient]:
    """
    一步到位：
      1) 通过 SessionClientManager.prepare_clients_for_user() 预备账号（含连接/parse_mode/探针）
      2) 基于健康态进行优先级筛选与排序
    返回：{phone: TelegramClient}（按优先级插入顺序构造；Python3.7+ 字典有序）
    """
    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "func": "unified_prepare_and_select_clients", "trace_id": trace_id})

    # prepare：直接复用你贴出来的 SessionClientManager 实现
    try:
        clients: Dict[str, TelegramClient] = await client_manager.prepare_clients_for_user(
            user_id,
            validator=validator,
            log_prefix=f"{log_prefix}.prepare",
            phones_whitelist=list(phones_whitelist) if phones_whitelist else None,
            skip_known_bad=True,     # 避免已知坏会话的无谓尝试
            deep_scan=deep_scan,
        )
    except Exception as e:
        log_exception("prepare_clients_for_user 异常", exc=e, extra={"user_id": user_id})
        clients = {}

    if not clients:
        log_warning("unified_prepare_and_select_clients: 未准备到任何可用账号", extra={"user_id": user_id})
        return {}

    # select：健康态优先
    selected = select_clients_by_health(
        user_id,
        clients,
        client_manager,
        prefer_ok=prefer_ok,
        allow_unknown_fallback=allow_unknown_fallback,
        shuffle_within_tier=shuffle_within_tier,
        exclude_bad=exclude_bad,
        max_pick=max_pick,
    )

    # 额外提示：如果 prepare 返回了但全部被过滤（例如都 BANNED/AUTH_EXPIRED）
    if clients and not selected:
        counts = {}
        for ph in clients.keys():
            st = _get_state_any(client_manager, user_id, ph)
            counts[st.name] = counts.get(st.name, 0) + 1
        log_warning(
            "unified_prepare_and_select_clients: 全部被过滤（BAD/策略导致）",
            extra={"user_id": user_id, "health_counts": counts},
        )

    return selected
