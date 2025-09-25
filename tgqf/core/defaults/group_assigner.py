# -*- coding: utf-8 -*-
# core/defaults/group_assigner.py
"""
分配与入群执行（并发统一由 TaskExecutor 控制）：
- 账号并行、账号内串行；
- 并发度通过 max_parallel_accounts 控制；
- INVITE 链接 smart_join(skip_check=True)；
- INVITE 超时 35s，其它 20s；
- 统一上下文字段：trace_id / group / me_phone / bot_name
- 命中永久无效场景 → 加入黑名单；申请制 → 加入 pending 集合
- 已加入/已在群 → 回写 Peer 缓存（便于后续快速解析）
"""
from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Any, Set, Sequence
import asyncio
import random
import uuid
import time
import json

from telethon import TelegramClient

from unified.trace_context import (
    inject_trace_context,
    set_log_context,
    get_log_context,
    with_trace,
    ctx_create_task,
)
from unified.logger import (
    log_info,
    log_warning,
    log_debug,
    log_exception,
)

from typess.link_types import ParsedLink
from tg.group_utils import smart_join
from tg.utils_username import display_name_from_me, username_from_me
from unified.config import (
    ASSIGN_TIMEOUT_DEFAULT_SECONDS,
    ASSIGN_TIMEOUT_INVITE_SECONDS,
    JOINED_USERNAMES_TTL_SECONDS,
    MAX_CONCURRENCY,
    REASSIGN_JITTER_MIN,
    REASSIGN_JITTER_MAX,
    CLIENT_CONNECT_TIMEOUT,  # ✅ 统一 get_me 超时
)
from core.task_status_reporter import TaskStatusReporter, K_PENDING_REASSIGN
from typess.join_status import normalize_join_result
from scheduler.ratelimit import FloodController
from core.telethon_errors import StrategyContext, handle_send_error
from tg.entity_utils import cache_peer
from typess.fsm_keys import K_BLACKLIST_TARGETS
from unified.context import get_redis

__all__ = ["BalancedGroupAssigner"]

# =====================
# 阶梯延时配置（统一管理）
# =====================
# 默认阶梯：
#  1次后 2.5~5.5s，2→3:30s，3→4:90s，4→5:300s，5→6:600s，6→7:900s，后续固定 900s
_DEFAULT_STAIR_TABLE: List[float | Tuple[float, float]] = [
    (2.5, 5.5), 30, 90, 300, 600, 900
]

# 允许通过 unified.config 提供同名常量 JOIN_STAIR_DELAYS（list[float|tuple]）覆盖
try:
    from unified.config import JOIN_STAIR_DELAYS as _CFG_STAIR
    STAIR_TABLE = list(_CFG_STAIR) if isinstance(_CFG_STAIR, (list, tuple)) and _CFG_STAIR else list(_DEFAULT_STAIR_TABLE)
except Exception:
    STAIR_TABLE = list(_DEFAULT_STAIR_TABLE)


def _stair_delay(attempt_index: int) -> float:
    if attempt_index <= 0:
        return 0.0
    spec = STAIR_TABLE[attempt_index - 1] if (attempt_index - 1) < len(STAIR_TABLE) else STAIR_TABLE[-1]
    if isinstance(spec, (list, tuple)) and len(spec) >= 2:
        lo, hi = float(spec[0]), float(spec[1])
        if hi < lo: lo, hi = hi, lo
        return random.uniform(max(0.0, lo), max(0.0, hi))
    return max(0.0, float(spec))


async def _load_joined_usernames_from_cache(user_id: int, phone: str):
    redis = get_redis()
    if not redis:
        return None
    key = f"joined_usernames:{user_id}:{phone}"
    try:
        s = await redis.get(key)
        if s:
            return set(json.loads(s))
    except Exception as e:
        log_warning(f"joined_usernames cache 读取失败: {e}")
    return None


async def _save_joined_usernames_to_cache(user_id: int, phone: str, usernames: Set[str]) -> None:
    redis = get_redis()
    if not redis:
        return
    key = f"joined_usernames:{user_id}:{phone}"
    try:
        await redis.set(key, json.dumps(sorted(usernames)), ex=JOINED_USERNAMES_TTL_SECONDS)
    except Exception as e:
        log_warning(f"joined_usernames cache 写入失败: {e}")


async def _collect_joined_usernames(client: TelegramClient) -> Set[str]:
    result: Set[str] = set()
    try:
        async for dlg in client.iter_dialogs(limit=2000):
            ent = dlg.entity
            uname = getattr(ent, "username", None)
            if uname:
                result.add(str(uname).lower())
    except Exception as e:
        log_warning("collect_joined_usernames 失败", extra={"err": str(e)})
    return result


class BalancedGroupAssigner:
    def __init__(self, fsm=None) -> None:
        self._last_assignments: Dict[str, List[ParsedLink]] = {}
        self._last_joined: Dict[str, List[ParsedLink]] = {}
        self._last_already: Dict[str, List[ParsedLink]] = {}
        self._state_lock = asyncio.Lock()
        self._last_run_id: Optional[str] = None
        self.fsm = fsm

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _elapsed_ms(ts_start_ms: int) -> int:
        return max(0, BalancedGroupAssigner._now_ms() - ts_start_ms)

    async def load_joined_usernames(self, user_id: int, phone: str, client: TelegramClient) -> Set[str]:
        cached = await _load_joined_usernames_from_cache(user_id, phone)
        if cached is not None:
            return cached
        usernames = await _collect_joined_usernames(client)
        await _save_joined_usernames_to_cache(user_id, phone, usernames)
        return usernames

    async def build_assignment(
        self,
        user_id: int,
        phone_to_client: Dict[str, TelegramClient],
        targets: Sequence[ParsedLink],
    ) -> Dict[str, List[ParsedLink]]:
        joined_map: Dict[str, Set[str]] = {}
        for phone, client in phone_to_client.items():
            try:
                joined_map[phone] = await self.load_joined_usernames(user_id, phone, client)
            except Exception as e:
                log_warning("load_joined_usernames 失败，降级为空集", extra={"phone": phone, "err": str(e)})
                joined_map[phone] = set()

        assignment: Dict[str, List[ParsedLink]] = {p: [] for p in phone_to_client.keys()}
        remaining: List[ParsedLink] = []
        for pl in targets:
            try:
                uname = (pl.username or "").lower()
                is_public = bool(uname)
                if is_public:
                    for phone, jset in joined_map.items():
                        if uname in jset:
                            assignment[phone].append(pl)
                            break
                    else:
                        remaining.append(pl)
                else:
                    remaining.append(pl)
            except Exception:
                remaining.append(pl)

        if remaining:
            phones = list(phone_to_client.keys())
            random.shuffle(remaining)
            i = 0
            for pl in remaining:
                assignment[phones[i % len(phones)]].append(pl)
                i += 1

        return {p: lst for p, lst in assignment.items() if lst}

    async def assign(
        self,
        user_id: int,
        clients: Dict[str, TelegramClient],
        groups: List[ParsedLink],
    ) -> Dict[str, List[ParsedLink]]:
        ctx = get_log_context() or {}
        set_log_context({**ctx, "user_id": user_id})
        if not clients or not groups:
            return {}
        try:
            plan = await self.build_assignment(user_id, clients, groups)
            log_info(
                "assign.summary",
                extra={
                    "user_id": user_id,
                    "phones_total": len(clients),
                    "groups_total": len(groups),
                    "assigned": {p: len(v) for p, v in plan.items()},
                },
            )
            return plan
        except Exception as e:
            log_exception("assign 失败", exc=e, extra={"user_id": user_id})
            return {}

    @with_trace(action_name="execute_assignment", phase="start")
    async def execute_assignment(
        self,
        user_id: int,
        clients: Dict[str, TelegramClient],
        assignment: Dict[str, List[ParsedLink]],
        event: Optional[Any] = None,
        reporter: Optional[TaskStatusReporter] = None,
        *,
        max_parallel_accounts: Optional[int] = None,
        to_user_id: Optional[int] = None,
        notify_summary: bool = True,
        flood: Optional[FloodController] = None,
    ) -> Dict[str, List[ParsedLink]]:
        ts_start = int(time.time() * 1000)
        run_id = str(uuid.uuid4())
        inject_trace_context("assign_group", user_id=user_id)
        recv_uid = int(to_user_id or user_id)

        if reporter and notify_summary:
            try:
                reporter.set_user(user_id)
            except Exception:
                pass

        if not assignment:
            log_warning("execute_assignment: assignment 为空，直接返回", extra={"user_id": user_id})
            return {}

        ctx = get_log_context() or {}
        trace_id = ctx.get("trace_id")
        set_log_context({**ctx, "user_id": user_id, "run_id": run_id})

        log_info(
            "🚀 开始执行入群",
            extra={
                "user_id": user_id,
                "run_id": run_id,
                "trace_id": trace_id,
                "assignment_count": len(assignment),
            },
        )

        # 轻微扰动，避免集中启动
        await asyncio.sleep(random.uniform(REASSIGN_JITTER_MIN, REASSIGN_JITTER_MAX))

        parallel = max(1, int(max_parallel_accounts or MAX_CONCURRENCY))
        sem = asyncio.Semaphore(parallel)

        joined_map: Dict[str, List[ParsedLink]] = {}
        already_map: Dict[str, List[ParsedLink]] = {}
        failures_map: Dict[str, List[Tuple[ParsedLink, str]]] = {}

        async def _worker(phone_key: str, links: List[ParsedLink]) -> Tuple[str, List[ParsedLink], List[ParsedLink], List[Tuple[ParsedLink, str]]]:
            local_joined: List[ParsedLink] = []
            local_already: List[ParsedLink] = []
            local_failures: List[Tuple[ParsedLink, str]] = []
            local_pending: List[ParsedLink] = []

            async with sem:
                client = clients.get(phone_key)
                if not client:
                    log_warning("worker: client missing for phone", extra={"user_id": user_id, "me_phone": phone_key})
                    return phone_key, local_joined, local_already, local_failures

                # 细微抖动：进一步分散启动尖峰
                try:
                    await asyncio.sleep(random.uniform(0.0, 0.3))
                except asyncio.CancelledError:
                    raise

                # get_me 加统一超时兜底，避免卡住一个并发槽位
                try:
                    me = await asyncio.wait_for(client.get_me(), timeout=CLIENT_CONNECT_TIMEOUT)
                except asyncio.TimeoutError:
                    log_warning("获取账号信息超时（跳过本账号本轮）", extra={"user_id": user_id, "me_phone": phone_key})
                    return phone_key, local_joined, local_already, local_failures
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log_exception("获取账号信息失败", exc=e, extra={"phone": phone_key, "user_id": user_id})
                    return phone_key, local_joined, local_already, local_failures

                name = display_name_from_me(me)
                username = username_from_me(me, with_at=True) or ""

                # 邀请优先
                ordered = sorted(links or [], key=lambda x: (not x.is_invite(), x.username or ""))

                # —— 阶梯延时状态 —— #
                attempt_idx = 0
                for pl in ordered:
                    timeout = ASSIGN_TIMEOUT_INVITE_SECONDS if pl.is_invite() else ASSIGN_TIMEOUT_DEFAULT_SECONDS
                    ctx_now = get_log_context() or {}
                    current_trace = ctx_now.get("trace_id") or trace_id or str(uuid.uuid4())
                    set_log_context({
                        "trace_id": current_trace,
                        "user_id": user_id,
                        "me_phone": phone_key,
                        "group": pl.to_link(),
                        "chat_key": pl.cache_key(),
                        "run_id": run_id,
                    })

                    # —— 阶梯延时：除首次外，在本次尝试前等待 —— #
                    if attempt_idx >= 1:
                        delay_sec = _stair_delay(attempt_idx)
                        if delay_sec > 0:
                            try:
                                await asyncio.sleep(delay_sec)
                            except asyncio.CancelledError:
                                raise

                    # —— 入群尝试 —— #
                    ok, reason, peer_obj = False, "", None
                    try:
                        res = await asyncio.wait_for(
                            smart_join(client, pl, skip_check=pl.is_invite()),
                            timeout=timeout,
                        )
                        if isinstance(res, (list, tuple)):
                            ok = bool(res[0])
                            reason = str(res[1] or "")
                            peer_obj = res[2] if len(res) >= 3 else None
                        else:
                            ok, reason = bool(res), ""
                    except asyncio.TimeoutError:
                        log_warning("入群超时", extra={
                            "user_id": user_id, "me_phone": phone_key,
                            "group": pl.to_link(), "chat_key": pl.cache_key()
                        })
                        local_failures.append((pl, "timeout"))
                        if reporter:
                            try:
                                reporter.track_result(
                                    {"user_id": user_id, "phone": phone_key, "name": name, "username": username,
                                     "group": pl.to_link(), "code": "timeout", "reason": "timeout"}
                                )
                            except Exception:
                                log_warning("reporter.track_result on timeout 失败（忽略）", extra={"user_id": user_id, "me_phone": phone_key})
                        attempt_idx += 1
                        continue  # 下一条

                    except Exception as e:
                        log_exception("smart_join 调用异常", exc=e, extra={
                            "user_id": user_id, "me_phone": phone_key,
                            "group": pl.to_link(), "chat_key": pl.cache_key()
                        })
                        ok, reason = False, str(e)

                    # 统一 code
                    try:
                        nr = normalize_join_result(ok, reason)
                        code_val = getattr(nr, "code", nr)
                        code_str = (code_val.value if hasattr(code_val, "value") else str(code_val)).lower().strip()
                    except Exception:
                        code_str = "success" if ok else "failure"
                    if ok and code_str not in {"success", "already_in", "pending"}:
                        code_str = "already_in" if ("already" in reason.lower() or "已在" in reason) else "success"

                    # 可见化每条入群结果（方便排障）
                    log_info("join_attempt_result", extra={
                        "user_id": user_id, "me_phone": phone_key, "group": pl.to_link(),
                        "code": code_str, "reason": reason
                    })

                    if reporter:
                        try:
                            reporter.track_result(
                                {"user_id": user_id, "phone": phone_key, "name": name, "username": username,
                                 "group": pl.to_link(), "code": code_str, "reason": reason}
                            )
                        except Exception:
                            log_warning("reporter.track_result 失败（忽略）", extra={"user_id": user_id, "me_phone": phone_key})

                    if (code_str in {"success", "already_in"}) and peer_obj is not None:
                        try:
                            await cache_peer(pl.cache_key(), peer_obj, user_id)
                        except Exception:
                            pass

                    PERMANENT_CODES = {
                        "skip_invalid_link", "username_not_occupied", "invite_invalid",
                        "invite_expired", "invite_hash_expired", "channel_private",
                    }
                    if (not ok) and (code_str in PERMANENT_CODES):
                        redis = get_redis()
                        if redis:
                            try:
                                await redis.sadd(K_BLACKLIST_TARGETS.format(uid=user_id), pl.cache_key())
                            except Exception:
                                pass

                    if code_str == "pending":
                        local_pending.append(pl)
                        redis = get_redis()
                        if redis:
                            try:
                                await redis.sadd(K_PENDING_REASSIGN.format(uid=user_id), pl.cache_key())
                            except Exception:
                                pass

                    # 策略回调
                    try:
                        ctx2 = StrategyContext(
                            fsm=self.fsm, flood=flood, user_id=user_id, phone=phone_key,
                            chat_key=pl.cache_key(), reporter=reporter, scheduler=None, reassigner=None
                        )
                        if code_str not in {"success", "already_in"}:
                            await handle_send_error(code_str, {}, ctx2)
                    except Exception:
                        pass

                    if code_str == "success":
                        local_joined.append(pl)
                    elif code_str == "already_in":
                        local_already.append(pl)
                    elif code_str == "pending":
                        pass
                    else:
                        local_failures.append((pl, reason or ""))

                    # —— 仅“真实 join/import”才推进节流阶梯 —— #
                    # 约定：ALREADY_IN 不算真实 join；其余（成功/失败/受限/无效/超时）都算一次尝试
                    attempted_join = (code_str != "already_in")

                    if attempted_join:
                        attempt_idx = 1
                        # 后置阶梯延时：只影响下一次循环，且仅在确有 join/import 后触发
                        delay_sec = _stair_delay(attempt_idx)
                        if delay_sec > 0:
                            try:
                                await asyncio.sleep(delay_sec)
                            except asyncio.CancelledError:
                                raise

            return phone_key, local_joined, local_already, local_failures

        tasks = [ctx_create_task(_worker, p, l) for p, l in assignment.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                log_exception("worker 发生未捕获异常", exc=res, extra={"user_id": user_id, "run_id": run_id})
                continue
            try:
                phone_key, local_joined, local_already, local_failures = res
                if local_joined:
                    joined_map.setdefault(phone_key, []).extend(local_joined)
                if local_already:
                    already_map.setdefault(phone_key, []).extend(local_already)
                if local_failures:
                    failures_map.setdefault(phone_key, []).extend(local_failures)
                log_debug(
                    "worker 完成",
                    extra={
                        "user_id": user_id,
                        "me_phone": phone_key,
                        "joined": len(local_joined),
                        "already": len(local_already),
                        "failures": len(local_failures),
                    },
                )
            except Exception as e:
                log_exception("处理 worker 返回值失败", exc=e, extra={"user_id": user_id, "res": str(res)})

        self._last_joined = joined_map
        self._last_already = already_map
        self._last_run_id = run_id
        self._last_assignments = assignment

        if reporter and notify_summary:
            try:
                scope_links: List[str] = []
                for _p, _lst in (assignment or {}).items():
                    for _pl in (_lst or []):
                        try:
                            scope_links.append(_pl.to_link())
                        except Exception:
                            continue
                scope_links = list({s for s in scope_links if s})

                await reporter.notify_group_join_summary(
                    user_id,
                    reporter.results_by_user.get(user_id, []),
                    to_user_id=recv_uid,
                    target_groups_total=len(scope_links),
                    accounts_total=len(assignment or {}),
                    groups_scope=scope_links,
                )
            except Exception as e:
                log_exception("notify_group_join_summary 失败", exc=e, extra={"user_id": user_id})

        log_info(
            "🏁 入群执行完成",
            extra={
                "user_id": user_id,
                "elapsed_ms": self._elapsed_ms(ts_start),
                "groups_joined": sum(len(v) for v in joined_map.values()),
                "already_count": sum(len(v) for v in already_map.values()),
                "failures_total": sum(len(v) for v in failures_map.values()),
            },
        )
        try:
            from collections import Counter
            reasons = [r for pairs in failures_map.values() for _, r in pairs]
            top = Counter([str(r or "").strip() for r in reasons if r]).most_common(5)
            if top:
                log_debug("入群失败原因 Top", extra={"user_id": user_id, "top": top})
        except Exception:
            pass

        sendable_map: Dict[str, List[ParsedLink]] = {}
        for phone in assignment.keys():
            items: List[ParsedLink] = []
            if phone in joined_map:
                items.extend(joined_map[phone])
            if phone in already_map:
                items.extend(already_map[phone])
            if items:
                sendable_map[phone] = items

        return sendable_map
