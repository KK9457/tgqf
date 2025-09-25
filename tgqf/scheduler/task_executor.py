# -*- coding: utf-8 -*-
# scheduler/task_executor.py
from __future__ import annotations
from scheduler.runner import Runner
import json, asyncio
from typing import Dict, Optional, Any, List, Iterable, Callable, Awaitable
from telethon import TelegramClient
from core.defaults.message_sender import MessageSender
from core.defaults.group_assigner import BalancedGroupAssigner
from core.task_status_reporter import TaskStatusReporter
from typess.message_enums import SendTask, MessageType
from scheduler.round_executor import RoundExecutor
from unified.trace_context import generate_trace_id, set_log_context, get_log_context, inject_trace_context
from unified.logger import log_info, log_debug, log_warning, log_exception
from tg.link_utils import parse_links_with_interval
from typess.link_types import ParsedLink
from scheduler.ratelimit import FloodController
from unified.message_builders import smart_text_parse, extract_custom_emoji_ids
from unified.config import LINKS_CHECK_MAX_CONCURRENCY, PARSE_LINK_INTERVAL, LOG_SAMPLE_COUNT
from typess.join_status import JoinResult, JoinCode           # ✅ 统一入群结果
from tg.group_utils import ensure_join                        # ✅ 统一入群入口
from core.telethon_errors import classify_telethon_error      # （若存在）用于异常到策略的映射
try:
    from tg import link_utils as _lu
except Exception:
    _lu = None

_CHECK_LINKS_AVAILABILITY = getattr(_lu, "check_links_availability", None) if _lu else None
__all__ = ["TaskExecutor"]

async def _precheck_links_for_assignment(
    user_id: int,
    clients: Dict[str, TelegramClient],
    groups: List[ParsedLink],
    *,
    on_username_invalid: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[list[ParsedLink], list[tuple[ParsedLink, str]]]:
    """
    分配前的并发预检查（公开/邀请都检查），对“全部可用账号”并发执行：
      - 任一账号判断有效 => 视为有效（fail-open）
      - 所有成功检查的账号一致判定失效 => 才标记为失效
      - 若所有账号检查都失败（如全部掉线） => fail-open 放行
    返回：(有效列表, [(失效项, 原因)])。只做过滤，不做任何状态写入。
    """
    if not groups:
        return [], []
    if not clients or not callable(_CHECK_LINKS_AVAILABILITY):
        log_debug("precheck_links_for_assignment: no clients or checker not available, fail-open",
                  extra={"user_id": user_id})
        return list(groups), []

    # 规范化 key（按 to_link）以便合并
    def _key_of(pl: ParsedLink) -> str:
        try:
            return pl.to_link()
        except Exception:
            return str(pl)

    phones = [p for p in (clients or {}).keys() if clients.get(p)]
    if not phones:
        return list(groups), []

    # 并发对每个账号执行检查
    async def _check_with_client(phone: str) -> tuple[set[str], Dict[str, str]] | Exception:
        client = clients.get(phone)
        try:
            valid_links, invalid_pairs = await _CHECK_LINKS_AVAILABILITY(
                client,
                groups,
                max_concurrency=LINKS_CHECK_MAX_CONCURRENCY,
                skip_invite_check=False,  # 邀请也校验
                redis=None,
                user_id=user_id,
                on_username_invalid=on_username_invalid,
            )
            valid_keys = { _key_of(g) for g in valid_links if isinstance(g, ParsedLink) }
            invalid_map: Dict[str, str] = {}
            for g, reason in (invalid_pairs or []):
                if not isinstance(g, ParsedLink):
                    try:
                        g = ParsedLink.auto_parse(str(g))
                    except Exception:
                        pass
                invalid_map[_key_of(g)] = str(reason or "")
            return valid_keys, invalid_map
        except Exception as e:
            log_warning("precheck_links_for_assignment: per-client checker failed (ignored)",
                        extra={"user_id": user_id, "phone": phone, "err": str(e)})
            return e

    tasks = [asyncio.create_task(_check_with_client(p)) for p in phones]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    # 合并：任一账号判定有效即有效；若所有“成功返回”的账号都判无效，才标无效
    union_valid: set[str] = set()
    invalid_votes: Dict[str, int] = {}
    invalid_reason_any: Dict[str, str] = {}
    successful_checks = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        valid_keys, invalid_map = res
        successful_checks += 1
        union_valid |= valid_keys
        for k, r in (invalid_map or {}).items():
            invalid_votes[k] = invalid_votes.get(k, 0) + 1
            if k not in invalid_reason_any and r:
                invalid_reason_any[k] = r

    # 如果一个成功检查都没有，fail-open：全部放行
    if successful_checks == 0:
        log_warning("precheck_links_for_assignment: no successful client checks, fail-open",
                    extra={"user_id": user_id})
        return list(groups), []

    # 构造输出：保序
    ok: list[ParsedLink] = []
    bad: list[tuple[ParsedLink, str]] = []
    for g in groups:
        k = _key_of(g)
        if k in union_valid:
            ok.append(g)
        else:
            votes = invalid_votes.get(k, 0)
            if votes > 0 and votes == successful_checks:
                bad.append((g, invalid_reason_any.get(k, "invalid")))
            else:
                # 有分歧或部分失败 → fail-open 当作有效
                ok.append(g)

    return ok, bad


def _dedup_links_keep_order(groups: List[ParsedLink]) -> List[ParsedLink]:
    """按 to_link() 去重，保序。"""
    seen: set[str] = set()
    out: list[ParsedLink] = []
    for g in groups or []:
        try:
            key = g.to_link()
        except Exception:
            key = str(g)
        if key in seen:
            continue
        seen.add(key)
        out.append(g)
    return out

def _pre_normalize_groups(raw_groups: Iterable[Any]) -> List[str]:
    norm: List[str] = []
    for item in (raw_groups or []):
        try:
            if isinstance(item, dict) and "type" in item and "value" in item:
                pl = ParsedLink.from_dict(item)
                norm.append(pl.to_link()); continue
            if isinstance(item, str):
                s = (item or "").strip()
                if not s: continue
                if s.startswith("{") and s.endswith("}"):
                    try:
                        pl = ParsedLink.from_storage(s)
                        norm.append(pl.to_link()); continue
                    except Exception:
                        pass
                norm.append(s); continue
            s2 = str(item).strip()
            if s2:
                if s2.startswith("{") and s2.endswith("}"):
                    try:
                        d = json.loads(s2)
                        if isinstance(d, dict) and "type" in d and "value" in d:
                            pl = ParsedLink.from_dict(d)
                            norm.append(pl.to_link()); continue
                    except Exception:
                        pass
                norm.append(s2)
        except Exception:
            continue

    seen, out = set(), []
    for s in norm:
        if s not in seen:
            seen.add(s); out.append(s)
    return out


class TaskExecutor:
    def __init__(
        self,
        fsm,
        reporter: Optional[TaskStatusReporter] = None,
        group_assigner: Optional[BalancedGroupAssigner] = None,
        concurrency_limit: int = 5,
        # ✅ 新增：可注入，也可由 TaskExecutor 统一持有一份
        flood: Optional[FloodController] = None,
        sender_factory: Optional[Any] = None,
    ) -> None:
        self.fsm = fsm
        self.reporter = reporter
        self.group_assigner = group_assigner or BalancedGroupAssigner()
        self.concurrency_limit = int(max(1, concurrency_limit))
        self._joined: Optional[Dict[str, List[ParsedLink]]] = None
        # ✅ 新增：同一个 FloodController 贯穿 TaskExecutor → Runner
        self.flood = flood or FloodController()
        self.sender_factory = sender_factory
        self._on_username_invalid_cb = None  

        if not self.reporter:
            log_warning("TaskExecutor 初始化：reporter 未注入，后续通知/审计可能丢失",
                        extra={"concurrency_limit": self.concurrency_limit})

    # ---------------- 准备分配与入群 ---------------- #

    async def prepare_assignment(
        self,
        user_id: int,
        clients: Dict[str, TelegramClient],
        task: SendTask,
        event: Optional[Any] = None,
        *,
        to_user_id: Optional[int] = None,
    ) -> Dict[str, List[ParsedLink]]:
        recv_uid = int(to_user_id or user_id)

        ctx = get_log_context() or {}
        trace_id = ctx.get("trace_id") or generate_trace_id()
        set_log_context({
            **ctx, "trace_id": trace_id, "phase": "prepare_assignment",
            "user_id": user_id, "task_id": getattr(task, "task_id", None)
        })

        raw_groups: List[Any] = list(task.group_list or [])
        raw_total = len(raw_groups)

        normalized_groups = _pre_normalize_groups(raw_groups)
        try:
            group_links: List[ParsedLink] = await parse_links_with_interval(
                normalized_groups, interval=PARSE_LINK_INTERVAL
            )

        except Exception as e:
            log_exception("群链接解析失败（parse_links_with_interval 异常）", exc=e, extra={
                "user_id": user_id, "task_id": getattr(task, "task_id", None), "trace_id": trace_id
            })
            if self.reporter:
                try:
                    await self.reporter.notify_simple(user_id, "群组解析异常，任务无法启动", [
                        f"解析时发生异常：{str(e)}"
                    ], level="error", to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_simple 发送失败（parse links exception）", extra={"user_id": user_id})
            return {}

        # —— 一次性“用户名无效”提醒回调 —— #
        async def _notify_username_invalid(uname: str):
            if self.reporter and isinstance(self.reporter, TaskStatusReporter):
                try:
                    await self.reporter.notify_username_invalid_once(user_id, uname, to_user_id=recv_uid)
                except Exception:
                    pass
        self._on_username_invalid_cb = _notify_username_invalid         

        try:
            pre_ok, pre_bad = await _precheck_links_for_assignment(
                user_id, clients, group_links, on_username_invalid=_notify_username_invalid
            )
            if pre_bad:
                for g, reason in pre_bad:
                    try:
                        gl = g.to_link()
                    except Exception:
                        gl = str(g)
                    log_warning("precheck_skip_invalid_link", extra={"user_id": user_id, "group": gl, "reason": reason})
            group_links = _dedup_links_keep_order(pre_ok)
        except Exception as e:
            log_warning("pre_assignment_precheck_failed(fail-open)", extra={"user_id": user_id, "err": str(e)})

        parsed_total = len(group_links)
        log_info("🔎 群链接解析（含预检+去重）结果", extra={
            "user_id": user_id,
            "task_id": getattr(task, "task_id", None),
            "trace_id": trace_id,
            "parsed_total": parsed_total,
            "parsed_samples": [getattr(pl, "log_repr", lambda: str(pl))() for pl in group_links[:LOG_SAMPLE_COUNT]],
        })

        if not group_links:
            log_warning("群组链接解析后为空，停止启动", extra={"user_id": user_id, "parsed_total": parsed_total, "trace_id": trace_id})
            if self.reporter:
                try:
                    await self.reporter.notify_simple(
                        user_id,
                        "群组链接解析失败，任务无法启动",
                        [
                            f"• 原始数量：{raw_total}",
                            f"• 解析后数量：{parsed_total}",
                            "• 请检查链接格式是否为 @username / t.me/xxx / 邀请链接",
                        ],
                        level="error",
                        to_user_id=recv_uid,
                    )
                except Exception:
                    log_warning("notify_simple 发送失败（parsed empty）", extra={"user_id": user_id})
            return {}

        ordered_clients = clients
        

        log_info("📦 分配前准备完成（进入入群）", extra={
            "user_id": user_id,
            "task_id": getattr(task, "task_id", None),
            "trace_id": trace_id,
            "clients_total": len(clients or {}),
            "groups_total": parsed_total,
            "strategy": "default",
        })

        if not self.group_assigner:
            log_warning("没有可用的 group_assigner，无法进行分配", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
            if self.reporter:
                try:
                    await self.reporter.notify_assignment_fail(user_id, "分配器不可用，任务无法启动", to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_assignment_fail 发送失败（no group_assigner）", extra={"user_id": user_id})
            return {}

        try:
            inject_trace_context("assign_group", user_id=user_id)
            # ⚠️ 这里传入的是“预检+去重”后的 group_links
            assignments = await self.group_assigner.assign(user_id, ordered_clients, group_links)
        except Exception as e:
            log_exception("调用 group_assigner.assign 失败", exc=e, extra={"user_id": user_id, "trace_id": trace_id})
            if self.reporter:
                try:
                    await self.reporter.notify_assignment_fail(user_id, f"分配阶段异常：{e}", to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_assignment_fail 发送失败（assign exception）", extra={"user_id": user_id})
            return {}

        try:
            log_info("📤 分配结果准备执行入群检查", extra={
                
                "user_id": user_id,
                "task_id": getattr(task, "task_id", None),
                "trace_id": trace_id,
                "assignments_count": len(assignments) if assignments is not None else 0,
                "assignments_sample": {p: len(l) for p, l in list((assignments or {}).items())[:10]},
            })
        except Exception:
            log_debug("log assignments snapshot failed", extra={"user_id": user_id})

        if not assignments:
            log_warning("分配结果为空（assignments 为空）", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
            if self.reporter:
                try:
                    await self.reporter.notify_assignment_fail(
                        user_id, "无可用账号或链接均不可用", to_user_id=recv_uid,
                    )
                except Exception:
                    log_warning("notify_assignment_fail 发送失败（assignments empty）", extra={"user_id": user_id})
            return {}

        per_phone_counts = {p: len(v or []) for p, v in assignments.items()}
        log_debug("分配结果按账号统计", extra={"user_id": user_id, "assignments_per_phone": per_phone_counts, "trace_id": trace_id})

        log_info("🚀 开始执行入群", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None), "trace_id": trace_id})
        try:
            joined = await self.group_assigner.execute_assignment(
                user_id, ordered_clients, assignments,
                event=event, reporter=self.reporter,
                max_parallel_accounts=self.concurrency_limit,
                to_user_id=recv_uid,
                flood=self.flood,
            )
        except Exception as e:
            log_exception("group_assigner.execute_assignment 失败", exc=e, extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
            if self.reporter:
                try:
                    await self.reporter.notify_assignment_fail(user_id, f"入群阶段异常：{e}", to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_assignment_fail 发送失败（execute_assignment exception）", extra={"user_id": user_id})
            return {}

        if not joined:
            try:
                reported_list = None
                if self.reporter:
                    reported_list = getattr(self.reporter, "results_by_user", {}).get(user_id, None)
                log_warning("execute_assignment 没有返回任何 joined 记录", extra={
                    "user_id": user_id,
                    "task_id": getattr(task, "task_id", None),
                    "trace_id": trace_id,
                    "assignments_count": len(assignments),
                    "reporter_present": bool(self.reporter),
                    "reporter_results_by_user_len": len(reported_list) if isinstance(reported_list, list) else "N/A",
                    "assignments_sample": {p: len(l) for p, l in list(assignments.items())[:10]},
                })
                if reported_list:
                    log_debug("reporter.results_by_user sample", extra={"user_id": user_id, "sample": reported_list[:50]})
            except Exception:
                log_debug("inspect reporter state failed", extra={"user_id": user_id})
        else:
            log_info("execute_assignment 返回 joined 快照", extra={"user_id": user_id, "joined_phones": list(joined.keys()), "joined_counts": {p: len(l) for p, l in joined.items()}})

        self._joined = joined or {}
        success_total = sum(len(v) for v in (joined or {}).values())

        log_info("📦 入群阶段完成", extra={
            "user_id": user_id,
            "task_id": getattr(task, "task_id", None),
            "trace_id": trace_id,
            "phones_assigned": len(assignments),
            "phones_joined": len(joined or {}),
            "sendable_total": success_total,
            "success_total": success_total,
        })

        return joined

    # ================= 入群（单目标） ================= #
    # ✅ 统一“账号内串行 join”的最小执行单元；仅负责返回 JoinResult，不在此处 sleep
    async def join_one(self, client: TelegramClient, target: ParsedLink) -> JoinResult:
        ctx = get_log_context() or {}
        try:
            res = await ensure_join(client, target)
            return res
        except Exception as e:
            # 兜底：若有 telethon 错误分类器，尽量给策略秒数
            try:
                pol = classify_telethon_error(e)  # -> {action, seconds?, reason}
                seconds = int(getattr(pol, "seconds", 0) or 0)
                code = JoinCode.FLOOD if seconds > 0 else JoinCode.UNKNOWN
                return JoinResult(code=code, meta={"attempted_join": True, "seconds": seconds, "reason": getattr(pol, "reason", None)})
            except Exception:
                return JoinResult(code=JoinCode.UNKNOWN, meta={"attempted_join": True, "reason": str(e)})

    async def _load_template_maps(self, user_id: int) -> dict:
        try:
            meta = await self.fsm.get_metadata(user_id)
            if isinstance(meta, dict):
                return {
                    "emoji_seq_ids": meta.get("emoji_seq_ids") or [],
                    "emoji_name_map": meta.get("emoji_name_map") or {},
                }
        except Exception:
            pass
        return {"emoji_seq_ids": [], "emoji_name_map": {}}

    # ---------------- 主执行：一轮群发 ---------------- #

    async def execute_round(
        self,
        user_id: int,
        clients: Dict[str, TelegramClient],
        task: SendTask,
        event: Optional[Any] = None,
        round_no: int = 1,
        *,
        to_user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        recv_uid = int(to_user_id or user_id)

        ctx = get_log_context() or {}
        trace_id = ctx.get("trace_id") or generate_trace_id()
        set_log_context({**ctx, "user_id": user_id, "trace_id": trace_id, "cmd": "/ok", "round": round_no})

        log_info(
            f"🚀 启动第 {round_no} 轮消息群发",
            extra={"user_id": user_id, "task_id": getattr(task, "task_id", None), "trace_id": trace_id}
        )

        if not self._joined:
            rebuilt = await self.prepare_assignment(user_id, clients, task, event=event, to_user_id=recv_uid)
            if not rebuilt:
                if self.reporter:
                    try:
                        await self.reporter.notify_task_error(user_id, "未分配账号-群组", "assignment_missing", to_user_id=recv_uid)
                    except Exception:
                        log_warning("notify_task_error 发送失败（assignment_missing）", extra={"user_id": user_id})
                log_warning("prepare_assignment 未生成 assignments，结束本轮", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None)})
                return {"task_id": getattr(task, "task_id", None) or f"TASK-{user_id}", "results": [], "error": "assignment_missing"}

        phone_group_map: Dict[str, List[ParsedLink]] = {
            phone: groups for phone, groups in (self._joined or {}).items() if phone in clients
        }
        if not phone_group_map:
            if self.reporter:
                try:
                    await self.reporter.notify_task_error(user_id, "没有任何账号可用于群发，任务结束", "no_clients", to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_task_error 发送失败（no_clients）", extra={"user_id": user_id})
            log_warning("phone_group_map 为空：没有匹配到可用 clients 与 joined", extra={"user_id": user_id, "task_id": getattr(task, "task_id", None), "joined_snapshot": list(self._joined.keys() if self._joined else [])})
            return {"task_id": getattr(task, "task_id", None) or f"TASK-{user_id}", "results": [], "error": "no_clients"}

        message = task.message
        task_id = getattr(task, "task_id", None) or f"TASK-{user_id}"
        try:
            message.validate()
        except Exception as ve:
            if self.reporter:
                try:
                    await self.reporter.notify_task_error(user_id, "任务校验失败", str(ve), to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_task_error 发送失败（message validation）", extra={"user_id": user_id})
            return {"task_id": task_id, "results": [], "error": "message_validation_failed"}

        try:
            maps = await self._load_template_maps(user_id)
            seq_ids = maps.get("emoji_seq_ids") or []
            name_map = maps.get("emoji_name_map") or {}

            text_field = None
            if message.type == MessageType.TEXT:
                text_field = "content"
            elif message.type in (MessageType.MEDIA, MessageType.ALBUM):
                text_field = "caption"

            if text_field:
                raw_text = (getattr(message, text_field, "") or "").strip()
                new_text, new_parse_mode, new_entities, warns = smart_text_parse(
                    raw_text,
                    template_seq_ids=seq_ids,
                    template_name_map=name_map
                )
                setattr(message, text_field, new_text)
                if new_entities is not None:
                    message.entities = new_entities
                if new_parse_mode is not None:
                    message.parse_mode = new_parse_mode

                # 仅传 entities，记录自定义 emoji id
                message.emoji_ids = extract_custom_emoji_ids(message.entities)

                try:
                    await self.fsm.set_state(user_id, task)
                    await self.fsm.set_metadata(user_id, {"emoji_ids": message.emoji_ids})
                except Exception:
                    pass

                if warns:
                    log_warning("模板解析告警", extra={"user_id": user_id, "task_id": task_id, "warns": warns})
                else:
                    log_debug(
                        "模板解析完成",
                        extra={
                            "user_id": user_id, "task_id": task_id,
                            "entities": len(message.entities or []), "parse_mode": message.parse_mode
                        }
                    )
        except Exception as e:
            log_warning(f"模板扩展失败，已降级为原文发送：{e}", extra={"user_id": user_id, "task_id": task_id})

        # 4) Runner 工厂：统一上下文与签名
        async def runner_factory(phone: str, client: TelegramClient, groups: List[ParsedLink], **kwargs: Any):
            log_info(
                "🛠️ 启动 Runner 工厂",
                extra={
                    "user_id": user_id,
                    "phone": phone,
                    "session": getattr(getattr(client, 'session', None), 'filename', ''),
                    "groups": [getattr(g, "short", lambda: str(g))() for g in groups[:5]],
                }
            )
            try:
                me = await client.get_me()
                set_log_context({"me_id": getattr(me, "id", None), "me_username": getattr(me, "username", None)})
                log_debug("👤 当前账号身份", extra={"user_id": user_id, "phone": phone, "me_id": getattr(me, "id", None), "username": getattr(me, "username", "")})
            except Exception as e:
                log_warning(f"账号掉线/未连接或获取资料失败，跳过: {phone} | {e}", extra={"user_id": user_id})
                return None

            # 清洁 kwargs，避免传递冲突
            for key in ["user_id", "task_id", "phone", "client", "event"]:
                kwargs.pop(key, None)

            # ✅ 关键新增：带上下文注入的 Sender 工厂
            def _make_sender() -> MessageSender:
                if callable(self.sender_factory):
                    # 优先尝试“新签名”：具名参数更齐全
                    try:
                        return self.sender_factory(
                            user_id=user_id,
                            client=client,
                            phone=phone,
                            task_id=task_id,
                            fsm=self.fsm,
                            reporter=self.reporter,
                            flood=self.flood,
                        )
                    except TypeError:
                        # 回退尝试“旧签名”：(user_id, client)
                        try:
                            return self.sender_factory(user_id, client)  # type: ignore[misc]
                        except TypeError:
                            pass
                        except Exception:
                            pass
                    except Exception:
                        pass
                # 兜底：默认 Sender
                return MessageSender(
                    client=client,
                    phone=phone,
                    user_id=user_id,
                    task_id=task_id,
                    fsm=self.fsm,
                    reporter=self.reporter,
                    flood=self.flood,
                )

            
            return Runner(
                phone=phone,
                client=client,
                groups=groups,
                message=message,
                user_id=user_id,
                task_id=task_id,
                name=getattr(me, "first_name", "未知"),
                username=getattr(me, "username", "unknown"),
                reporter=self.reporter,
                event=event,
                fsm=self.fsm,
                to_user_id=recv_uid,
                trace_id=trace_id,
                sender_factory=_make_sender,   # ✅ 把 sender 工厂传给 Runner
                flood=self.flood,
                reassigner=getattr(self, "reassigner", None),  # ← 若你将 reassigner 挂到 TaskExecutor 或 scheduler 上
                 on_username_invalid=self._on_username_invalid_cb,
            )

        # 5) 并发执行本轮群发
        round_executor = RoundExecutor(
            clients=clients,
            groups_by_phone=phone_group_map,
            message=message,
            user_id=user_id,
            task_id=task_id,
            runner_factory=runner_factory,
            reporter=self.reporter,
            event=event,
            concurrency_limit=self.concurrency_limit,
            to_user_id=recv_uid,
            trace_id=trace_id,
            round_index=round_no,
        )

        try:
            results = await round_executor.run()
        except Exception as err:
            log_exception("RoundExecutor.run 发生异常，终止本轮并上报", exc=err, extra={"user_id": user_id, "task_id": task_id, "trace_id": trace_id})
            if self.reporter:
                try:
                    await self.reporter.notify_task_error(user_id, "群发轮次执行异常", err, to_user_id=recv_uid)
                except Exception:
                    log_warning("notify_task_error 发送失败（round executor exception）", extra={"user_id": user_id})
            return {"task_id": task_id, "results": [], "error": "round_executor_failed"}

        # 统计结果（简单计数 success/failure/pending）
        try:
            succ = 0
            fail = 0
            pend = 0
            for r in (results or []):
                code = None
                if isinstance(r, dict):
                    code = r.get("code") or r.get("status") or r.get("result") or r.get("state")
                    if not code:
                        if r.get("groups_failed"):
                            code = "partial"
                        elif r.get("groups_success") and not r.get("groups_failed"):
                            code = "success"
                if str(code).lower() in ("success", "ok", "already_in"):
                    succ += 1
                elif str(code).lower() in ("failure", "failed", "error"):
                    fail += 1
                elif str(code).lower() == "pending":
                    pend += 1
                else:
                    texty = json.dumps(r, ensure_ascii=False) if isinstance(r, (dict, list)) else str(r)
                    if "error" in texty.lower() or "exception" in texty.lower():
                        fail += 1
                    else:
                        pend += 1
            log_info("本轮结果统计", extra={"user_id": user_id, "task_id": task_id, "trace_id": trace_id, "succ": succ, "fail": fail, "pending": pend})
        except Exception:
            log_debug("results summary failed", extra={"user_id": user_id, "task_id": task_id})

        log_info(
            "📊 群发轮次完成",
            extra={
                "user_id": user_id,
                "task_id": task_id,
                "trace_id": trace_id,
                "phones": len(phone_group_map),
                "results_count": len(results or []),
            }
        )
        return {"task_id": task_id, "results": results, "error": None}
