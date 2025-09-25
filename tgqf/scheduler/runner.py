# -*- coding: utf-8 -*-
# scheduler/runner.py
from __future__ import annotations

import asyncio
import random
from typing import List, Dict, Optional, Any, Tuple, Callable, Awaitable

from telethon import TelegramClient

from core.task_status_reporter import TaskStatusReporter
from typess.link_types import ParsedLink
from typess.message_enums import MessageContent, TaskStatus

from unified.config import MAX_CONCURRENCY, RUNNER_TIMEOUT ,LOG_SAMPLE_COUNT
from unified.trace_context import generate_trace_id, set_log_context, get_log_context
from unified.logger import log_info, log_warning, log_exception, log_debug
from scheduler.ratelimit import FloodController

# 集中策略：错误归类 & 策略分发（冷却/黑名单/重分配等）
from core.telethon_errors import (
    StrategyContext,
    handle_send_error,
    classify_send_exception,
    Reassigner,
)

# === 弹性依赖：tg.link_utils 中的函数可能不存在（避免导入失败导致崩溃） ===
try:
    from tg import link_utils as _lu
except Exception:
    _lu = None

_CHECK_LINKS_AVAILABILITY: Optional[Callable[..., Any]] = getattr(_lu, "check_links_availability", None) if _lu else None


class Runner:
    """单账号串行执行器（P0 增量：预检结果缓存 + FLOOD 回升探针）"""

    def __init__(
        self,
        phone: str,
        client: TelegramClient,
        groups: List[ParsedLink],
        message: MessageContent,
        user_id: int,
        task_id: str,
        reporter: Optional[TaskStatusReporter] = None,
        *,
        timeout: int = RUNNER_TIMEOUT,
        to_user_id: Optional[int] = None,
        trace_id: Optional[str] = None,
        flood: Optional[FloodController] = None,
        listeners: Optional[List[Callable[[str, Dict[str, Any]], Awaitable[None]]]] = None,
        sender_factory: Optional[Callable[[], Any]] = None,
        sender: Optional[Any] = None,
        fsm: Optional[Any] = None,
        reassigner: Optional["Reassigner"] = None,
        on_username_invalid: Optional[Callable[[str], Awaitable[None]]] = None,
        # ✅ 新增：本轮链接可用性缓存（TaskExecutor 可注入），结构：{ to_link: {"valid": bool, "reason": str} }
        link_check_cache: Optional[Dict[str, Dict[str, Any]]] = None,
        **_: Any,
    ) -> None:
        self.phone = str(phone)
        self.client = client
        self.groups = list(groups or [])
        self.message = message
        self.user_id = int(user_id)
        self.task_id = str(task_id)
        self.reporter = reporter
        self.timeout = int(timeout or RUNNER_TIMEOUT)
        self.to_user_id = int(to_user_id or user_id)
        self._trace_id = str(trace_id or "")

        self.success_count = 0
        self.failed_count = 0
        self.groups_success: List[str] = []
        self.groups_failed: List[Dict[str, Any]] = []

        self.flood = flood or FloodController()
        self._listeners: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = list(listeners or [])

        self._sender_factory = sender_factory
        self._sender = sender
        self.fsm = fsm
        self.reassigner = reassigner
        self._on_username_invalid = on_username_invalid

        # ✅ P0：预检缓存（本轮有效），默认空字典
        self._link_check_cache: Dict[str, Dict[str, Any]] = dict(link_check_cache or {})

    def on(self, listener: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> None:
        if callable(listener):
            self._listeners.append(listener)

    async def _emit(self, event: str, payload: Dict[str, Any]) -> None:
        if not self._listeners:
            return
        for cb in list(self._listeners):
            try:
                res = cb(event, payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                log_warning("runner_event_listener_failed", extra={"event": event, "err": str(e)})

    async def run(self) -> Dict[str, Any]:
        trace_id = self._trace_id or generate_trace_id()
        self._trace_id = trace_id
        set_log_context({
            "trace_id": trace_id,
            "task_id": self.task_id,
            "phone": self.phone,
            "user_id": self.user_id,
            "groups_total": len(self.groups or []),
        })
        log_info("🚀 启动 Runner", extra={"user_id": self.user_id, "task_id": self.task_id, "phone": self.phone, "groups_total": len(self.groups)})
        await self._emit("runner_start", {
            "user_id": self.user_id, "task_id": self.task_id, "phone": self.phone,
            "groups_total": len(self.groups or []), "trace_id": trace_id
        })
        try:
            if self.reporter:
                try:
                    await self.reporter.update_status(self.user_id, TaskStatus.RUNNING)
                except Exception:
                    log_warning("reporter.update_status(RUNNING) 失败（忽略）", extra={"user_id": self.user_id, "phone": self.phone})
            return await self._run_all()
        except Exception as e:
            log_exception("Runner.run 未捕获异常", exc=e, extra={"user_id": self.user_id, "task_id": self.task_id, "phone": self.phone})
            return await self._finalize(TaskStatus.FAILED, f"runner exception: {e}", result_status_override="failed")

    async def _run_all(self) -> Dict[str, Any]:
        if not self.groups:
            log_warning("Runner: groups 为空，直接 finalize", extra={"user_id": self.user_id, "task_id": self.task_id, "phone": self.phone})
            return await self._finalize(TaskStatus.FAILED, "无群组可发")

        # ✅ 发前预检：可用性（fail-open）+ 复用缓存
        try:
            valid_links, invalid_pairs = await self._safe_check_links(
                self.client,
                self.groups,
                on_username_invalid=self._on_username_invalid,
            )
            if invalid_pairs:
                for g, reason in invalid_pairs:
                    try:
                        gl = g.to_link() if hasattr(g, "to_link") else str(g)
                    except Exception:
                        gl = str(g)
                    # 写回缓存
                    self._link_check_cache[gl] = {"valid": False, "reason": str(reason)}
                    log_warning("skip_invalid_link", extra={
                        "user_id": self.user_id, "phone": self.phone, "group": gl, "reason": str(reason)
                    })
                    await self._emit("send_skip", {"user_id": self.user_id, "phone": self.phone, "group": gl, "reason": str(reason)})
                    if self.reporter:
                        try:
                            self.reporter.track_result({
                                "user_id": self.user_id, "phone": self.phone, "group": gl,
                                "code": "skip_invalid_link", "reason": str(reason)
                            })
                        except Exception:
                            pass
            self.groups = list(valid_links or [])
        except Exception as _e:
            log_warning("check_links_availability_skip_on_error", extra={"user_id": self.user_id, "err": str(_e)})

        if not self.groups:
            log_warning("预检后无可发送群组，终止本轮", extra={"user_id": self.user_id, "task_id": self.task_id, "phone": self.phone})
            return await self._finalize(TaskStatus.STOPPED, "全部链接无效或被跳过")

        try:
            sample_links = [getattr(g, "to_link", lambda: str(g))() for g in self.groups[:LOG_SAMPLE_COUNT]]
        except Exception:
            sample_links = []
        log_debug("Runner.groups 样本", extra={"user_id": self.user_id, "task_id": self.task_id, "phone": self.phone, "sample": sample_links, "count": len(self.groups)})

        total = len(self.groups)

        from core.defaults.message_sender import MessageSender
        sender = self._sender or (self._sender_factory() if callable(self._sender_factory) else None)
        if sender is None:
            sender = MessageSender(
                client=self.client,
                phone=self.phone,
                user_id=self.user_id,
                task_id=self.task_id,
                fsm=self.fsm,
                reporter=self.reporter,
                flood=self.flood,
            )

        ok_count_before = self.success_count

        for idx, pl in enumerate(self.groups, 1):
            group_link = pl.to_link()
            link_kind = getattr(pl, "type", None).value if getattr(pl, "type", None) else "unknown"

            ck = getattr(pl, "cache_key", None)
            chat_key = ck() if callable(ck) else (ck or group_link)

            msg_type = getattr(self.message, "type", None)
            msg_type_str = getattr(msg_type, "value", str(msg_type)) if msg_type is not None else "unknown"

            set_log_context({"trace_id": self._trace_id, "task_id": self.task_id, "phone": self.phone, "user_id": self.user_id, "group": group_link, "index": idx, "total": total})

            try:
                async with self.flood.sending(self.phone, chat_key):
                    log_info("📤 准备发送到群组", extra={"user_id": self.user_id, "phone": self.phone, "group": group_link, "index": idx, "total": total})

                    result = await sender.send(pl=pl, msg=self.message, link_preview=False)
                    ok = bool(result.get("success"))

                    if ok:
                        self.success_count += 1
                        self.groups_success.append(group_link)
                        # ✅ FLOOD 回升探针（成功反馈）
                        try:
                            fn = getattr(self.flood, "on_success", None)
                            if callable(fn):
                                fn(self.phone, chat_key)
                        except Exception:
                            pass

                        await self._emit("send_ok", {
                            "user_id": self.user_id, "phone": self.phone, "group": group_link,
                            "index": idx, "total": total, "message_id": result.get("message_id"),
                            "link_kind": link_kind, "msg_type": msg_type_str
                        })
                        if self.reporter:
                            try:
                                self.reporter.track_result({
                                    "user_id": self.user_id,
                                    "phone": self.phone,
                                    "group": group_link,
                                    "code": "success",
                                    "reason": "",
                                    "attr": "ok",
                                    "index": idx, "total": total,
                                    "link_kind": link_kind,
                                    "msg_type": msg_type_str,
                                    "trace_id": get_log_context().get("trace_id"),
                                    "task_id": self.task_id,
                                    "seconds": 0,
                                })
                            except Exception as e:
                                log_warning("reporter.track_result(success) 失败（忽略）", extra={"user_id": self.user_id, "phone": self.phone, "group": group_link, "error": str(e)})
                    else:
                        err_msg = result.get("error") or result.get("reason") or "send_failed"
                        err_kind = result.get("kind")
                        err_code = result.get("error_code")
                        err_meta = result.get("error_meta")

                        # ✅ FLOOD 回升探针（失败反馈）
                        try:
                            fn = getattr(self.flood, "on_fail", None)
                            if callable(fn):
                                fn(self.phone, chat_key)
                        except Exception:
                            pass

                        # 策略中心处理（返回 action）
                        action_attr = "unknown"
                        try:
                            ctx = StrategyContext(
                                fsm=self.fsm,
                                flood=self.flood,
                                user_id=self.user_id,
                                phone=self.phone,
                                chat_key=chat_key,
                                reporter=self.reporter,
                                scheduler=None,
                                reassigner=self.reassigner,
                            )
                            act = await handle_send_error(err_code, {**(err_meta or {}), "link_kind": link_kind}, ctx)
                            # 兼容：旧实现返回 bool，新实现返回 dict
                            if isinstance(act, dict):
                                action_attr = (act.get("attr") or "unknown").lower()
                                if act.get("cooldown_chat"):
                                    await self._emit("cooldown", {
                                        "type": "handled_by_strategy",
                                        "error_code": err_code,
                                        "chat_key": chat_key,
                                        "phone": self.phone,
                                        "seconds": int(act.get("cooldown_chat") or 0),
                                    })
                            elif act is True:
                                await self._emit("cooldown", {
                                    "type": "handled_by_strategy",
                                    "error_code": err_code,
                                    "chat_key": chat_key,
                                    "phone": self.phone,
                                })
                        except Exception as _se:
                            log_warning("strategy_handle_error_failed", extra={
                                "user_id": self.user_id, "phone": self.phone, "group": group_link,
                                "error_code": err_code, "err": str(_se)
                            })

                        log_warning("发送失败（runner）", extra={
                            "user_id": self.user_id, "phone": self.phone, "group": group_link,
                            "error": err_msg, "kind": err_kind, "error_code": err_code, "error_meta": err_meta,
                        })
                        self._fail(group_link, f"{err_msg} (code={err_code} kind={err_kind})", report=False)
                        await self._emit("send_fail", {
                            "user_id": self.user_id, "phone": self.phone, "group": group_link,
                            "index": idx, "total": total, "error": err_msg,
                            "error_code": err_code, "error_kind": err_kind, "error_meta": err_meta,
                            "link_kind": link_kind, "msg_type": msg_type_str
                        })

                        if self.reporter:
                            try:
                                seconds = 0
                                if isinstance(err_meta, dict):
                                    s = err_meta.get("seconds")
                                    if isinstance(s, (int, float)):
                                        seconds = max(0, int(s))
                                self.reporter.track_result({
                                    "user_id": self.user_id,
                                    "phone": self.phone,
                                    "group": group_link,
                                    "code": err_code or "failure",
                                    "status": "failure",
                                    "reason": err_msg,
                                    "attr": action_attr or "unknown",
                                    "seconds": seconds,
                                    "index": idx, "total": total,
                                    "link_kind": link_kind,
                                    "msg_type": msg_type_str,
                                    "trace_id": get_log_context().get("trace_id"),
                                    "task_id": self.task_id,
                                    "error_kind": err_kind,
                                    "error_meta": err_meta,
                                })
                            except Exception:
                                pass
            except Exception as e:
                log_exception("单群发送异常（未分类）", exc=e, extra={"user_id": self.user_id, "phone": self.phone, "group": group_link})
                try:
                    code, meta = classify_send_exception(e)
                    try:
                        ctx = StrategyContext(
                            fsm=self.fsm,
                            flood=self.flood,
                            user_id=self.user_id,
                            phone=self.phone,
                            chat_key=chat_key,
                            reporter=self.reporter,
                            scheduler=None,
                            reassigner=self.reassigner,
                        )
                        await handle_send_error(code, (meta or {}), ctx)
                    except Exception:
                        pass
                except Exception:
                    code = None
                self._fail(group_link, str(e))

            if self.reporter:
                try:
                    await self.reporter.update_progress(self.user_id, idx, total)
                except Exception:
                    pass

        # ✅ 成功率回升：一轮结束后可触发全局恢复（可选实现）
        try:
            fn = getattr(self.flood, "on_round_complete", None)
            if callable(fn):
                before = ok_count_before
                after = self.success_count
                fn(self.phone, success_delta=max(0, after - before), total=len(self.groups))
        except Exception:
            pass

        return await self._finalize(TaskStatus.STOPPED, None)

    async def _safe_check_links(
        self,
        client: TelegramClient,
        groups: List[ParsedLink],
        *,
        on_username_invalid: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> Tuple[List[ParsedLink], List[Tuple[ParsedLink, str]]]:
        """
        P0：预检缓存（本轮 TTL）
        - 命中缓存：直接采用缓存判断
        - 未命中：调用 check_links_availability，对增量集合做校验，并把结果写回缓存
        - fail-open：远端检查失败 → 未命中部分全部视为有效（但尊重缓存里的无效）
        """
        # 分桶：缓存命中/未命中
        to_check: List[ParsedLink] = []
        cached_valid: List[ParsedLink] = []
        cached_invalid: List[Tuple[ParsedLink, str]] = []

        for g in groups or []:
            try:
                k = g.to_link()
            except Exception:
                k = str(g)
            cache_rec = self._link_check_cache.get(k)
            if isinstance(cache_rec, dict) and "valid" in cache_rec:
                if cache_rec.get("valid"):
                    cached_valid.append(g)
                else:
                    cached_invalid.append((g, str(cache_rec.get("reason") or "cache_invalid")))
            else:
                to_check.append(g)

        # 无检查函数 → 全部视为有效（尊重缓存的无效）
        if not callable(_CHECK_LINKS_AVAILABILITY):
            return cached_valid + to_check, cached_invalid

        if not to_check:
            return cached_valid, cached_invalid

        # 对未命中部分做真实检查
        try:
            result = await _CHECK_LINKS_AVAILABILITY(
                client,
                to_check,
                max_concurrency=MAX_CONCURRENCY,
                skip_invite_check=False,
                redis=None,
                user_id=self.user_id,
                on_username_invalid=on_username_invalid,
            )
            if isinstance(result, tuple) and len(result) == 2:
                valid_links_new, invalid_links_new = result
            else:
                valid_links_new = list(result) if isinstance(result, (list, tuple)) else []
                invalid_links_new = []
        except Exception as e:
            # fail-open：把未命中部分全部视为有效
            log_exception("⚠️ 链接可用性检查失败（按可用放行）", exc=e, extra=get_log_context() or {"user_id": self.user_id})
            return cached_valid + to_check, cached_invalid

        # 写回缓存
        for g in valid_links_new or []:
            try:
                k = g.to_link()
            except Exception:
                k = str(g)
            self._link_check_cache[k] = {"valid": True}
        for g, reason in (invalid_links_new or []):
            try:
                k = g.to_link() if hasattr(g, "to_link") else str(g)
            except Exception:
                k = str(g)
            self._link_check_cache[k] = {"valid": False, "reason": str(reason)}

        # 合并返回
        fixed_invalid: List[Tuple[ParsedLink, str]] = list(cached_invalid)
        for g, reason in (invalid_links_new or []):
            if not isinstance(g, ParsedLink):
                try:
                    g = ParsedLink.auto_parse(str(g))
                except Exception:
                    pass
            fixed_invalid.append((g, str(reason)))

        valid_links_final = list(cached_valid) + [g for g in (valid_links_new or []) if isinstance(g, ParsedLink)]
        return valid_links_final, fixed_invalid

    def _fail(self, group_link: str, error: str, *, report: bool = True) -> None:
        self.failed_count += 1
        self.groups_failed.append({"group": group_link, "phone": self.phone, "error": error, "status": "failed"})
        if report and self.reporter:
            try:
                self.reporter.track_result({
                    "user_id": self.user_id,
                    "phone": self.phone,
                    "group": group_link,
                    "code": "failure",
                    "reason": error,
                })
            except Exception:
                log_warning("reporter.track_result(failure) 失败（忽略）", extra={"user_id": self.user_id, "phone": self.phone, "group": group_link})

    async def _finalize(self, status: TaskStatus, error: Optional[str], *, result_status_override: Optional[str] = None) -> Dict[str, Any]:
        result_status = result_status_override or (
            "ok" if self.success_count > 0 and self.failed_count == 0 else
            ("failed" if self.success_count == 0 else "partial")
        )
        result = {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "phone": self.phone,
            "status": result_status,
            "error": error,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "groups_success": self.groups_success,
            "groups_failed": self.groups_failed,
            "trace_id": self._trace_id or generate_trace_id(),
        }

        log_info("Runner 完成", extra={"user_id": self.user_id, "task_id": self.task_id, "phone": self.phone, "status": result_status, "success": self.success_count, "failed": self.failed_count})
        await self._emit("runner_done", result)

        if self.reporter:
            try:
                await self.reporter.update_result(self.user_id, [result])
            except Exception:
                log_warning("reporter.update_result 失败（忽略）", extra={"user_id": self.user_id})

        return result
