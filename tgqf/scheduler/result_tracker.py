# -*- coding: utf-8 -*-
# scheduler/result_tracker.py
from __future__ import annotations

from typing import Any, Dict, Optional, List
import time
from typess.join_status import JoinCode,JoinResult
from typess.message_enums import TaskStatus
from unified.logger import log_debug, log_exception
from unified.trace_context import (
    generate_trace_id,
    get_log_context,
    set_log_context,
    with_trace,
)
from typess.link_types import ParsedLink
from core.task_status_reporter import TaskStatusReporter


def _now_ms() -> int:
    return int(time.time() * 1000)



class ResultTracker:
    """
    轻量代理：统一使用 TaskStatusReporter 来存储/分类结果。
    - 保持与旧接口兼容（track/summary/as_report_payload/get_status）
    - 不再维护 success/failed/cancelled 本地字段，summary 提供兼容字段
    """

    def __init__(self, user_id: int, reporter: Optional[TaskStatusReporter] = None) -> None:
        self.user_id = int(user_id)
        self.reporter = reporter or TaskStatusReporter()
        self._results: List[Dict[str, Any]] = []
        ctx = get_log_context() or {}
        # trace/task/run id 尽量从上下文取，若没有则生成/保留 None
        self.trace_id: str = str(ctx.get("trace_id") or generate_trace_id())
        self.task_id: Optional[str] = ctx.get("task_id")
        self.run_id: Optional[str] = ctx.get("run_id")

        # 保证上下文中存在 user_id/trace_id，方便 with_trace 等使用
        if "user_id" not in ctx:
            set_log_context({"user_id": self.user_id})
        if "trace_id" not in ctx:
            set_log_context({"trace_id": self.trace_id})

        log_debug("🧩 ResultTracker 初始化", extra={"user_id": self.user_id, "trace_id": self.trace_id})
        # 事件时间线（最多保留最近 500 条）
        self._timeline: list[dict[str, Any]] = []
        # === Join 聚合（账号维度） ===
        # { account_id: {"total":int,"ok":int,"already":int,"need_approve":int,"forbidden":int,"invalid":int,
        #                "flood_hist":{sec:int}, "details":[{group,code,seconds,reason}], "first_ts":int,"last_ts":int} }
        self._join_accounts: dict[str, dict[str, Any]] = {}

    # ---------------- Join 聚合：单条记录 ----------------
    def add_join_result(self, account_id: str, target: ParsedLink | str, res: JoinResult) -> None:
        """
        记录“入群”结果到账号维度聚合。
        只做统计，不做限流/休眠。
        """
        try:
            aid = str(account_id)
            g = ""
            try:
                # 兼容 ParsedLink
                if hasattr(target, "to_link"):
                    g = target.to_link()  # type: ignore[attr-defined]
                elif hasattr(target, "short"):
                    g = target.short()  # type: ignore[attr-defined]
                else:
                    g = str(target)
            except Exception:
                g = str(target)

            acc = self._join_accounts.setdefault(aid, {
                "total": 0, "ok": 0, "already": 0, "need_approve": 0,
                "forbidden": 0, "invalid": 0,
                "flood_hist": {}, "details": [],
                "first_ts": _now_ms(), "last_ts": _now_ms(),
            })
            acc["total"] += 1
            acc["last_ts"] = _now_ms()

            code = getattr(res, "code", None)
            meta = getattr(res, "meta", {}) or {}
            seconds = int(meta.get("seconds") or 0)
            reason = str(meta.get("reason") or "")

            # 计数
            if code in (JoinCode.ALREADY, JoinCode.ALREADY_IN):  # 两个别名都支持
                acc["already"] += 1
            elif code in (JoinCode.OK, JoinCode.SUCCESS):
                acc["ok"] += 1
            elif code in (JoinCode.NEED_APPROVE, JoinCode.PENDING):
                acc["need_approve"] += 1
            elif code in (JoinCode.FORBIDDEN,):
                acc["forbidden"] += 1
            elif code in (JoinCode.INVALID,):
                acc["invalid"] += 1
            elif code in (JoinCode.FLOOD,):
                # FLOOD 视作失败类，但单独进直方图
                bucket = max(0, seconds)
                acc["flood_hist"][bucket] = acc["flood_hist"].get(bucket, 0) + 1
            # 其它（UNKNOWN/FAILURE/TIMEOUT/RESTRICTED 等）不额外计专属字段

            # 明细（可选，用于诊断/审计）
            acc["details"].append({
                "group": g, "code": str(code.name if hasattr(code, "name") else code),
                "seconds": seconds if seconds > 0 else None,
                "reason": reason or None,
            })
        except Exception as e:
            log_exception("ResultTracker.add_join_result failed", exc=e)

    # ---------------- Join 聚合：快照 ----------------
    def snapshot(self, *, account_id: Optional[str] = None) -> dict[str, Any]:
        """
        返回账号级或总体 Join 统计快照：
        {
          "total","ok","already","need_approve","forbidden","invalid",
          "flood_hist": {seconds: count},
          "duration_ms": int, "accounts": [account_id...]
        }
        """
        if not self._join_accounts:
            return {"total": 0, "ok": 0, "already": 0, "need_approve": 0,
                    "forbidden": 0, "invalid": 0, "flood_hist": {}, "duration_ms": 0, "accounts": []}

        if account_id is not None:
            aid = str(account_id)
            acc = self._join_accounts.get(aid) or {}
            first_ts = int(acc.get("first_ts") or _now_ms())
            last_ts = int(acc.get("last_ts") or first_ts)
            return {
                "total": int(acc.get("total", 0)),
                "ok": int(acc.get("ok", 0)),
                "already": int(acc.get("already", 0)),
                "need_approve": int(acc.get("need_approve", 0)),
                "forbidden": int(acc.get("forbidden", 0)),
                "invalid": int(acc.get("invalid", 0)),
                "flood_hist": dict(acc.get("flood_hist", {})),
                "duration_ms": max(0, last_ts - first_ts),
                "accounts": [aid],
            }

        # 聚合全体账号
        total = ok = already = need_approve = forbidden = invalid = 0
        flood_hist: dict[int, int] = {}
        firsts, lasts = [], []
        for aid, acc in self._join_accounts.items():
            total += int(acc.get("total", 0))
            ok += int(acc.get("ok", 0))
            already += int(acc.get("already", 0))
            need_approve += int(acc.get("need_approve", 0))
            forbidden += int(acc.get("forbidden", 0))
            invalid += int(acc.get("invalid", 0))
            for sec, cnt in (acc.get("flood_hist", {}) or {}).items():
                flood_hist[int(sec)] = flood_hist.get(int(sec), 0) + int(cnt)
            firsts.append(int(acc.get("first_ts") or _now_ms()))
            lasts.append(int(acc.get("last_ts") or _now_ms()))
        duration = max(0, (max(lasts) if lasts else _now_ms()) - (min(firsts) if firsts else _now_ms()))
        return {
            "total": total, "ok": ok, "already": already, "need_approve": need_approve,
            "forbidden": forbidden, "invalid": invalid,
            "flood_hist": flood_hist, "duration_ms": duration,
            "accounts": list(self._join_accounts.keys()),
        }

    def _push_timeline(self, event: str, data: Optional[dict[str, Any]] = None) -> None:
        try:
            item = {
                "ts": _now_ms(),
                "event": str(event),
                "trace_id": (get_log_context() or {}).get("trace_id", self.trace_id),
                "task_id": (get_log_context() or {}).get("task_id", self.task_id),
                "run_id": (get_log_context() or {}).get("run_id", self.run_id),
                "data": dict(data or {}),
            }
            self._timeline.append(item)
            if len(self._timeline) > 500:
                self._timeline = self._timeline[-500:]
        except Exception:
            pass

    def event(self, name: str, **kwargs) -> None:
        """外部可显式记录关键节点（如 round_start/round_end/reassign_try 等）"""
        self._push_timeline(name, kwargs)

    # —— Track 直接交给 reporter —— #
    @with_trace(action_name="result_track", phase="track")
    def track(self, result: Dict[str, Any]) -> None:
        """收集单条执行结果（result 为 dict，包含 code/status/reason 等）"""
        if not isinstance(result, dict):
            return
        self._results.append(result)
        # 时间线：落一条 send_result（关键字段抽取）
        try:
            pick = {k: result.get(k) for k in ("code", "status", "reason", "group", "phone", "index", "total", "msg_type", "link_kind")}
            self._push_timeline("send_result", pick)
        except Exception:
            pass
        try:
            if self.reporter and hasattr(self.reporter, "track_result"):
                self.reporter.track_result(result)
        except Exception as e:
            log_exception("ResultTracker.track reporter.track_result 失败", exc=e)

    def all_results(self) -> List[Dict[str, Any]]:
        """返回所有收集到的结果"""
        return list(self._results)

    # —— 汇总 —— #
    @with_trace(action_name="result_summary", phase="summary")
    def summary(self) -> Dict[str, Any]:
        """
        生成执行结果汇总（分类统计 + 基本数值）
        返回结构（兼容旧调用）：
        {
            "user_id": int,
            "total": int,
            "success_count": int,
            "failed_count": int,
            "pending_count": int,
            "ok": int,   # alias success_count
            "fail": int, # alias failed_count
            "pending": int, # alias pending_count
            "categories": { ... },
            "trace_id": str,
            "task_id": Optional[str],
            "run_id": Optional[str],
        }
        """
        total = len(self._results)
        def _lc(v): return str(v or "").strip().lower()

        success_count = sum(
            1 for r in self._results
            if (_lc(r.get("code")) in {"success", "already_in", "ok"} or _lc(r.get("status")) in {"success", "ok"})
        )
        failed_count = sum(
            1 for r in self._results
            if (_lc(r.get("code")) in {"failure", "failed", "error"} or _lc(r.get("status")) in {"failure", "failed", "error"})
        )
        pending_count = sum(
            1 for r in self._results
            if (_lc(r.get("code")) == "pending" or _lc(r.get("status")) == "pending")
        )

        categories: Dict[str, int] = {}
        try:
            if self.reporter:
                # 优先走公开方法；保留对旧私有方法的容错
                if hasattr(self.reporter, "categorize"):
                    categories = self.reporter.categorize(self._results)
                elif hasattr(self.reporter, "_categories"):
                    categories = self.reporter._categories(self._results)  # legacy
        except Exception as e:
            log_exception("ResultTracker.summary 调用 reporter._categories 失败", exc=e)

        # ensure trace/task/run id reflect latest context if available
        ctx = get_log_context() or {}
        trace = str(ctx.get("trace_id") or self.trace_id)
        task = ctx.get("task_id") or self.task_id
        run = ctx.get("run_id") or self.run_id

        return {
            "user_id": self.user_id,
            "total": total,
            "success_count": success_count,
            "failed_count": failed_count,
            "pending_count": pending_count,
            # legacy aliases (保留，便于旧代码使用)
            "ok": success_count,
            "fail": failed_count,
            "pending": pending_count,
            "categories": categories,
            "trace_id": trace,
            "task_id": task,
            "run_id": run,
        }

    # —— Payload —— #
    @with_trace(action_name="result_payload", phase="payload")
    def as_report_payload(self) -> Dict[str, Any]:
        """
        返回给上报/持久化的 payload，字段名稳定：
        {
          "user_id": int,
          "trace_id": str,
          "task_id": Optional[str],
          "run_id": Optional[str],
          "counts": {"success": int, "failed": int, "pending": int},
          "categories": {...},
        }
        """
        s = self.summary()
        return {
            "user_id": self.user_id,
            "trace_id": s.get("trace_id"),
            "task_id": s.get("task_id"),
            "run_id": s.get("run_id"),
            "counts": {
                "success": int(s.get("success_count", 0)),
                "failed": int(s.get("failed_count", 0)),
                "pending": int(s.get("pending_count", 0)),
            },
            "categories": s.get("categories", {}),
            "total": int(s.get("total", 0)),
            "timeline": list(self._timeline[-100:]),
        }

    # —— 状态 —— #
    @with_trace(action_name="result_status", phase="status")
    def get_status(self) -> TaskStatus:
        """
        根据当前汇总判断任务状态：
        - 若有 failed 且没有 success => FAILED
        - 若有 both success & failed => RUNNING
        - 若 only success => COMPLETED
        - 否则 PENDING
        """
        s = self.summary()
        sc = int(s.get("success_count", 0))
        fc = int(s.get("failed_count", 0))
        if fc and not sc:
            return TaskStatus.FAILED
        if sc and fc:
            return TaskStatus.RUNNING
        if sc:
            return TaskStatus.COMPLETED
        return TaskStatus.PENDING

    def partially_ok(self) -> bool:
        s = self.summary()
        return int(s.get("success_count", 0)) > 0 and int(s.get("failed_count", 0)) > 0

    def __repr__(self) -> str:
        s = self.summary()
        return f"<ResultTracker user:{self.user_id} ✅:{s.get('success_count', 0)} ❌:{s.get('failed_count', 0)} trace:{s.get('trace_id')}>"
