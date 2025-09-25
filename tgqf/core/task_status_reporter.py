# -*- coding: utf-8 -*-
# core/task_status_reporter.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union, Deque, Callable
import html, asyncio
import json
import re
import time
import random
from collections import deque

from typess.join_status import JoinCode
from telethon import TelegramClient
from redis.asyncio import Redis

from unified.logger import log_info, log_warning, log_exception, log_debug
from unified.trace_context import get_trace_id, get_log_context, set_log_context, with_trace
from unified.ui_kit import UiKit
from core.defaults.bot_utils import BotUtils
from typess.message_enums import TaskStatus
from typess.link_types import ParsedLink

SHOW_ERROR_STATS = False  # 关闭错误代码/分类的详细展示（卡片更简洁）

__all__ = ["TaskStatusReporter"]

# ------------------------------------------------------------
# Redis Keys（多用户隔离）
# ------------------------------------------------------------
K_STATUS   = "user:{uid}:task:status"
K_RESULT   = "user:{uid}:task:result"
K_PROGRESS = "user:{uid}:task:progress"
K_AUDIT    = "audit_log:{uid}"
K_ONCE_USERNAME_INVALID = "tg:notify:username_invalid:{uid}:{username}"
K_PENDING_APPROVAL      = "user:{uid}:pending:approval"
K_PENDING_REJOIN        = "user:{uid}:pending:rejoin"
K_PENDING_REASSIGN      = "user:{uid}:pending:reassign"


@dataclass
class FailureCategory:
    name: str
    icon: str
    pattern: re.Pattern
    entries: List[str] = field(default_factory=list)


def _failure_categories() -> List[FailureCategory]:
    return [
        FailureCategory("群组失效或错误", "📛", re.compile(r"invalid|链接无效|peer_id_invalid|not occupied|username|invitation|expired", re.I)),
        FailureCategory("被限流/慢速",   "⏱️", re.compile(r"flood|限流|slow\s*mode|too\s*fast", re.I)),
        FailureCategory("权限/私有/禁言", "🔒", re.compile(r"write forbidden|private|banned_in_channel|not participant|readonly", re.I)),
        FailureCategory("格式/实体错误", "🔤", re.compile(r"(entity.*invalid)|(invalid.*entity)", re.I)),
        FailureCategory("发送失败",     "🚫", re.compile(r"send_failed|failure|error", re.I)),
        FailureCategory("掉线或连接失败", "🔌", re.compile(r"掉线|disconnect|未连接|network|timeout", re.I)),
    ]


# ------------------------------------------------------------
# 小工具
# ------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _elapsed_ms(ts: int) -> int:
    return max(0, _now_ms() - ts)

def _h(s: Optional[str]) -> str:
    """HTML 安全转义（不转义引号，便于 URL 显示）"""
    return html.escape(s or "", quote=False)

def _ensure_https_tme(url: str) -> str:
    s = str(url or "").strip()
    if not s:
        return s
    if s.startswith("@"):
        return f"https://t.me/{s[1:]}"
    if s.startswith("t.me/"):
        return f"https://{s}"
    if s.startswith("http://t.me/"):
        return "https://" + s[len("http://"):]
    return s


# -------------------------------------------------------------------------
# Link 安全工具（解析失败降级为原字符串，保证通知不丢）
# -------------------------------------------------------------------------
def ensure_parsed_link(link: Union[str, "ParsedLink"]) -> "ParsedLink":
    try:
        from typess.link_types import ParsedLink as _PL
        return link if isinstance(link, _PL) else _PL.parse(str(link))
    except Exception:
        class _FakePL:
            def __init__(self, raw: str) -> None:
                self._raw = str(raw or "")
                self.title = None
                self.username = None
            def short(self) -> str:
                return self._raw[:64]
            def to_link(self) -> str:
                return self._raw
        return _FakePL(str(link or ""))  # type: ignore[return-value]

def safe_short(link: Union[str, "ParsedLink"]) -> str:
    try:
        pl = ensure_parsed_link(link)
        return pl.short()
    except Exception:
        return str(link or "")[:64]


# ------------------------------------------------------------
# TaskStatusReporter
# ------------------------------------------------------------
class TaskStatusReporter:
    def __init__(
        self,
        event: Optional[Any] = None,
        redis_client: Optional[Redis] = None,
        bot: Optional["TelegramClient"] = None,
    ):
        # 统计缓存（可选）
        self.success: List[str] = []
        self.failed: Dict[str, str] = {}
        self.cancelled: Dict[str, str] = {}
        self.skipped: List[str] = []

        # 状态/结果缓存（进程内）
        self.status_map: Dict[int, str] = {}
        self.summary_result: Dict[int, List[Dict[str, Any]]] = {}
        self._default_user_id: Optional[int] = None
        self.results_by_user: Dict[int, List[Dict[str, Any]]] = {}

        self.event = event
        self.bot = bot
        self.redis: Optional[Redis] = redis_client
        self._title_cache: Dict[str, str] = {}

        # === Ring buffer for reliable reporting ===
        self._rb: Deque[dict] = deque(maxlen=2000)
        self._rb_task: Optional[asyncio.Task] = None
        self._rb_jitter: float = 0.25
        self._rb_running: bool = False

        # 错误码中文映射
        self.ERROR_CODE_LABELS: Dict[str, str] = {
            "ok": "成功",
            "completed": "已完成",
            "skip_invalid_link": "跳过无效链接",
            # 权限/只读/私有
            "readonly_channel": "管理权限不足",
            "admin_required": "管理权限不足",
            "banned_in_channel": "已被拉黑",
            "channel_private": "已被踢出/拉黑",
            "chat_write_forbidden": "没有发言权",
            # 对象/链接无效
            "username_not_occupied": "目标不存在",
            "invite_invalid": "无效邀请链接",
            "invite_expired": "无效邀请链接",
            "chat_id_invalid": "无效目标",
            "channel_invalid": "无效目标",
            "peer_id_invalid": "无效目标",
            "from_peer_unresolvable": "转发的源头异常",
            "not_in_group": "不在群中",
            # 限流/慢速/容量
            "slowmode": "慢速限制",
            "flood_wait": "限流等待",
            "peer_flood": "Peer限流",
            "flood": "限流",
            "channels_too_much": "加入的频道/群组数量已达上限",
            # 计划消息/论坛
            "schedule_date_too_late": "设置的时间太久",
            "schedule_too_much": "设置的消息太多",
            "topic_deleted": "话题已删除",
            # 媒体/消息ID/发送权限
            "media_empty": "不支持媒体",
            "grouped_media_invalid": "相册消息异常",
            "message_id_invalid": "无效消息ID",
            "message_ids_empty": "消息不存在",
            "chat_send_media_forbidden": "不允许发送媒体",
            "chat_send_gifs_forbidden": "不允许发送 GIF",
            "chat_send_stickers_forbidden": "不允许发送贴纸",
            "chat_send_plain_forbidden": "不允许发送普通消息/转发",
            # 用户状态/关系
            "user_deleted": "目标用户不存在",
            "user_is_bot": "对方是机器人",
            "user_is_blocked": "你已被对方拉黑",
            "you_blocked_user": "你拉黑了对方",
            # 授权/连接/系统
            "auth_invalid": "无效授权",
            "session_conflict": "会话在多端同时登入",
            "cdn_method_invalid": "CDN 方法无效",
            "api_id_invalid": "无效API",
            "device_model_empty": "设备型号异常",
            "lang_pack_invalid": "预设语言异常",
            "connection_not_inited": "未初始化",
            "system_empty": "系统信息异常",
            "input_layer_invalid": "协议层异常",
            # 其它
            "payment_required": "需要付费",
            "timeout": "异常超时",
            "network_dc_error": "网络/DC异常",
            "random_id_duplicate": "随机ID异常",
            "random_id_invalid": "无效随机ID ",
            # 协议/状态
            "need_member_invalid": "成员状态异常",
            "pts_change_empty": "数据同步暂不可用",
            # 投票/测验
            "poll_public_voters_forbidden": "不允许投票",
            "quiz_answer_missing": "转发测验前需先作答",
            # 审批/待定
            "join_request_sent": "已提交申请，等待管理员批准",
        }

        # 错误码 → 分类（用于聚合统计）
        self.ERROR_CATEGORY_MAP: Dict[str, set] = {
            "🔒权限/私有/禁言": {
                "readonly_channel", "admin_required", "banned_in_channel",
                "channel_private", "chat_write_forbidden",
            },
            "📛目标/链接无效": {
                "username_not_occupied", "invite_invalid", "invite_expired",
                "chat_id_invalid", "channel_invalid", "peer_id_invalid",
                "from_peer_unresolvable", "not_in_group",
            },
            "⏱️限流/慢速/数量上限": {
                "slowmode", "flood_wait", "peer_flood", "flood", "channels_too_much",
            },
            "🗓️计划消息/论坛": {
                "schedule_date_too_late", "schedule_too_much", "topic_deleted",
            },
            "🎞️媒体/消息ID/权限": {
                "media_empty", "grouped_media_invalid", "message_id_invalid",
                "message_ids_empty", "chat_send_media_forbidden",
                "chat_send_gifs_forbidden",
                "chat_send_stickers_forbidden",
                "chat_send_plain_forbidden",
            },
            "👤用户状态/关系": {
                "user_deleted", "user_is_bot", "user_is_blocked", "you_blocked_user",
            },
            "🔑授权/连接": {
                "auth_invalid", "session_conflict", "cdn_method_invalid", "api_id_invalid",
                "device_model_empty", "lang_pack_invalid", "connection_not_inited",
                "system_empty", "input_layer_invalid",
                "need_member_invalid", "pts_change_empty",
            },
            "💳付费限制": {"payment_required"},
            "🔌超时/网络": {"timeout", "network_dc_error"},
            "📊投票/测验": {"poll_public_voters_forbidden", "quiz_answer_missing"},
            "⏳审批/待定": {"join_request_sent"},
        }

    # -------- 注入/上下文 ----------
    async def ensure_bot(self):
        if not self.bot:
            try:
                from core.bot_client import get_notify_bot
                self.bot = get_notify_bot()
            except Exception as e:
                log_warning(f"ensure_bot 失败: {e}")

    # ------------------------------------------------------------
    # Ring-buffer & retry loop
    # ------------------------------------------------------------
    def _ensure_retry_loop(self) -> None:
        if self._rb_task and not self._rb_task.done():
            return
        self._rb_running = True
        self._rb_task = asyncio.create_task(self._retry_loop())

    def _enqueue(self, kind: str, payload: dict, *, max_attempts: int = 5, base_delay: float = 1.0) -> None:
        item = {
            "kind": kind,
            "payload": payload,
            "attempts": 0,
            "max_attempts": max_attempts,
            "next_ts": time.time(),
            "base_delay": base_delay,
        }
        self._rb.append(item)
        self._ensure_retry_loop()

    async def _retry_loop(self) -> None:
        while self._rb_running:
            now = time.time()
            fired = False
            for _ in range(len(self._rb)):
                it = self._rb[0]
                if it["next_ts"] <= now:
                    self._rb.popleft()
                    fired = True
                    ok = await self._process_rb_item(it)
                    if not ok:
                        # backoff
                        it["attempts"] += 1
                        if it["attempts"] < it["max_attempts"]:
                            jitter = 1.0 + random.uniform(-self._rb_jitter, self._rb_jitter)
                            delay = (it["base_delay"] * (2 ** (it["attempts"] - 1))) * jitter
                            it["next_ts"] = now + min(60.0, delay)
                            self._rb.append(it)
                        else:
                            log_warning("report_retry_give_up", extra={"kind": it["kind"], "payload": str(it["payload"])[:160]})
                else:
                    # rotate
                    self._rb.rotate(-1)
            # idle sleep
            if not fired:
                await asyncio.sleep(0.5)

    async def _process_rb_item(self, it: dict) -> bool:
        kind = it.get("kind")
        p = it.get("payload") or {}
        try:
            if kind == "send":
                await self._send_immediate(**p)
                return True
            if kind == "audit":
                if self.redis:
                    await self.redis.lpush(p["key"], p["value"])  # type: ignore[index]
                    await self.redis.ltrim(p["key"], 0, 1000)
                return True
            if kind == "redis_set":
                if self.redis:
                    await self.redis.set(p["key"], p["value"])  # type: ignore[index]
                return True
            if kind == "redis_set_json":
                if self.redis:
                    await self.redis.set(p["key"], json.dumps(p["value"], ensure_ascii=False))  # type: ignore[index]
                return True
        except Exception as e:
            log_warning("report_retry_failed_once", extra={"kind": kind, "err": str(e)})
            return False
        return True

    def bind(self, *, event: Optional[Any] = None, redis_client: Optional[Redis] = None, bot: Optional["TelegramClient"] = None) -> "TaskStatusReporter":
        if event is not None:
            self.event = event
        if redis_client is not None:
            self.redis = redis_client
        if bot is not None:
            self.bot = bot
        return self

    def set_event(self, event) -> None:
        self.event = event

    def set_redis(self, redis_client: Redis) -> None:
        self.redis = redis_client

    def set_user(self, user_id: int) -> None:
        try:
            self._default_user_id = int(user_id)
        except Exception:
            self._default_user_id = None

    # -------- Redis 封装 ----------
    async def _rset(self, key: str, value: str) -> None:
        if self.redis:
            try:
                await self.redis.set(key, value)
                log_debug("redis_set", extra={"phase": "redis", "key": key})
            except Exception as e:
                log_warning(f"Redis set 失败: {e}", extra={"phase": "redis", "key": key, **(get_log_context() or {})})

    async def _rget(self, key: str) -> Optional[str]:
        """从 redis 读取一个字符串值；若为 bytes/bytearray 则解码；若为空则返回 None。"""
        if not self.redis:
            return None
        try:
            raw = await self.redis.get(key)
            if raw is None:
                log_debug("redis_get", extra={"phase": "redis", "key": key, "hit": False})
                return None
            if isinstance(raw, (bytes, bytearray)):
                try:
                    s = raw.decode("utf-8", errors="ignore")
                except Exception:
                    s = str(raw)
            else:
                s = str(raw)
            log_debug("redis_get", extra={"phase": "redis", "key": key, "hit": bool(s)})
            return s
        except Exception as e:
            log_warning(f"Redis get 失败: {e}", extra={"phase": "redis", "key": key, **(get_log_context() or {})})
            return None

    async def _rset_json(self, key: str, value: Any) -> None:
        if self.redis:
            try:
                await self.redis.set(key, json.dumps(value, ensure_ascii=False))
            except Exception as e:
                log_warning(f"Redis set_json 失败: {e}", extra={"key": key})

    async def _rget_json(self, key: str) -> Any:
        raw = await self._rget(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception as e:
            log_warning(f"Redis json 解析失败: {e}", extra={"phase": "redis", "key": key, **(get_log_context() or {})})
            return None

    # -------- 状态/结果缓存（按 user_id 隔离） ----------
    @with_trace(action_name="update_status", phase="status")
    async def update_status(self, user_id: int, status: Union[TaskStatus, str]) -> None:
        ts = _now_ms()
        text = status.value if isinstance(status, TaskStatus) else str(status)
        self.status_map[user_id] = text
        await self._rset(K_STATUS.format(uid=user_id), text)
        log_info("【任务状态】更新状态", extra={"user_id": user_id, "status": text, "elapsed_ms": _elapsed_ms(ts)})

    async def get_status_text(self, user_id: int) -> Optional[str]:
        if user_id in self.status_map:
            return self.status_map[user_id]
        val = await self._rget(K_STATUS.format(uid=user_id))
        if val:
            self.status_map[user_id] = val
        return val

    @with_trace(action_name="update_result", phase="result")
    async def update_result(self, user_id: int, results: List[Dict[str, Any]]) -> None:
        ts = _now_ms()
        self.summary_result[user_id] = results
        await self._rset_json(K_RESULT.format(uid=user_id), results)
        log_info("task_result_updated", extra={"user_id": user_id, "items": len(results or []), "elapsed_ms": _elapsed_ms(ts)})

    async def get_result(self, user_id: int) -> Any:
        if user_id in self.summary_result:
            return self.summary_result[user_id]
        val = await self._rget_json(K_RESULT.format(uid=user_id))
        if val is not None:
            self.summary_result[user_id] = val
        return val

    @with_trace(action_name="update_progress", phase="progress")
    async def update_progress(self, user_id: int, done: int, total: int) -> None:
        ts = _now_ms()
        data = {
            "done": max(0, done),
            "total": max(1, total),
            "percent": round(max(0, done) * 100.0 / max(1, total), 2),
        }
        await self._rset_json(K_PROGRESS.format(uid=user_id), data)
        log_debug("task_progress", extra={"user_id": user_id, **data, "elapsed_ms": _elapsed_ms(ts)})

    async def get_progress(self, user_id: int) -> Optional[Dict[str, Any]]:
        return await self._rget_json(K_PROGRESS.format(uid=user_id))

    # -------- 审计 ----------
    async def _log_audit(self, uid: int, text: str, data: Dict[str, Any]) -> None:
        if not self.redis:
            return
        entry = {"ts": int(time.time()), "user_id": uid, "trace_id": get_trace_id(), "message": text, "data": data}
        try:
            self._enqueue("audit", {"key": K_AUDIT.format(uid=uid), "value": json.dumps(entry, ensure_ascii=False)})
        except Exception as e:
            log_warning(f"audit_log_enqueue_failed: {e}", extra={"uid": uid})

    # -------- 发送（Telethon 官方参数直通） ----------
    async def _send(self, text: Any, *, to_user_id: int, **kwargs) -> None:
        """
        kwargs 会透传到 BotUtils.safe_respond：
          - parse_mode: "html"/"md" 等
          - link_preview: bool
          - buttons / file / reply_to
          - formatting_entities（或 entities，会在 BotUtils 内部做兼容转换）
        """
        if not to_user_id or int(to_user_id) <= 0:
            log_warning("notice_send_skip_invalid_target", extra={"to_user_id": to_user_id})
            return
        self._enqueue("send", {"text": text, "to_user_id": int(to_user_id), "kwargs": kwargs})

    async def _send_immediate(self, text: Any, *, to_user_id: int, kwargs: dict) -> None:
        try:
            await BotUtils.safe_respond(None, None, text, target_user_id=int(to_user_id), **(kwargs or {}))
            log_info("notice_sent", extra={"to_user_id": to_user_id})
        except Exception as e:
            raise e

    # -------- 去重一次性提示 ----------
    @with_trace(action_name="notify_once", phase="dedup")
    async def _notify_once(self, user_id: int, to_user_id: Optional[int], key: str, message_html: str, *, ex: int = 86400) -> None:
        if not (self.redis and key and message_html):
            return
        recv_uid = int(to_user_id or self._default_user_id or user_id)
        try:
            if await self.redis.set(key, "1", nx=True, ex=ex):
                log_debug("notify_once_fire", extra={"user_id": user_id, "to_user_id": recv_uid, "key": key})
                msg = UiKit.card("提示", [message_html], level="info", with_rule=True, ctx_footer=True)
                await self._send(msg, parse_mode="html", to_user_id=recv_uid)
            else:
                log_debug("notify_once_skip", extra={"user_id": user_id, "to_user_id": recv_uid, "key": key})
        except Exception as e:
            log_warning(f"_notify_once 失败: {e}", extra={"user_id": user_id, "to_user_id": recv_uid, "key": key})

    async def notify_username_invalid_once(self, user_id: int, username: str, *, to_user_id: Optional[int] = None) -> None:
        if not (self.redis and username):
            return
        key = K_ONCE_USERNAME_INVALID.format(uid=user_id, username=username)
        msg = f"⚠️ 用户名 @{_h(username)} 已失效，已缓存 24 小时内跳过"
        await self._notify_once(user_id, to_user_id, key, msg, ex=86400)

    # -------- 通用通知（兼容旧签名） --------
    async def notify_simple(self, user_id_or_title, title_or_lines=None, lines=None, *, level: str = "info", to_user_id: Optional[int] = None, target_user_id: Optional[int] = None, **send_kwargs) -> None:
        """
        支持两种调用：
          新： notify_simple(user_id:int, title:str, lines:list, level=..., to_user_id=..., **send_kwargs)
          旧： notify_simple("标题", ["行1", ...], level=..., target_user_id=..., **send_kwargs)
        同时支持 target_user_id（旧名）和 to_user_id（新名）。
        send_kwargs 透传到 Telethon（parse_mode/link_preview/buttons/file/reply_to/formatting_entities）
        """
        # 检查是否使用新签名
        if isinstance(user_id_or_title, int):
            user_id = int(user_id_or_title)
            title = title_or_lines or ""
            content_lines = lines or []
        else:
            # legacy: (title, lines)
            title = user_id_or_title or ""
            if isinstance(title_or_lines, (list, tuple)):
                content_lines = list(title_or_lines)
            elif isinstance(title_or_lines, str):
                content_lines = [title_or_lines]
            else:
                content_lines = list(lines or [])
            # 对 legacy 调用尽量走到默认 user 或 0（但记录 warning）
            user_id = int(self._default_user_id) if self._default_user_id is not None else 0
            log_warning("notify_simple: legacy call style detected; prefer notify_simple(user_id, title, lines, to_user_id=...)")

        recv_uid = int(to_user_id or target_user_id or self._default_user_id or user_id or 0)
        try:
            msg = UiKit.card(title or "-", list(content_lines), level=level, with_rule=True, ctx_footer=True)
            # 卡片是 HTML：默认指定 parse_mode=html（可在 send_kwargs 覆盖）
            if "parse_mode" not in send_kwargs:
                send_kwargs["parse_mode"] = "html"
            await self._send(msg, to_user_id=recv_uid, **send_kwargs)
        except Exception as e:
            log_warning(f"notify_simple 发送失败: {e}", extra={"user_id": user_id})

    async def notify_round_mix_result(self, user_id: int, *, to_user_id: Optional[int] = None) -> None:
        await self.notify_simple(user_id, "本轮发送部分成功", [UiKit.bullet("存在失败项，可稍后重试或检查账号/权限")], level="progress", to_user_id=to_user_id)

    async def notify_task_stopped(self, user_id_or_task_id, task_id: Optional[str] = None, *, to_user_id: Optional[int] = None, target_user_id: Optional[int] = None, **send_kwargs) -> None:
        """
        支持：
          - 新： notify_task_stopped(user_id:int, task_id=str, to_user_id=..., **send_kwargs)
          - 旧： notify_task_stopped(task_id=str, to_user_id=...)  (legacy)
        """
        if isinstance(user_id_or_task_id, int):
            user_id = int(user_id_or_task_id)
            tid = task_id or "-"
        else:
            # legacy call without explicit user_id
            tid = user_id_or_task_id or (task_id or "-")
            user_id = int(self._default_user_id) if self._default_user_id is not None else 0
            log_warning("notify_task_stopped: legacy call style detected; prefer notify_task_stopped(user_id, task_id=..., to_user_id=...)")

        recv_uid = int(to_user_id or target_user_id or self._default_user_id or user_id or 0)
        try:
            await self.notify_simple(user_id, "任务已停止", [UiKit.kv("任务", tid)], level="ok", to_user_id=recv_uid, **send_kwargs)
        except Exception as e:
            log_warning(f"notify_task_stopped 发送失败: {e}", extra={"user_id": user_id})

    async def notify_task_error(self, user_id_or_title, title_or_err=None, err=None, *, to_user_id: Optional[int] = None, target_user_id: Optional[int] = None, **send_kwargs) -> None:
        """
        支持两种调用：
          新： notify_task_error(user_id:int, title:str, err:Exception|str, to_user_id=..., **send_kwargs)
          旧： notify_task_error("标题", err, target_user_id=..., **send_kwargs)
        """
        if isinstance(user_id_or_title, int):
            user_id = int(user_id_or_title)
            title = title_or_err or "任务异常"
            error = err
        else:
            title = user_id_or_title or "任务异常"
            error = title_or_err
            user_id = int(self._default_user_id) if self._default_user_id is not None else 0
            log_warning("notify_task_error: legacy call style detected; prefer notify_task_error(user_id, title, err, to_user_id=...)")

        recv_uid = int(to_user_id or target_user_id or self._default_user_id or user_id or 0)
        try:
            why = self._friendly_error(str(error)) if error else "-"
            await self.notify_simple(user_id, title or "任务发生错误", [UiKit.bullet(why)], level="error", to_user_id=recv_uid, **send_kwargs)
        except Exception as e:
            log_warning(f"notify_task_error 内部发送失败: {e}", extra={"user_id": user_id})

    async def notify_assignment_fail(self, user_id: int, reason: Optional[str] = None, *, to_user_id: Optional[int] = None, target_user_id: Optional[int] = None, **send_kwargs) -> None:
        lines = [UiKit.bullet(reason)] if reason else []
        recv = int(to_user_id or target_user_id or self._default_user_id or user_id or 0)
        await self.notify_simple(user_id, "账号分配失败", lines, level="error", to_user_id=recv, **send_kwargs)

    async def notify_no_valid_clients(self, user_id_or_none=None, *, to_user_id: Optional[int] = None, **send_kwargs) -> None:
        """没有群组可分配时的提示：可传 user_id 或省略（使用默认 user）"""
        if isinstance(user_id_or_none, int):
            user_id = int(user_id_or_none)
        else:
            user_id = int(self._default_user_id) if self._default_user_id is not None else 0
            if user_id_or_none is not None:
                log_warning("notify_no_valid_clients: legacy/positional call detected")
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        try:
            log_info("❌ 无可用群组", extra={"user_id": user_id})
            msg = UiKit.card("无可用群组", [UiKit.bullet("任务未执行：没有群组可分配")], level="warn", with_rule=True)
            await self._send(msg, parse_mode="html", to_user_id=recv, **send_kwargs)
        except Exception as e:
            log_warning("notify_no_groups 失败", extra={"err": str(e), "user_id": user_id})

    # ---------- 事件类 UI 卡片 ----------
    async def notify_flood_wait(self, user_id: int, phone: str, seconds: int, *, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        title = "⏱️ 限流等待"
        lines = [
            UiKit.kv("账号", _h(phone)),
            UiKit.kv("等待", f"{int(seconds)} 秒"),
            UiKit.bullet("任务已自动暂停，倒计时结束后将自动恢复"),
        ]
        await self._send(UiKit.card(title, lines, level="warn", with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    async def notify_recovered(self, user_id: int, phone: str, *, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        await self._send(UiKit.card("✅ 已恢复", [UiKit.kv("账号", _h(phone))], level="ok", with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    async def notify_banned(self, user_id: int, phone: str, *, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        await self._send(UiKit.card("🛑 账号被封禁", [UiKit.kv("账号", _h(phone))], level="error", with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    async def notify_write_forbidden(self, user_id: int, chat_title: str, chat_link: str, *, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        lines = [UiKit.kv("群组", _h(chat_title)), UiKit.kv("链接", _ensure_https_tme(chat_link))]
        await self._send(UiKit.card("🔒 写权限被限制", lines, level="warn", with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    async def notify_join_approved(self, user_id: int, chat_title: str, chat_link: str, *, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        lines = [UiKit.kv("群组", _h(chat_title)), UiKit.kv("链接", _ensure_https_tme(chat_link))]
        await self._send(UiKit.card("✅ 入群审批通过", lines, level="ok", with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    async def notify_kicked(self, user_id: int, chat_title: str, chat_link: str, *, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        lines = [UiKit.kv("群组", _h(chat_title)), UiKit.kv("链接", _ensure_https_tme(chat_link)), UiKit.bullet("已尝试回群/登记待回群")]
        await self._send(UiKit.card("🦵 被移出群组", lines, level="error", with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    async def notify_rejoin_result(self, user_id: int, chat_title: str, chat_link: str, *, success: bool, to_user_id: Optional[int] = None) -> None:
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        lvl = "ok" if success else "warn"
        tip = "已回群并恢复发送" if success else "回群失败，已登记待分配"
        lines = [UiKit.kv("群组", _h(chat_title)), UiKit.kv("链接", _ensure_https_tme(chat_link)), UiKit.bullet(tip)]
        await self._send(UiKit.card("🔁 回群结果", lines, level=lvl, with_rule=True, ctx_footer=True), parse_mode="html", to_user_id=recv)

    # -------------------- 待恢复/待审批/待分配 登记 --------------------
    async def mark_approval_pending(self, user_id: int, chat_key: str) -> None:
        if self.redis:
            try:
                await self.redis.sadd(K_PENDING_APPROVAL.format(uid=user_id), chat_key)
            except Exception:
                pass

    async def mark_approval_done(self, user_id: int, chat_key: str) -> None:
        if self.redis:
            try:
                await self.redis.srem(K_PENDING_APPROVAL.format(uid=user_id), chat_key)
            except Exception:
                pass

    async def mark_rejoin_pending(self, user_id: int, chat_key: str) -> None:
        if self.redis:
            try:
                await self.redis.sadd(K_PENDING_REJOIN.format(uid=user_id), chat_key)
            except Exception:
                pass

    async def mark_reassign_pending(self, user_id: int, chat_key: str) -> None:
        if self.redis:
            try:
                await self.redis.sadd(K_PENDING_REASSIGN.format(uid=user_id), chat_key)
            except Exception:
                pass

    # -------------------- 核心：入群统计归并 --------------------
    @staticmethod
    def _is_trash_group(g: Any) -> bool:
        gs = (str(g or "").strip()).lower()
        if (not gs) or gs in {"-", "none", "null"}:
            return True
        if gs.endswith("/-") or gs in {"t.me/-", "https://t.me/-", "http://t.me/-"}:
            return True
        return False

    @staticmethod
    def _normalize_code(code: str) -> str:
        return (code or "").strip().lower()

    @staticmethod
    def _map_code_to_join(code: str) -> JoinCode:
        """
        将结果中的 code/status 映射为 JoinCode；仅用于群组层面合并判断。
        更全面地覆盖 invite/peer/权限/容量等错误码。
        """
        c = (code or "").strip().lower()
        if c == "already_in":
            return JoinCode.ALREADY_IN
        if c in {"success", "ok", "completed"}:
            return JoinCode.SUCCESS
        if c in {"pending", "join_request_sent"}:
            return JoinCode.PENDING
        if c == "timeout":
            return JoinCode.TIMEOUT
        if c in {
            "invite_invalid", "invite_expired", "username_not_occupied",
            "chat_id_invalid", "channel_invalid", "peer_id_invalid",
            "from_peer_unresolvable",
        }:
            return JoinCode.INVALID
        if c in {
            "flood_wait", "flood", "peer_flood", "slowmode",
            "channel_private", "readonly_channel", "admin_required",
            "banned_in_channel", "chat_write_forbidden", "channels_too_much",
            "not_in_group", "chat_send_plain_forbidden",
        }:
            return JoinCode.RESTRICTED
        # 其它一律归为 FAILURE
        return JoinCode.FAILURE

    @staticmethod
    def _better(a: JoinCode, b: JoinCode) -> JoinCode:
        """
        选更“好”的群组结果（决定群组层面的最终状态）。
        优先级：已在群 > 成功 > 待审批 > 受限 > 超时 > 无效 > 失败
        """
        order = [
            JoinCode.ALREADY_IN,
            JoinCode.SUCCESS,
            JoinCode.PENDING,
            JoinCode.RESTRICTED,
            JoinCode.TIMEOUT,
            JoinCode.INVALID,
            JoinCode.FAILURE,
        ]
        pos = {c: i for i, c in enumerate(order)}
        return a if pos[a] <= pos[b] else b

    def _reduce_groups(self, results: List[Dict[str, Any]]) -> Tuple[Dict[str, JoinCode], Dict[str, List[str]]]:
        """
        将明细结果按群组去重并归并为群组级状态。
        - 忽略空群组/占位符（"-"等）
        - 忽略明确的跳过记录：skip_invalid_link / invalid_link / bad_link
        返回：
          group_state: {group_key: JoinCode}
          group_reasons: {group_key: [reasons...]} （仅用于失败原因分类）
        """
        group_state: Dict[str, JoinCode] = {}
        group_reasons: Dict[str, List[str]] = {}

        for r in results or []:
            code = self._normalize_code(r.get("code") or r.get("status") or "")
            # 跳过无效记录
            if code in {"skip_invalid_link", "invalid_link", "bad_link"}:
                continue
            g_raw = r.get("group") or r.get("link")
            if self._is_trash_group(g_raw):
                continue
            g_key = safe_short(g_raw)

            jc = self._map_code_to_join(code)
            prev = group_state.get(g_key)
            group_state[g_key] = jc if prev is None else self._better(prev, jc)

            # 收集失败原因文本
            if jc in {JoinCode.RESTRICTED, JoinCode.INVALID, JoinCode.TIMEOUT, JoinCode.FAILURE}:
                reason = str(r.get("reason") or "").strip() or code
                if reason:
                    group_reasons.setdefault(g_key, []).append(reason)

        return group_state, group_reasons

    def track_result(self, item: Dict[str, Any]) -> None:
        """
        采集一条细粒度结果（成功/失败/跳过等），仅做内存聚合与可选审计。
        预期字段（尽量宽松）：user_id, phone, group, code/status, reason, ...
        """
        try:
            uid = int(item.get("user_id") or self._default_user_id or 0)
        except Exception:
            uid = int(self._default_user_id or 0)

        if uid <= 0:
            # 无法归属到某个 user，仍然容忍但不入 dict
            return

        lst = self.results_by_user.setdefault(uid, [])
        lst.append(dict(item))

        # 可选：写入审计日志（避免过量 I/O，这里只挑关键字段）
        try:
            data = {
                k: item.get(k) for k in (
                    "phone", "group", "code", "status", "reason",
                    "error", "error_code", "error_kind", "index", "total",
                    "msg_type", "link_kind", "task_id", "trace_id",
                )
            }
            asyncio.create_task(self._log_audit(uid, "track_result", data))  # fire-and-forget
        except Exception:
            pass

    # ---------------- 汇总通知 ----------------
    async def notify_group_join_summary(
        self,
        user_id: int,
        results: List[Dict[str, Any]],
        *,
        to_user_id: Optional[int] = None,
        target_groups_total: Optional[int] = None,
        accounts_total: Optional[int] = None,
        groups_scope: Optional[List[str]] = None,
    ) -> None:
        recv_uid = int(to_user_id or self._default_user_id or user_id)

        # —— 作用域过滤（若传入 groups_scope） —— #
        scope_set = {str(s).strip() for s in (groups_scope or []) if str(s).strip()}
        if scope_set:
            results = [r for r in (results or []) if str(r.get("group") or r.get("link") or "").strip() in scope_set]

        # 再做去重归并
        group_state, group_reasons = self._reduce_groups(results)

        total_groups = int(target_groups_total) if target_groups_total is not None else len(group_state)
        # 账号数：优先外部传入；否则以本轮所有明细统计唯一 phone（不受 groups_scope 影响），最后再回退当前 results
        if accounts_total is not None:
            total_accounts = int(accounts_total)
        else:
            all_detail = list(self.results_by_user.get(user_id, []))
            phones_all = {r.get("phone") for r in all_detail if r.get("phone")}
            total_accounts = len(phones_all) if phones_all else len({r.get("phone") for r in results if r.get("phone")})
        count_already = sum(1 for v in group_state.values() if v == JoinCode.ALREADY_IN)
        count_success = sum(1 for v in group_state.values() if v == JoinCode.SUCCESS)
        count_pending = sum(1 for v in group_state.values() if v == JoinCode.PENDING)
        count_fail = max(0, total_groups - count_already - count_success - count_pending)

        # 失败原因分类（按群组合并后的失败项）
        fail_reasons: List[str] = []
        if count_fail:
            for g, state in group_state.items():
                if state in {JoinCode.RESTRICTED, JoinCode.INVALID, JoinCode.TIMEOUT, JoinCode.FAILURE}:
                    fail_reasons.extend(group_reasons.get(g, []))
        reason_counts = self._categorize_failures(fail_reasons)
        # 去除“❓其它”
        reason_counts = {k: v for k, v in reason_counts.items() if k != "❓其它"}

        lines = [
            f"🎯目标群组：{total_groups} 👨🏻‍🌾账号数：{total_accounts}",
            f"🟢已在群：{count_already} ⭐️成功：{count_success} 👹失败：{count_fail}",
        ]
        if count_pending:
            lines.append(f"⏳待审批：{count_pending}")

        if reason_counts:
            lines.append("🔴失败原因分类：")
            for reason, cnt in reason_counts.items():
                lines.append(f"- {reason} × {cnt}")

        # —— 可选：错误代码 Top/分类统计（默认关闭） —— #
        if SHOW_ERROR_STATS:
            try:
                stats = self.categorize(results)
                cat_stats = {k[4:]: v for k, v in stats.items() if k.startswith("cat:")}
                code_stats = [(k, v) for k, v in stats.items() if k.startswith("code:")]
                code_stats.sort(key=lambda kv: kv[1], reverse=True)
                if cat_stats:
                    lines.append("📦错误分类统计：")
                    for cname, cnt in cat_stats.items():
                        lines.append(f"- {cname} × {cnt}")
                top = code_stats[:5]
                if top:
                    lines.append("🧷Top 错误代码：")
                    for k, cnt in top:
                        _, rest = k.split(":", 1)
                        code, label = (rest.split("|", 1) + [""])[:2]
                        lines.append(f"- {label}（{code}）× {cnt}")
            except Exception:
                pass

        msg = UiKit.card("🚀 加群统计", lines, level="warn" if count_fail else "ok", with_rule=True)

        try:
            await self._send(msg, parse_mode="html", to_user_id=recv_uid)
        except Exception:
            log_warning("notify_group_join_summary 发送失败", extra={"user_id": user_id})
        self.results_by_user[user_id] = []

    # ---------------- 失败统计工具（供面板/轮次摘要） ----------------
    def categorize(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        对结果按“错误分类/错误码”做聚合统计（公开方法）。
        返回：
          - "cat:<分类名>" -> 计数
          - "code:<code>|<中文名>" -> 计数
        """
        per_code, per_cat = self._group_counts(results)
        out: Dict[str, int] = {}
        for cname, cnt in per_cat.items():
            out[f"cat:{cname}"] = cnt
        for code, cnt in per_code.items():
            label = self.ERROR_CODE_LABELS.get(code, code)
            out[f"code:{code}|{label}"] = cnt
        return out

    def _categories(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        """兼容旧接口：请改用 categorize()"""
        return self.categorize(results)

    def build_error_stats(self, user_id: int, *, top_n: int = 5) -> Dict[str, Any]:
        detail = list(self.results_by_user.get(user_id, []))
        per_code, per_cat = self._group_counts(detail)
        top = sorted(per_code.items(), key=lambda kv: kv[1], reverse=True)[:max(1, int(top_n))]
        top_fmt = [{"code": c, "label": self.ERROR_CODE_LABELS.get(c, c), "count": n} for c, n in top]
        cat_fmt = [{"category": k, "count": v} for k, v in per_cat.items()]
        total_errors = sum(per_code.values())
        return {
            "total_errors": total_errors,
            "per_category": cat_fmt,
            "per_code": [{"code": c, "label": self.ERROR_CODE_LABELS.get(c, c), "count": n} for c, n in per_code.items()],
            "top_codes": top_fmt,
        }

    def _group_counts(self, results: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, int]]:
        """基于 ERROR_CATEGORY_MAP & ERROR_CODE_LABELS 统计 (按 code, 按分类) 计数"""
        per_code: Dict[str, int] = {}
        for r in results or []:
            code = str(r.get("code") or r.get("status") or "").strip().lower() or "unknown"
            NON_ERROR_CODES = {"success", "already_in", "pending", "ok", "completed", "skip_invalid_link"}
            if code in NON_ERROR_CODES:
                continue
            per_code[code] = per_code.get(code, 0) + 1

        per_cat: Dict[str, int] = {}
        for code, cnt in per_code.items():
            matched = False
            for cname, codes in self.ERROR_CATEGORY_MAP.items():
                if code in codes:
                    per_cat[cname] = per_cat.get(cname, 0) + cnt
                    matched = True
            if not matched and code not in {"success", "already_in", "pending"} and code != "unknown":
                per_cat["❓其它"] = per_cat.get("❓其它", 0) + cnt
        return per_code, per_cat

    async def notify_no_groups(self, user_id_or_none=None, *, to_user_id: Optional[int] = None) -> None:
        if isinstance(user_id_or_none, int):
            user_id = int(user_id_or_none)
        else:
            user_id = int(self._default_user_id) if self._default_user_id is not None else 0
            if user_id_or_none is not None:
                log_warning("notify_no_groups: legacy/positional call detected")
        recv = int(to_user_id or self._default_user_id or user_id or 0)
        try:
            log_info("❌ 无可用群组", extra={"user_id": user_id})
            msg = UiKit.card("无可用群组", [UiKit.bullet("任务未执行：没有群组可分配")], level="warn", with_rule=True)
            await self._send(msg, parse_mode="html", to_user_id=recv)
        except Exception as e:
            log_warning("notify_no_groups 发送失败", extra={"err": str(e), "user_id": user_id})

    async def notify_round_summary(self, user_id: int, round_index: int, results: Union[str, Dict[str, Any], List[Dict[str, Any]]], *, to_user_id: Optional[int] = None) -> None:
        """
        群发轮次结果汇总（兼容 results 类型）
        """
        # —— 结果规范化 —— #
        if isinstance(results, dict):
            results = [results]
        elif isinstance(results, str):
            try:
                parsed = json.loads(results)
                if isinstance(parsed, dict):
                    results = [parsed]
                elif isinstance(parsed, list):
                    results = parsed
                else:
                    results = []
            except Exception:
                results = []
        elif not isinstance(results, list):
            results = []

        recv_uid = int(to_user_id or self._default_user_id or user_id)

        # groups_success / groups_failed 兼容处理
        groups: set[str] = set()
        for r in results:
            for f in (r.get("groups_success", []) + r.get("groups_failed", [])):
                if isinstance(f, dict):
                    g = f.get("group") or f.get("link") or str(f)
                else:
                    g = str(f)
                if g:
                    groups.add(g)

        total_groups = len(groups)
        total_accounts = len({r.get("phone") for r in results if r.get("phone")})
        count_success = sum(len(r.get("groups_success", [])) for r in results)
        count_fail = sum(len(r.get("groups_failed", [])) for r in results)

        reasons = []
        for r in results:
            for f in r.get("groups_failed", []):
                if isinstance(f, dict):
                    reasons.append(f.get("error") or "未知错误")
                else:
                    reasons.append(str(f))
        reason_counts = self._categorize_failures(reasons)
        # 去除“❓其它”
        reason_counts = {k: v for k, v in reason_counts.items() if k != "❓其它"}

        # —— 纠正展示轮次（以实际统计为准） —— #
        display_round = int(round_index or 0) or 1
        try:
            inferred = []
            for r in results:
                if isinstance(r, dict):
                    for k in ("round_index", "round", "round_no"):
                        v = r.get(k)
                        if isinstance(v, int) and v > 0:
                            inferred.append(v)
            if inferred:
                max_inferred = max(inferred)
                if display_round == 1 and max_inferred > 1:
                    display_round = max_inferred
        except Exception:
            pass

        lines = [
            f"⭐️ 第{display_round}轮 群发消息",
            f"🎯群组：{total_groups} 👨🏻‍🌾账号：{total_accounts} ⭐️成功：{count_success} 👹失败：{count_fail}"
        ]
        if reason_counts:
            lines.append("🔴失败原因分类：")
            for reason, cnt in reason_counts.items():
                lines.append(f"- {reason} × {cnt}")

        if SHOW_ERROR_STATS:
            try:
                detail_results = list(self.results_by_user.get(user_id, []))
                stats = self.categorize(detail_results)
                cat_stats = {k[4:]: v for k, v in stats.items() if k.startswith("cat:")}
                code_stats = [(k, v) for k, v in stats.items() if k.startswith("code:")]
                code_stats.sort(key=lambda kv: kv[1], reverse=True)
                if cat_stats:
                    lines.append("📦错误分类统计：")
                    for cname, cnt in cat_stats.items():
                        lines.append(f"- {cname} × {cnt}")
                top = code_stats[:5]
                if top:
                    lines.append("🧷Top 错误代码：")
                    for k, cnt in top:
                        _, rest = k.split(":", 1)
                        code, label = (rest.split("|", 1) + [""])[:2]
                        lines.append(f"- {label}（{code}）× {cnt}")
            except Exception:
                pass

        msg = UiKit.card("📊 群发结果", lines, level="warn" if count_fail else "ok", with_rule=True)
        try:
            await self._send(msg, parse_mode="html", to_user_id=recv_uid)
        except Exception:
            log_warning("notify_round_summary 发送失败", extra={"user_id": user_id})
        self.results_by_user[user_id] = []

    def _categorize_failures(self, reasons: List[str]) -> Dict[str, int]:
        cats = _failure_categories()
        unmatched: List[str] = []
        for r in reasons or []:
            matched = False
            for c in cats:
                if c.pattern.search(r):
                    c.entries.append(r)
                    matched = True
                    break
            if not matched:
                unmatched.append(r)
        result: Dict[str, int] = {}
        for c in cats:
            if c.entries:
                result[f"{c.icon}{c.name}"] = len(c.entries)
        if unmatched:
            result["❓其它"] = len(unmatched)
        return result

    # -------- 文案友好化 ----------
    def _friendly_error(self, s: str) -> str:
        if not s:
            return "-"
        s = s.strip()
        # 轻量归一：常见关键词替换
        s = re.sub(r"\bFLOOD(_WAIT)?\b", "限流", s, flags=re.I)
        s = re.sub(r"\bSLOW\s*MODE\b", "慢速模式", s, flags=re.I)
        s = s.replace("CHANNELS_TOO_MUCH", "加入频道/群组数量达上限")
        return s


# ==============================
# ✅ 模块级：入群统计汇总通知（最小骨架）
# ==============================
async def notify_group_join_summary(
    account_id: str,
    stats: Dict[str, Any],
    *,
    reporter: Optional[TaskStatusReporter] = None,
    user_id: Optional[int] = None,
    to_user_id: Optional[int] = None,
) -> None:
    """
    兼容“账号维度串行 Join 队列”结束时的汇总通知。
    - 必填：account_id, stats（来自 ResultTracker.snapshot(account_id=...)）
    - 可选：reporter + user_id（有则推送卡片；无则仅记录日志）

    stats 期望字段：
      total, ok, already, need_approve, forbidden, invalid, flood_hist{sec:count}, duration_ms
    """
    try:
        t = int(stats.get("total", 0))
        ok = int(stats.get("ok", 0))
        al = int(stats.get("already", 0))
        na = int(stats.get("need_approve", 0))
        fb = int(stats.get("forbidden", 0))
        iv = int(stats.get("invalid", 0))
        fh = dict(stats.get("flood_hist", {}))
        dur = int(stats.get("duration_ms", 0))

        lines = [
            UiKit.kv("账号", _h(account_id)),
            UiKit.kv("用时", f"{round(dur/1000,1)}s"),
            f"🎯目标：{t}  🟢已在群：{al}  ⭐️成功：{ok}  ⏳待审：{na}  👹失败：{max(0, t - ok - al - na)}",
        ]
        if fb or iv:
            lines.append(f"🔒权限/私有：{fb}  📛无效：{iv}")
        if fh:
            # 展示 FLOOD 秒数 Top-3 桶
            top = sorted(fh.items(), key=lambda kv: kv[1], reverse=True)[:3]
            top_s = "，".join(f"{sec}s×{cnt}" for sec, cnt in top if int(sec) > 0)
            if top_s:
                lines.append("⏱️限流分布：" + top_s)

        card = UiKit.card("🚀 加群统计（账号）", lines, level="warn" if (t - ok - al - na) > 0 else "ok", with_rule=True)

        if reporter and user_id:
            recv = int(to_user_id or reporter._default_user_id or user_id)
            await reporter._send(card, parse_mode="html", to_user_id=recv)
        else:
            log_info("notify_group_join_summary(log-only)", extra={
                "account_id": account_id, "total": t, "ok": ok, "already": al,
                "need_approve": na, "forbidden": fb, "invalid": iv, "flood_hist": fh, "duration_ms": dur,
            })
    except Exception as e:
        log_warning(f"notify_group_join_summary failed: {e}", extra={"account_id": account_id})