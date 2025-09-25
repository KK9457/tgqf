# typess/join_status.py
# -*- coding: utf-8 -*-
"""
统一 Join 状态枚举 / 上报结构 / 展示元数据

改进点（兼容现有 API）：
- 错误码集合常量化，便于与 send/telethon 映射保持一致
- prefer_join_code(a,b)：统一“更好”结果优先级（已在模块内固化）
- join_meta_from_error() 利用 meta（如 seconds）动态生成更贴切的文案
- JoinCode.from_reason() 使用预编译正则映射，稳健匹配多语言关键词
- 保留向后兼容的公开 API 与字段命名（外层/内层均有 "code" 字段）
"""
from __future__ import annotations
from enum import Enum
from typing import TypedDict, Literal, Optional, Dict, Any, cast, Tuple
from dataclasses import dataclass, field
import re

# -------------------- 常量定义（字段 Key） --------------------
USER_ID = "user_id"
TASK_ID = "task_id"
TRACE_ID = "trace_id"
PHONE = "phone"
NAME = "name"

CODE = "code"          # 顶层报告中的 code（保持向后兼容）
RESULT = "result"
REASON = "reason"
DETAIL = "detail"
EXCEPTION = "exception"
ERROR = "error"

JOIN_CODE = "code"     # 展示元数据中的 code（保持向后兼容）
JOIN_TEXT = "text"
JOIN_ICON = "icon"
JOIN_TITLE = "title"
JOIN_LEVEL = "level"

# -------------------- 错误码集合（对齐 send-stage/classifier） --------------------
SUCCESS_CODES    = frozenset({"success", "ok", "completed"})
ALREADY_CODES    = frozenset({"already_in"})
PENDING_CODES    = frozenset({"pending", "join_request_sent"})
TIMEOUT_CODES    = frozenset({"timeout"})
INVALID_CODES    = frozenset({
    "invite_invalid", "invite_expired", "username_not_occupied",
    "chat_id_invalid", "channel_invalid", "peer_id_invalid",
    "from_peer_unresolvable",
})
RESTRICTED_CODES = frozenset({
    "flood_wait", "flood", "peer_flood", "slowmode",
    "channel_private", "readonly_channel", "admin_required",
    "banned_in_channel", "chat_write_forbidden", "channels_too_much",
    "not_in_group", "chat_send_webpage_forbidden", "chat_send_videos_forbidden", "chat_send_plain_forbidden",
    "payment_required",
})

# 预编译的 “原因文本 → JoinCode” 映射（中英混搭关键词支持）
_REASON_MAP = [
    (re.compile(r"(?:pending|approval|等待|审批|处理中)", re.I), "PENDING"),
    (re.compile(r"(?:timeout|超时)", re.I), "TIMEOUT"),
    (re.compile(r"(?:invalid|过期|revoked|not\s*found)", re.I), "INVALID"),
    (re.compile(r"(?:restrict|受限|限流|no\s*rights|forbidden|private)", re.I), "RESTRICTED"),
    (re.compile(r"(?:already|已在|已加入)", re.I), "ALREADY_IN"),
]

# -------------------- Join 状态枚举 --------------------
class JoinCode(str, Enum):
    SUCCESS     = "success"
    ALREADY_IN  = "already_in"
    RESTRICTED  = "restricted"
    INVALID     = "invalid"
    TIMEOUT     = "timeout"
    FAILURE     = "failure"
    PENDING     = "pending"

    def zh_title(self) -> str:
        return {
            self.SUCCESS: "加入成功",
            self.ALREADY_IN: "已在群里",
            self.RESTRICTED: "受限/无权限",
            self.INVALID: "链接无效/过期",
            self.TIMEOUT: "加群超时",
            self.FAILURE: "入群失败",
            self.PENDING: "等待管理员审批",
        }[self]

    def icon(self) -> str:
        return {
            self.SUCCESS: "✅",
            self.ALREADY_IN: "🟢",
            self.RESTRICTED: "⛔️",
            self.INVALID: "📛",
            self.TIMEOUT: "⏱️",
            self.FAILURE: "🛑",
            self.PENDING: "⏳",
        }[self]

    def level(self) -> Literal["ok", "warn", "error", "progress", "info"]:
        return {
            self.SUCCESS: "ok",
            self.ALREADY_IN: "ok",
            self.RESTRICTED: "warn",
            self.INVALID: "error",
            self.TIMEOUT: "warn",
            self.FAILURE: "error",
            self.PENDING: "progress",
        }[self]

    def default_text(self) -> str:
        return {
            self.SUCCESS: "加入成功",
            self.ALREADY_IN: "已在群里",
            self.RESTRICTED: "账号受限或限流，请稍后重试",
            self.INVALID: "群链接已过期/无效",
            self.TIMEOUT: "加群超时",
            self.FAILURE: "未知原因",
            self.PENDING: "等待管理员审批",
        }[self]

    def is_ok(self) -> bool:
        return self in (self.SUCCESS, self.ALREADY_IN)

    def to_meta(self, *, text: Optional[str] = None) -> "JoinResultMeta":
        return join_result_meta(
            code=self.value,
            text=text or self.default_text(),
            icon=self.icon(),
            title=self.zh_title(),
            level=self.level(),
        )

    @classmethod
    def from_reason(cls, reason: Optional[str], ok: Optional[bool] = None) -> "JoinCode":
        """根据自然语言原因/提示粗略归类（兜底策略；使用预编译正则）。"""
        r = (reason or "").strip()
        if ok is True:
            # ok 明确为 True：若文本含“已在”，返回 ALREADY_IN，否则 SUCCESS
            if _REASON_MAP[-1][0].search(r):
                return cls.ALREADY_IN
            return cls.SUCCESS
        for pat, code_name in _REASON_MAP:
            if pat.search(r):
                return getattr(cls, code_name)
        return cls.FAILURE


# -------------------- 结果元数据结构 --------------------
class JoinResultMeta(TypedDict):
    code: str
    text: str
    icon: str
    title: str
    level: str


# ✅ 最小 JoinResult：与上游 ensure_join()/统计器契合（code + meta）
@dataclass
class JoinResult:
    code: JoinCode
    meta: Dict[str, Any] = field(default_factory=dict)

class JoinReportPayload(TypedDict, total=False):
    user_id: int
    task_id: str
    code: str
    result: JoinResultMeta
    reason: Optional[str]
    detail: Optional[Dict[str, Any]]

# -------------------- 工厂函数 --------------------
def join_result_meta(
    *,
    code: str,
    text: str,
    icon: str,
    title: str,
    level: str,
) -> JoinResultMeta:
    return cast(JoinResultMeta, {
        JOIN_CODE: code,
        JOIN_TEXT: text,
        JOIN_ICON: icon,
        JOIN_TITLE: title,
        JOIN_LEVEL: level,
    })


def join_report_payload(
    *,
    user_id: int,
    task_id: str,
    code: str,
    result: JoinResultMeta,
    reason: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> JoinReportPayload:
    payload: Dict[str, Any] = {
        USER_ID: user_id,
        TASK_ID: task_id,
        CODE: code,
        RESULT: result,
    }
    if reason:
        payload[REASON] = reason
    if detail:
        payload[DETAIL] = detail
    return cast(JoinReportPayload, payload)


def normalize_join_result(ok: bool, reason: Optional[str] = None) -> Tuple[str, str]:
    """
    归一化输出：(code, text)
    - code：JoinCode.value
    - text：优先 reason 的非空友好文本，否则回退默认文案
    """
    jc = JoinCode.from_reason(reason, ok)
    text = (reason or "").strip() or jc.default_text()
    return jc.value, text


# 预构建：枚举 → 展示元数据
STATUS_META: Dict[JoinCode, JoinResultMeta] = {jc: jc.to_meta() for jc in JoinCode}


# -------------------- 桥接：error_code/meta → JoinCode/Meta --------------------
def join_code_from_error(error_code: Optional[str], error_meta: Optional[Dict[str, Any]] = None) -> JoinCode:
    """
    将 send-stage/classifier 的 (code, meta) 映射为 JoinCode。
    分类规则与全局保持一致：
      - already_in/success/ok/completed
      - pending/join_request_sent
      - timeout
      - invalid:   邀请/用户名/ID 类
      - restricted: 限流/私有/权限/容量/未在群/慢速/付费
      - 其它 → failure
    """
    c = (error_code or "").strip().lower()
    if not c:
        return JoinCode.FAILURE
    if c in ALREADY_CODES:
        return JoinCode.ALREADY_IN
    if c in SUCCESS_CODES:
        return JoinCode.SUCCESS
    if c in PENDING_CODES:
        return JoinCode.PENDING
    if c in TIMEOUT_CODES:
        return JoinCode.TIMEOUT
    if c in INVALID_CODES:
        return JoinCode.INVALID
    if c in RESTRICTED_CODES:
        return JoinCode.RESTRICTED
    return JoinCode.FAILURE



def _format_text_with_meta(jc: JoinCode, code: Optional[str], meta: Optional[Dict[str, Any]]) -> str:
    base = jc.default_text()
    c = (code or "").lower()
    seconds = None
    try:
        if isinstance(meta, dict):
            seconds = int(meta.get("seconds") or 0) or None
    except Exception:
        seconds = None

    if c == "flood_wait" and seconds:
        return f"限流等待 {seconds} 秒"
    if c == "slowmode" and seconds:
        return f"慢速模式，需等待 {seconds} 秒后再试"
    if c == "peer_flood":
        return "请求过多，请稍后再试"
    if c == "channels_too_much":
        return "加入的频道/群组数量已达上限"
    if c == "channel_private":
        return "目标群组为私有或不可访问"
    if c == "chat_write_forbidden":
        return "没有发言权限"
    if c == "payment_required":
        return "需要支付或目标为付费内容"
    return base

def join_meta_from_error(
    error_code: Optional[str],
    error_meta: Optional[Dict[str, Any]] = None,
    *,
    text: Optional[str] = None,
) -> JoinResultMeta:
    """生成用于展示的 JoinResultMeta；可利用 meta 生成更友好的 text。"""
    jc = join_code_from_error(error_code, error_meta)
    final_text = text or _format_text_with_meta(jc, error_code, error_meta)
    return jc.to_meta(text=final_text)


# -------------------- 合并优先级（与 Reporter 对齐） --------------------
# 已在群 > 成功 > 待审批 > 受限 > 超时 > 无效 > 失败
_JOIN_ORDER = [
    JoinCode.ALREADY_IN,
    JoinCode.SUCCESS,
    JoinCode.PENDING,
    JoinCode.RESTRICTED,
    JoinCode.TIMEOUT,
    JoinCode.INVALID,
    JoinCode.FAILURE,
]
_JOIN_POS = {c: i for i, c in enumerate(_JOIN_ORDER)}


def prefer_join_code(a: JoinCode, b: JoinCode) -> JoinCode:
    """返回更“好”的 JoinCode（用于群组层合并）。"""
    return a if _JOIN_POS[a] <= _JOIN_POS[b] else b


__all__ = [
    "JoinCode",
    "JoinResultMeta",
    "JoinReportPayload",
    "join_result_meta",
    "join_report_payload",
    "normalize_join_result",
    "join_code_from_error",
    "join_meta_from_error",
    "prefer_join_code",
    "STATUS_META",
    "JoinResult",
    "SUCCESS_CODES",
    "ALREADY_CODES",
    "PENDING_CODES",
    "TIMEOUT_CODES",
    "INVALID_CODES",
    "RESTRICTED_CODES",
]
