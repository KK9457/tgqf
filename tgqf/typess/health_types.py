# typess/health_types.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from enum import Enum
from typing import TypedDict, Any, Optional, Iterable, Dict

__all__ = [
    "HealthState",
    "HealthInfo",
    "make_health_info",
    "worst_state",
    "best_state",
]

# ---- 模块级常量（避免放在 Enum 类体内被枚举机制干扰） ----
_ZH_MAP: Dict[str, str] = {
    "ok":            "正常",
    "auth_expired":  "授权失效",
    "network_error": "网络异常",
    "flood_wait":    "限流等待",
    "banned":        "封号/冻结",
    "limited":       "限制功能",
    "unknown":       "未知",
}

_EMOJI_MAP: Dict[str, str] = {
    "ok":            "⭐️",
    "auth_expired":  "🔒",
    "network_error": "🌐",
    "flood_wait":    "⏳",
    "banned":        "☠️",
    "limited":       "⚠️",
    "unknown":       "❔",
}

_SEVERITY: Dict[str, int] = {
    # 数值越大越“糟糕”
    "ok":            0,
    "limited":       2,
    "flood_wait":    3,
    "network_error": 4,
    "auth_expired":  5,
    "banned":        6,
    "unknown":       7,
}


class HealthState(str, Enum):
    OK           = "ok"
    AUTH_EXPIRED = "auth_expired"
    NETWORK_ERROR= "network_error"
    FLOOD_WAIT   = "flood_wait"
    BANNED       = "banned"
    LIMITED      = "limited"
    UNKNOWN      = "unknown"

    # ---- 展示与权重 ----
    def zh_name(self) -> str:
        return _ZH_MAP.get(self.value, "未知")

    def emoji(self) -> str:
        return _EMOJI_MAP.get(self.value, "❔")

    def label(self) -> str:
        return f"{self.emoji()} {self.zh_name()}"

    def severity(self) -> int:
        return _SEVERITY.get(self.value, 7)

    def is_ok(self) -> bool:
        return self is HealthState.OK

    def is_degraded(self) -> bool:
        """可恢复但降级：受限/限流/网络问题等。"""
        return self in {HealthState.LIMITED, HealthState.FLOOD_WAIT, HealthState.NETWORK_ERROR}

    def is_fatal(self) -> bool:
        """不可继续（需要人工或重新登录）。"""
        return self in {HealthState.AUTH_EXPIRED, HealthState.BANNED}

    @classmethod
    def parse(cls, v: Any) -> "HealthState":
        """
        解析为 HealthState：
        - 已是枚举 → 原样返回
        - None/空串 → UNKNOWN
        - 字符串（大小写不敏感）→ 枚举；支持若干业务同义词
        - 非法值 → UNKNOWN
        """
        if isinstance(v, cls):
            return v
        if v is None:
            return cls.UNKNOWN
        try:
            s = str(v).strip().lower()
            if not s:
                return cls.UNKNOWN
            alias = {
                "healthy": "ok",
                "expired": "auth_expired",
                "timeout": "network_error",
                "throttled": "flood_wait",
                "blocked": "banned",
                "restricted": "limited",
                "limit": "limited",
            }
            s = alias.get(s, s)
            return cls(s)
        except Exception:
            return cls.UNKNOWN


class HealthInfo(TypedDict):
    state: HealthState   # 规范化状态
    emoji: str           # 表情
    zh: str              # 中文名
    label: str           # 组合标签："{emoji} {zh}"
    online: str          # "在线" / "离线" / "未知"


def _to_online_text(v: Optional[Any]) -> str:
    """统一 Online 文案：True/false/'online'/'offline'/None → '在线'/'离线'/'未知'。"""
    if isinstance(v, bool):
        return "在线" if v else "离线"
    if v is None:
        return "未知"
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "online"}:
        return "在线"
    if s in {"0", "false", "no", "n", "offline"}:
        return "离线"
    return "未知"


def make_health_info(state: HealthState | str | None, *, online: Any = None) -> HealthInfo:
    """
    生成标准化 HealthInfo，供 UI/监控统一展示。
    示例：
        make_health_info("flood_wait", online=True)
        -> { state: FLOOD_WAIT, emoji:"⏳", zh:"限流等待", label:"⏳ 限流等待", online:"在线" }
    """
    st = HealthState.parse(state)
    online_txt = _to_online_text(online)
    if st.is_fatal():   # 🔒 授权失效 / ☠️ 封号/冻结
        online_txt = "离线"
    return {
        "state": st,
        "emoji": st.emoji(),
        "zh": st.zh_name(),
        "label": st.label(),
        "online": online_txt,
    }


# --------- 多状态聚合（选最坏/最好） ---------
def worst_state(states: Iterable[HealthState | str | None]) -> HealthState:
    """
    从若干状态中挑选“最坏”的一个（severity 最大）。
    空序列时返回 UNKNOWN。
    """
    chosen = HealthState.UNKNOWN
    worst_val = -1
    for st in states or []:
        s = HealthState.parse(st)
        sv = s.severity()
        if sv > worst_val:
            worst_val = sv
            chosen = s
    return chosen


def best_state(states: Iterable[HealthState | str | None]) -> HealthState:
    """
    从若干状态中挑选“最好”的一个（severity 最小）。
    空序列时返回 UNKNOWN。
    """
    chosen = HealthState.UNKNOWN
    best_val = 10**9
    for st in states or []:
        s = HealthState.parse(st)
        sv = s.severity()
        if sv < best_val:
            best_val = sv
            chosen = s
    return chosen
