# -*- coding: utf-8 -*-
# typess/fsm_keys.py
from __future__ import annotations

from enum import Enum
from typing import Tuple, Iterable, Optional, Final

__all__ = [
    "FSMStage",
    # 字段常量
    "STAGE", "STATUS", "GROUP_LIST", "MESSAGE", "INTERVAL", "WHITELIST", "USERNAME",
    "USER_ID", "TASK_ID", "TRACE_ID", "PHONE", "NAME", "USERNAME_LOG",
    "GROUPS", "GROUP_COUNT",
    # 流程&工具
    "STAGES_ORDER",
    "stage_from",
    "stage_index",
    "is_terminal_stage",
    "is_valid_transition",
    "next_stage",
    "prev_stage",
    "format_progress",
]

K_PEER_CACHE = "user:{uid}:peer_cache"            # Hash: key -> json(InputPeer)
K_BLACKLIST_TARGETS = "user:{uid}:blacklist"      # Set: 永久无效目标（pl.cache_key()/用户名/邀请hash）
class FSMStage(str, Enum):
    INIT           = "init"
    INTERVAL_READY = "interval_ready"
    MESSAGE_READY  = "message_ready"
    GROUP_READY    = "group_ready"
    READY_TO_RUN   = "ready_to_run"   # ✅ 新阶段名（语义：配置就绪，准备运行）
    FINISHED       = "finished"

    # —— 静态映射（缓存，避免重复构造）—— #
    _ZH_MAP = {
        INIT:           "初始化",
        INTERVAL_READY: "间隔已设置",
        MESSAGE_READY:  "消息已配置",
        GROUP_READY:    "群组已导入",
        READY_TO_RUN:   "配置就绪",       # ✅ 更新中文名

        FINISHED:       "已完成",
    }
    _EMOJI_MAP = {
        INIT:           "🧭",
        INTERVAL_READY: "⏱️",
        MESSAGE_READY:  "📝",
        GROUP_READY:    "👥",
        READY_TO_RUN:   "🚦",

        FINISHED:       "✅",
    }

    def zh_name(self) -> str:
        return self._ZH_MAP.get(self, "未知阶段")

    def emoji(self) -> str:
        return self._EMOJI_MAP.get(self, "❓")

    def format(self) -> str:
        return f"{self.emoji()} {self.zh_name()}"

    # —— 进度工具 —— #
    def progress(self) -> Tuple[int, int, float]:
        """
        返回 (当前序号，从1开始, 总阶段数, 进度比例0~1)
        """
        idx = stage_index(self) + 1
        total = len(STAGES_ORDER)
        ratio = 0.0 if total <= 1 else round(idx / total, 4)
        return idx, total, ratio

    # —— 解析工具 —— #
    @classmethod
    def parse(cls, v: object, default: "FSMStage" = None) -> "FSMStage":
        """
        宽松解析：枚举/同名字符串/大小写不敏感；失败返回 default（缺省 INIT）
        """
        if default is None:
            default = FSMStage.INIT
        if isinstance(v, cls):
            return v
        if v is None:
            return default
        try:
            s = str(v).strip().lower()
            # 合法值兜底
            return cls(s) if s in {x.value for x in cls} else default
        except Exception:
            return default


# —— 标准阶段顺序（用于进度&校验）—— #
STAGES_ORDER: Final[Tuple[FSMStage, ...]] = (
    FSMStage.INIT,
    FSMStage.INTERVAL_READY,
    FSMStage.MESSAGE_READY,
    FSMStage.GROUP_READY,
    FSMStage.READY_TO_RUN,
    FSMStage.FINISHED,
)


def stage_from(v: object, *, default: FSMStage = FSMStage.INIT) -> FSMStage:
    """外部统一入口：宽松解析为 FSMStage。"""
    return FSMStage.parse(v, default=default)


def stage_index(stage: FSMStage) -> int:
    """返回阶段在 STAGES_ORDER 中的索引；未知则 -1（理论不会发生）。"""
    try:
        return STAGES_ORDER.index(stage)
    except ValueError:
        return -1


def is_terminal_stage(stage: FSMStage) -> bool:
    """是否已到终态（FINISHED）。"""
    return stage is FSMStage.FINISHED


def is_valid_transition(curr: FSMStage, nxt: FSMStage, *, allow_same: bool = True, allow_skip: bool = True) -> bool:
    """
    判断从 curr → nxt 是否“合理”：
      - allow_same: 允许不变（重复设置）
      - allow_skip: 允许跨阶段前进（例如一次性从 INIT 到 GROUP_READY）
    约束：不允许逆序跨越超过 1 步（避免大幅回退），但允许回退 1 步（修正配置）。
    """
    if curr == nxt:
        return allow_same
    i, j = stage_index(curr), stage_index(nxt)
    if i < 0 or j < 0:
        return False
    # 前进
    if j > i:
        return True if allow_skip else (j == i + 1)
    # 回退：最多 1 步
    return (i - j) <= 1


def next_stage(curr: FSMStage) -> FSMStage:
    """返回下一阶段；末尾则原样返回。"""
    i = stage_index(curr)
    return STAGES_ORDER[min(i + 1, len(STAGES_ORDER) - 1)] if i >= 0 else curr


def prev_stage(curr: FSMStage) -> FSMStage:
    """返回上一步；已是起点则原样返回。"""
    i = stage_index(curr)
    return STAGES_ORDER[max(i - 1, 0)] if i >= 0 else curr


def format_progress(stage: FSMStage) -> str:
    """格式化为 `📝 消息已配置 · 3/6 (50.0%)`"""
    idx, total, ratio = stage.progress()
    pct = f"{round(ratio * 100, 1)}%"
    return f"{stage.format()} · {idx}/{total} ({pct})"


# ===== FSM Task 字段名常量（与 FSM/TaskControl/Reporter 保持一致）=====
STAGE       = "stage"
STATUS      = "status"
GROUP_LIST  = "group_list"
MESSAGE     = "message"
INTERVAL    = "interval"
WHITELIST   = "whitelist"
USERNAME    = "username"

USER_ID     = "user_id"
TASK_ID     = "task_id"
TRACE_ID    = "trace_id"
PHONE       = "phone"
NAME        = "name"
USERNAME_LOG= "username"   # 日志展示同 key（与 FSM 的 USERNAME 同名字符串）

# —— 分组/群组信息 —— #
GROUPS      = "groups"       # 展示用群组列表
GROUP_COUNT = "group_count"  # 展示用群组数量
