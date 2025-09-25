# unified/logger.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import traceback
from collections import deque
from typing import Any, Deque, Dict, Iterable, Optional, Union, Set

from unified.config import LOG_DIR, LOG_FILE, LOG_FORMAT, LOG_PATH, DATE_FORMAT
from unified.trace_context import get_log_context

# 可选：Telethon 仅在启用 TG 发送时才真正使用
try:
    from telethon.tl import types as tl_types  # 类型解析辅助
except Exception:  # pragma: no cover - 运行环境没装 telethon 也不影响本地/文件/控制台日志
    tl_types = None

_global_logger: Optional[logging.Logger] = None
_logger_fingerprint: Optional[str] = None
_logger_initialized: bool = False
_RESERVED_FIELDS_CACHE: Optional[Set[str]] = None

# ============== 统一日志级别解析（支持环境变量覆盖） ============== #
_LEVEL_NAME_MAP: Dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def _resolve_level(value: Optional[Union[str, int]], fallback: int) -> int:
    if value is None:
        return int(fallback)
    if isinstance(value, int):
        return int(value)
    s = str(value).strip().upper()
    return _LEVEL_NAME_MAP.get(s, int(fallback))


DEFAULT_LOG_LEVEL = _resolve_level(os.getenv("LOG_LEVEL"), logging.INFO)
DEFAULT_CONSOLE_LOG_LEVEL = _resolve_level(os.getenv("LOG_LEVEL_CONSOLE"), DEFAULT_LOG_LEVEL)
DEFAULT_FILE_LOG_LEVEL = _resolve_level(os.getenv("LOG_LEVEL_FILE"), DEFAULT_LOG_LEVEL)
DEFAULT_TG_LOG_LEVEL = _resolve_level(os.getenv("LOG_LEVEL_TG"), logging.ERROR)

# 是否在日志“值”层面使用中文（status/phase/level/code/signal 等）
LOG_ZH = 1


def set_default_log_levels(
    *,
    global_level: Optional[Union[str, int]] = None,
    console_level: Optional[Union[str, int]] = None,
    file_level: Optional[Union[str, int]] = None,
    tg_level: Optional[Union[str, int]] = None,
) -> None:
    """运行时更新默认等级（下次 init_logger 生效）"""
    global DEFAULT_LOG_LEVEL, DEFAULT_CONSOLE_LOG_LEVEL, DEFAULT_FILE_LOG_LEVEL, DEFAULT_TG_LOG_LEVEL
    if global_level is not None:
        DEFAULT_LOG_LEVEL = _resolve_level(global_level, DEFAULT_LOG_LEVEL)
        if console_level is None:
            DEFAULT_CONSOLE_LOG_LEVEL = DEFAULT_LOG_LEVEL
        if file_level is None:
            DEFAULT_FILE_LOG_LEVEL = DEFAULT_LOG_LEVEL
    if console_level is not None:
        DEFAULT_CONSOLE_LOG_LEVEL = _resolve_level(console_level, DEFAULT_CONSOLE_LOG_LEVEL)
    if file_level is not None:
        DEFAULT_FILE_LOG_LEVEL = _resolve_level(file_level, DEFAULT_FILE_LOG_LEVEL)
    if tg_level is not None:
        DEFAULT_TG_LOG_LEVEL = _resolve_level(tg_level, DEFAULT_TG_LOG_LEVEL)


# ================= “丢最旧”的异步队列（非阻塞 put） ================= #
class DropOldestAsyncQueue:
    def __init__(self, maxlen: int = 2000, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._dq: Deque[str] = deque(maxlen=maxlen)
        self._cv = asyncio.Condition()
        self._loop = loop  # <-- 显式保存事件循环

    def put_nowait(self, item: str) -> None:
        if self._dq.maxlen is not None and len(self._dq) >= self._dq.maxlen:
            try:
                self._dq.popleft()
            except IndexError:
                pass
        self._dq.append(item)

        async def _notify():
            async with self._cv:
                self._cv.notify()

        # 总是尽量使用事件循环线程安全地唤醒等待者
        if self._loop and self._loop.is_running():
            try:
                self._loop.call_soon_threadsafe(lambda: asyncio.create_task(_notify()))
                return
            except Exception:
                pass

        # 无 loop 或失败则退化为“同步唤醒尝试”（极端场景仍有 1s 超时兜底）
        try:
            coro = _notify()
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)  # 同线程异步环境
            except RuntimeError:
                # 没有运行中的 loop，放弃即时唤醒（worker 有 1s timeout）
                pass
        except Exception:
            pass


    async def get(self) -> str:
        async with self._cv:
            while not self._dq:
                await self._cv.wait()
            return self._dq.popleft()

    def empty(self) -> bool:
        return not self._dq

    def qsize(self) -> int:
        return len(self._dq)


# 全局 TG 组件
_telegram_queue: Optional[DropOldestAsyncQueue] = None
_telegram_worker_task: Optional[asyncio.Task] = None
_telegram_stop_event: Optional[asyncio.Event] = None

# 供 Handler/worker 间传递的“禁止再转发到TG”的标记字段名
_NO_TG_FIELD = "_no_telegram"

# ============== Telegram 日志 Handler（10s 同类去重） ============== #
class TelegramLogHandler(logging.Handler):
    _re_ts = re.compile(r"\d{2}:\d{2}:\d{2}|\d{4}-\d{2}-\d{2}")

    def __init__(
        self,
        queue: DropOldestAsyncQueue,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        level=logging.ERROR,
        rate_window_sec: int = 10,
    ):
        super().__init__(level)
        self.queue = queue
        self.loop = loop
        self.rate_window_sec = max(1, int(rate_window_sec))
        self._last_sent: Dict[str, float] = {}  # key -> last_ts

    @classmethod
    def _normalize_for_rate(cls, text: str) -> str:
        t = cls._re_ts.sub("", text)
        t = re.sub(r"\s+", " ", t).strip()
        t = re.sub(r":\d+\b", ":#", t)  # 行号归一
        return t[:256]

    def _should_send(self, text: str) -> bool:
        now = time.monotonic()
        key = self._normalize_for_rate(text)
        last = self._last_sent.get(key)
        if last is not None and (now - last) < self.rate_window_sec:
            return False
        self._last_sent[key] = now
        return True

    def emit(self, record: logging.LogRecord):
        # 防递归：标注过 _NO_TG_FIELD 的记录不再转发到 TG
        if getattr(record, _NO_TG_FIELD, False):
            return
        try:
            msg = self.format(record)[:4000]
            if not self._should_send(msg):
                return
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self.queue.put_nowait, msg)
            else:
                self.queue.put_nowait(msg)
        except Exception as e:  # 实在失败就写到 stderr，避免抛出影响主流程
            try:
                import sys
                sys.stderr.write(f"[TelegramLogHandler emit error] {e}\n")
            except Exception:
                pass


# 解析形如 "U.123.456" / "C.123.456" 的实体编码
_ENTITY_STR_RE = re.compile(r"^([UBGCM E])\.(\d+)\.(-?\d+)$".replace(" ", ""))


def _peer_from_entity_str(s: str):
    """把 'T.id.hash' 风格的实体字符串解析为 InputPeerX（不依赖外部 Entity 类）"""
    if not tl_types:
        return None
    m = _ENTITY_STR_RE.match(str(s).strip())
    if not m:
        return None
    ty, id_s, h_s = m.groups()
    _id, _hash = int(id_s), int(h_s)
    if ty in ("U", "B"):
        return tl_types.InputPeerUser(_id, _hash)
    if ty == "G":
        return tl_types.InputPeerChat(_id)
    # C/M/E 归一为 Channel
    return tl_types.InputPeerChannel(_id, _hash)


async def _resolve_tg_target(client, target):
    """尽力把传入的 tg_target 解析为可以发送的对象。
    支持：事件对象、InputPeerX、'T.id.hash' 编码、@username、t.me 链接、纯数字 id、chat 对象
    解析失败返回 None
    """
    if target is None:
        return None

    # Event/Message：优先使用 chat_id
    chat_id = getattr(target, "chat_id", None)
    if chat_id is not None:
        return chat_id

    # 已是可用的 InputPeer
    if tl_types and isinstance(target, (tl_types.InputPeerUser, tl_types.InputPeerChannel, tl_types.InputPeerChat)):
        return target

    # T.id.hash 编码
    if isinstance(target, str):
        p = _peer_from_entity_str(target)
        if p is not None:
            return p

    # 其它情况交给 Telethon 解析（@username、链接、纯 id 等）
    try:
        return await client.get_input_entity(target)
    except Exception:
        return None


async def telegram_log_worker(tg_client, target, queue: DropOldestAsyncQueue, stop_event: asyncio.Event):
    """
    优雅关闭：
    - 收到 stop_event 后，继续 drain 队列，直至清空再退出。
    - 尽量直接用 client 发送到目标 chat；解析失败则回退到 Saved Messages。
    - 发送失败时，避免递归把错误再次投递到 TG（使用 _NO_TG_FIELD 标记）。
    """
    log = logging.getLogger("unified")

    # 先尝试连接（注意 is_connected() 是同步 bool；不要 await）
    try:
        if hasattr(tg_client, "is_connected") and not tg_client.is_connected():
            await tg_client.connect()
    except Exception:
        pass

    # 预解析目标，解析失败再在发送时回退
    resolved_target = None
    try:
        resolved_target = await _resolve_tg_target(tg_client, target)
    except Exception:
        resolved_target = None

    try:
        while True:
            if stop_event.is_set() and queue.empty():
                break
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                text = f"[日志]\n{msg}"

                # 支持事件（包含 respond）；否则直接 client 发送
                if hasattr(target, "respond"):
                    # 延迟导入，避免循环依赖开销
                    from core.defaults.bot_utils import BotUtils
                    await BotUtils.safe_respond(target, getattr(target, "client", None), text)
                else:
                    # 每次发送前都确保连接稳定
                    try:
                        if hasattr(tg_client, "is_connected") and not tg_client.is_connected():
                            await tg_client.connect()
                    except Exception:
                        pass

                    peer = resolved_target
                    if peer is None:
                        peer = await _resolve_tg_target(tg_client, target)
                    if peer is None:
                        peer = "me"

                    await tg_client.send_message(peer, message=text, parse_mode=None)
            except Exception as e:
                # 标注 _NO_TG_FIELD 避免再次进入 TG handler
                log.error(f"❌ 发送日志到Telegram失败: {e}", extra={_NO_TG_FIELD: True})

            await asyncio.sleep(0.3)
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logging.getLogger("unified").error("❌ Telegram日志worker异常: %s", exc, extra={_NO_TG_FIELD: True})


def get_global_logger() -> logging.Logger:
    global _global_logger
    if _global_logger is None:
        init_logger()
    return _global_logger


def set_global_logger(logger: logging.Logger) -> None:
    global _global_logger
    _global_logger = logger


# ============ Telethon 定向噪声过滤器：仅屏蔽 “Got difference for channel <id> updates” (INFO) ============ #
_TELETHON_DIFF_RE = re.compile(r"^Got difference for channel \d+ updates$")


class _TelethonChannelDiffFilter(logging.Filter):
    """只拦截 Telethon 打的 'Got difference for channel <id> updates' 这条 INFO 日志"""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # 仅 Telethon 命名空间，且等级为 INFO
            if record.name.startswith("telethon") and record.levelno == logging.INFO:
                msg = record.getMessage()
                if _TELETHON_DIFF_RE.match(msg):
                    return False  # 拦截该条日志
        except Exception:
            pass
        return True


def init_logger(
    to_console: bool = True,
    level: Optional[Union[int, str]] = None,
    tg_client=None,
    tg_target=None,
    tg_level: Optional[Union[int, str]] = None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    queue_maxsize: int = 2000,
    rate_window_sec: int = 10,
) -> None:
    """
    幂等初始化：
    - 相同 fingerprint（参数组合）则直接返回，避免重复初始化
    - 如启用 Telegram：使用“丢最旧队列” + 10s 同类错误去重
    - 避免递归：worker 内部报错不会再次投递到 TG
    """
    global _telegram_queue, _telegram_worker_task, _telegram_stop_event
    global _global_logger, _logger_fingerprint, _logger_initialized

    file_level = _resolve_level(DEFAULT_FILE_LOG_LEVEL if level is None else level, DEFAULT_FILE_LOG_LEVEL)
    console_level = _resolve_level(DEFAULT_CONSOLE_LOG_LEVEL if level is None else level, DEFAULT_CONSOLE_LOG_LEVEL)
    tg_level_res = _resolve_level(DEFAULT_TG_LOG_LEVEL if tg_level is None else tg_level, DEFAULT_TG_LOG_LEVEL)
    root_level = min(file_level, console_level, tg_level_res)

    fp = f"{to_console}|{file_level}|{console_level}|{bool(tg_client)}|{tg_target}|{tg_level_res}|{queue_maxsize}|{rate_window_sec}"
    if _logger_initialized and _logger_fingerprint == fp:
        return

    # 清理旧 handler
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    os.makedirs(LOG_DIR, exist_ok=True)
    handlers: list[logging.Handler] = []

    # file
    try:
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    except Exception:
        file_handler = logging.FileHandler(os.path.join(".", LOG_FILE), encoding="utf-8")
    file_handler.setLevel(file_level)
    handlers.append(file_handler)

    # console
    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        handlers.append(console_handler)

    # telegram
    if tg_client and tg_target:
        if not _telegram_queue:
            _telegram_queue = DropOldestAsyncQueue(maxlen=max(100, int(queue_maxsize)))
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()

        tg_handler = TelegramLogHandler(
            _telegram_queue,
            loop=loop,
            level=tg_level_res,
            rate_window_sec=rate_window_sec,
        )
        tg_handler.setLevel(tg_level_res)
        handlers.append(tg_handler)

        if not _telegram_stop_event:
            _telegram_stop_event = asyncio.Event()

        # 若之前已有 worker，先请求停止
        if _telegram_worker_task and not _telegram_worker_task.done():
            try:
                _telegram_stop_event.set()
            except Exception:
                pass

        # 启动新的 worker
        _telegram_stop_event.clear()
        _telegram_worker_task = loop.create_task(
            telegram_log_worker(tg_client, tg_target, _telegram_queue, _telegram_stop_event)
        )

    logging.basicConfig(
        format=LOG_FORMAT, datefmt=DATE_FORMAT, level=root_level, handlers=handlers
    )

    # —— 安装 Telethon 噪声过滤器（仅屏蔽特定 INFO 文案）——
    try:
        _telethon_noise_filter = _TelethonChannelDiffFilter()
        # 装到 root 与 telethon logger
        logging.getLogger().addFilter(_telethon_noise_filter)
        logging.getLogger("telethon").addFilter(_telethon_noise_filter)
        # ✅ 加固：给所有 handlers 也装一层，防 propagate=False 的情况
        for _h in handlers:
            _h.addFilter(_telethon_noise_filter)
    except Exception:
        pass

    _global_logger = logging.getLogger("unified")
    _logger_fingerprint = fp
    _logger_initialized = True


async def shutdown_logger(timeout: float = 5.0):
    """
    优雅关闭：
    - 通知 TG worker 停止；等待其排空或超时后取消。
    """
    global _telegram_worker_task, _telegram_stop_event
    try:
        if _telegram_stop_event:
            _telegram_stop_event.set()
        if _telegram_worker_task and not _telegram_worker_task.done():
            try:
                await asyncio.wait_for(asyncio.shield(_telegram_worker_task), timeout=timeout)
            except (asyncio.TimeoutError, Exception):
                _telegram_worker_task.cancel()
                try:
                    await _telegram_worker_task
                except Exception:
                    pass
        _telegram_worker_task = None
    except Exception:
        pass


# ========================= 中文映射（值级替换） =========================
_ZH_VALUE_MAP: Dict[str, Dict[str, str]] = {
    "status": {
        "ok": "全部成功",
        "partial": "部分成功",
        "failed": "失败",
        "timeout": "超时",
        "running": "运行中",
        "stopped": "已停止",
        "success": "成功",
        "failure": "失败",
        "error": "错误",
    },
    "code": {
        "success": "加入成功",
        "already_in": "已在群里",
        "pending": "等待管理员审批",
        "restricted": "受限/无权限",
        "invalid": "链接无效/过期",
        "timeout": "加群超时",
        "failure": "入群失败",
        "error": "错误",
    },
    "phase": {
        "assign": "分配",
        "start": "开始",
        "status": "状态",
        "result": "结果",
        "summary": "汇总",
        "prepare_assignment": "准备分配",
        "join": "入群",
        "send": "群发",
        "uikit": "通知渲染",
        "handler": "处理器",
    },
    "error_kind": {
        "write_forbidden": "禁言/无写入权限",
        "private": "私有/不可见",
        "username_invalid": "用户名非法",
        "username_not_occupied": "用户名未注册",
        "banned_in_channel": "被频道禁用",
        "send_failed": "发送失败",
        "flood": "限流",
    },
}

_ZH_FIELD_KEY_MAP: Dict[str, str] = {
    "user_id": "用户",
    "trace_id": "追踪",
    "task_id": "任务",
    "to_user_id": "接收用户",
    "me_phone": "手机号",
    "phone": "手机号",
    "me_id": "账号ID",
    "me_username": "用户名",
    "bot_name": "机器人",
    "function": "函数",
    "func": "函数",
    "function_raw": "原函数",
    "module": "模块",
    "mod_name": "模块",
    "signal": "信号",
    "phase": "阶段",
    "level": "级别",
    "status": "状态",
    "elapsed_ms": "耗时ms",
    "items": "项数",
    "group": "群组",
    "message_id": "消息ID",
    "round": "轮次",
    "session": "会话",
    "session_path": "会话",
    "json_path": "JSON路径",
    "index": "序号",
    "total": "总数",
    "clients": "账号数",
    "phones": "账号数",
    "groups_total": "群组总数",
    "results_count": "结果数",
    "success": "成功",
    "failed": "失败",
    "pending": "待定",
    "success_count": "成功",
    "failed_count": "失败",
    "pending_count": "待定",
    "succ": "成功",
    "fail": "失败",
    "code": "错误码",
    "error_code": "错误码",
    "error_kind": "错误类",
    "error_meta": "错误元",
    "error": "错误",
    "categories": "分类",
    "link_kind": "链接类型",
    "msg_type": "消息类型",
    "ok": "OK",
}
_NO_ZH_VALUE_KEYS = {"phase", "status", "level"}

_EN_VALUE_MAP: Dict[str, Dict[str, str]] = {
    "phase": {
        "汇总": "summary",
        "状态": "status",
        "结果": "result",
        "开始": "start",
        "分配": "assign",
        "准备分配": "prepare_assignment",
        "入群": "join",
        "群发": "send",
        "通知": "uikit",
        "处理器": "handler",
    },
    "status": {
        "已停止": "stopped",
        "运行中": "running",
        "全部成功": "ok",
        "部分成功": "partial",
        "失败": "failed",
        "成功": "success",
        "错误": "error",
        "超时": "timeout",
    },
    "level": {
        "正常": "info",
        "信息": "info",
        "警告": "warn",
        "错误": "error",
        "进行中": "progress",
    },
}


def _en_value_for(key: str, val: Any) -> Optional[str]:
    try:
        m = _EN_VALUE_MAP.get(str(key))
        if not m:
            return None
        s = str(val)
        return m.get(s) or m.get(s.lower())
    except Exception:
        return None


def _zh_value_for(key: str, val: Any) -> Optional[str]:
    if not LOG_ZH:
        return None
    try:
        m = _ZH_VALUE_MAP.get(str(key))
        if not m:
            return None
        sval = str(val)
        return m.get(sval.lower()) or m.get(sval)
    except Exception:
        return None


def _apply_zh_ctx(ctx: Dict[str, Any], *, except_keys: Optional[set[str]] = None) -> Dict[str, Any]:
    if not LOG_ZH or not ctx:
        return ctx or {}
    out: Dict[str, Any] = {}
    skip = set(except_keys or set())
    for k, v in ctx.items():
        if k in skip:
            ev = _en_value_for(k, v)
            out[k] = ev if ev is not None else v
            continue
        zh = _zh_value_for(k, v)
        out[k] = zh if zh is not None else v
    return out


# ========================= 格式化与输出 API =========================
CANONICAL_KEY_ALIASES: Dict[str, str] = {
    "uid": "user_id",
    "user": "user_id",
    "userid": "user_id",
    "trace": "trace_id",
    "fn": "function",
    "func": "function",
    "mod": "module",
    "mid": "message_id",
    "msg_id": "message_id",
    "kind": "error_kind",
    "round_no": "round",
}
_INT_KEYS: Iterable[str] = ("user_id", "me_id", "message_id", "round", "elapsed_ms")
_BOOL_KEYS: Iterable[str] = ("ok",)


def _normalize_extra(data: Dict[str, Any]) -> Dict[str, Any]:
    if not data:
        return {}
    out: Dict[str, Any] = {}
    for k, v in list(data.items()):
        k_norm = CANONICAL_KEY_ALIASES.get(k, k)
        if k_norm in out and v is not None and out[k_norm] != v:
            out[f"{k}_raw"] = v
            continue
        out[k_norm] = v
    if "func_raw" in out and "function" in out and out["function"] != out["func_raw"]:
        out["wrapped_function"] = out.get("function")
        out["function"] = out.get("func_raw")
    if isinstance(out.get("function"), str) and out["function"] in ("async_wrapper", "sync_wrapper"):
        if "func" in data:
            out["wrapped_function"] = out["function"]
            out["function"] = data["func"]
    for key in _INT_KEYS:
        if key in out and out[key] is not None:
            try:
                out[key] = int(out[key])
            except Exception:
                pass
    for key in _BOOL_KEYS:
        if key in out and out[key] is not None:
            try:
                out[key] = bool(out[key])
            except Exception:
                pass
    if "error_kind" in out and out.get("error_kind"):
        out["error_kind"] = str(out["error_kind"]).lower()
    if "error" in out and out.get("error"):
        out["error"] = str(out["error"])[:500]
    return out


def _merge_context(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ctx = dict(get_log_context() or {})
    if extra:
        ctx.update(extra or {})
    return _normalize_extra(ctx)


def _reserved_logrecord_fields() -> Set[str]:
    global _RESERVED_FIELDS_CACHE
    if _RESERVED_FIELDS_CACHE is not None:
        return _RESERVED_FIELDS_CACHE
    try:
        _RESERVED_FIELDS_CACHE = set(logging.makeLogRecord({}).__dict__.keys()) | {"message", "asctime"}
    except Exception:
        _RESERVED_FIELDS_CACHE = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
        }
    return _RESERVED_FIELDS_CACHE


def _ctx_extra(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        merged = dict(_merge_context(extra))
        reserved = _reserved_logrecord_fields()
        sanitized = {k: v for k, v in merged.items() if k not in reserved}
        return sanitized
    except Exception:
        raw = dict(extra or {})
        reserved = _reserved_logrecord_fields()
        return {k: v for k, v in raw.items() if k not in reserved}


def _format_json_payload(message: Union[str, Dict[str, Any]], ctx: Dict[str, Any]) -> str:
    ctx_to_write = _apply_zh_ctx(ctx, except_keys=_NO_ZH_VALUE_KEYS)
    payload: Dict[str, Any] = {"message": None, "ctx": ctx_to_write or {}}
    if isinstance(message, dict):
        payload["message"] = ""
        payload["data"] = message
    else:
        payload["message"] = str(message)
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return f'{payload.get("message", "")} | ' + " ".join(
            f"{k}={v}" for k, v in (ctx_to_write or {}).items() if v is not None
        )


def _format_msg(msg: Union[str, Any], extra: Optional[Dict[str, Any]] = None) -> str:
    ctx_raw = _merge_context(extra)

    if os.environ.get("LOG_JSON") == "1":
        as_dict: Optional[Dict[str, Any]] = None
        try:
            if hasattr(msg, "model_dump"):
                as_dict = dict(msg.model_dump())
            elif hasattr(msg, "dict"):
                as_dict = dict(msg.dict())
            elif isinstance(msg, dict):
                as_dict = dict(msg)
        except Exception:
            as_dict = None
        if as_dict is not None:
            return _format_json_payload(as_dict, ctx_raw)
        return _format_json_payload(str(msg), ctx_raw)

    try:
        if hasattr(msg, "model_dump"):
            msg = json.dumps(msg.model_dump(), ensure_ascii=False)
        elif hasattr(msg, "dict"):
            msg = json.dumps(msg.dict(), ensure_ascii=False)
        elif isinstance(msg, dict):
            msg = json.dumps(msg, ensure_ascii=False)
    except Exception:
        msg = str(msg)

    ctx_to_write = _apply_zh_ctx(ctx_raw, except_keys=_NO_ZH_VALUE_KEYS)
    parts = []
    for k, v in ctx_to_write.items():
        if v is None:
            continue
        k_disp = _ZH_FIELD_KEY_MAP.get(k, k) if LOG_ZH else k
        parts.append(f"{k_disp}={v}")
    ctx_str = " ".join(parts)
    return f"{msg} | {ctx_str}" if ctx_str else str(msg)


def log_debug(msg: Union[str, Any], extra: Optional[Dict[str, Any]] = None):
    get_global_logger().debug(_format_msg(msg, extra), extra=_ctx_extra(extra))


def log_info(msg: Union[str, Any], extra: Optional[Dict[str, Any]] = None):
    get_global_logger().info(_format_msg(msg, extra), extra=_ctx_extra(extra))


def log_warning(msg: Union[str, Any], extra: Optional[Dict[str, Any]] = None):
    get_global_logger().warning(_format_msg(msg, extra), extra=_ctx_extra(extra))


def log_error(msg: Union[str, Any], extra: Optional[Dict[str, Any]] = None):
    get_global_logger().error(_format_msg(msg, extra), extra=_ctx_extra(extra))


def log_critical(msg: Union[str, Any], extra: Optional[Dict[str, Any]] = None):
    get_global_logger().critical(_format_msg(msg, extra), extra=_ctx_extra(extra))


def log_exception(
    msg: str,
    exc: Optional[BaseException] = None,
    extra: Optional[Dict[str, Any]] = None,
):
    ext = dict(extra or {})
    if exc is not None:
        try:
            ext.setdefault("error_type", type(exc).__name__)
            ext.setdefault("error", str(exc))
        except Exception:
            pass
    base = _format_msg(msg, ext)
    if exc is not None:
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        # 标注 _NO_TG_FIELD 避免异常日志递归投递至 TG
        get_global_logger().error(f"{base}\n{trace}", extra={**_ctx_extra(ext), _NO_TG_FIELD: True})
    else:
        get_global_logger().error(base, extra={**_ctx_extra(ext), _NO_TG_FIELD: True})
