# core/bot_client.py 
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
单例 Bot 客户端创建器（Telethon）
- 统一会话路径解析：优先 unified.config.get_bot_session_path()，回退 BOT_SESSION 或默认 kcdbot.session
- 单实例文件锁：防止同一台机器同时运行多个进程导致“命令重复响应”
- SQLite 锁重试：处理 "database is locked"，并清理 -journal/-wal/-shm 残留
- 只读/权限问题：显式 chmod 目录/文件，确保可写
- 协程级 session 互斥：避免同一路径并发创建/启动
- 解析模式：设置为 AutoParseMode()（官方解析优先，自动在 HTML/Markdown 间选择）
"""

import asyncio
import atexit
import os
import sqlite3
from typing import Any, Dict, Optional
import inspect
from telethon import TelegramClient
from tg.telethon_aligned import AutoParseMode  # ✅ 与 tg/telethon_aligned 对齐

from unified.logger import log_error, log_info, log_debug, log_warning, log_exception
from unified.trace_context import set_log_context
from unified.context import set_bot

try:
    from unified.config import API_HASH, API_ID, BOT_TOKEN, LOCK_DIR, SESSION_DIR
    from unified.config import get_bot_session_path as _get_bot_session_path
except Exception as e:
    raise RuntimeError(f"[bot_client] 配置导入失败：{e}")

try:
    from unified.config import BOT_SESSION as _LEGACY_BOT_SESSION
except Exception:
    _LEGACY_BOT_SESSION = None

# ==== 平台文件锁（Linux/macOS） ====
try:
    import fcntl  # type: ignore
    _HAS_FCNTL = True
except Exception:
    _HAS_FCNTL = False

# 全局单例与锁
_BOT: Optional[TelegramClient] = None
_BOT_LOCK = asyncio.Lock()

# 与会话参数一致的默认项（适度稳健，避免侵入）
_DEFAULTS: Dict[str, Any] = dict(
    use_ipv6=False,
    timeout=15,                 # 连接超时
    request_retries=5,          # 请求级自动重试（短 Flood / ServerError）
    connection_retries=5,       # 连接重试
    retry_delay=3,              # 重连间隔
    auto_reconnect=True,        # 掉线自动重连
    sequential_updates=False,   # Bot 事件并行吞吐更高；如需顺序可自行覆盖
    flood_sleep_threshold=45,   # 短 Flood 自动 sleep
    raise_last_call_error=True, # 抛出最后一次 RPC 错误

)
def _filter_client_kwargs(d: Dict[str, Any]) -> Dict[str, Any]:
    """按 TelegramClient.__init__ 签名过滤，只传官方支持的参数。"""
    try:
        sig = inspect.signature(TelegramClient.__init__)
        allow = set(sig.parameters.keys())
        return {k: v for k, v in (d or {}).items() if k in allow}
    except Exception:
        return d
# 模块级日志上下文
set_log_context({"module": __name__})

# atexit 退出保护
_shutdown_called = False
_run_lock_acquired = False

# 协程级 session 锁表（按 session 路径）
_session_locks: Dict[str, asyncio.Lock] = {}


# ---------- 工具：会话路径解析 ----------
def _resolve_session_path(explicit: Optional[str] = None) -> str:
    """
    优先使用统一 API；否则回退到显式入参；再回退旧变量；最后默认 kcdbot.session
    """
    if callable(_get_bot_session_path):
        try:
            p = _get_bot_session_path()
            if p:
                return p
        except Exception:
            pass

    if explicit:
        return explicit

    if _LEGACY_BOT_SESSION:
        return _LEGACY_BOT_SESSION

    os.makedirs(SESSION_DIR, exist_ok=True)
    return os.path.join(SESSION_DIR, "kcdbot.session")


# ---------- 工具：单实例运行锁 ----------
def _acquire_run_lock() -> None:
    """
    简单文件锁，确保同路径只运行一个 Bot 实例，避免重复响应。
    """
    global _run_lock_acquired
    if _run_lock_acquired:
        return
    lock_path = os.path.join(LOCK_DIR or SESSION_DIR, "bot.lock")
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    fd = open(lock_path, "w")
    if _HAS_FCNTL:
        try:
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fd.write(str(os.getpid()))
            fd.flush()
            _run_lock_acquired = True
            # 进程退出自动释放文件描述符
            atexit.register(lambda: fd.close())
            log_debug("✅ 运行锁获取成功", extra={"lock": lock_path})
        except BlockingIOError:
            raise RuntimeError(f"另一个 Bot 实例已在运行（锁文件：{lock_path}）")
    else:
        log_info("不支持 fcntl 文件锁，将不启用单实例保护")


def _sync_disconnect():
    """进程退出前尽力断开（在 atexit 中调用，无 await）"""
    global _shutdown_called
    if _shutdown_called:
        return
    _shutdown_called = True
    try:
        bot = _BOT
        if bot and bot.is_connected():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(bot.disconnect())
                else:
                    loop.run_until_complete(bot.disconnect())
            except Exception:
                try:
                    asyncio.run(bot.disconnect())
                except Exception:
                    pass
    except Exception:
        pass


# 在导入时就注册兜底断连
atexit.register(_sync_disconnect)


# ========== 配置解析 ==========
def _get_env(*names: str, default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


def _resolve_bot_config() -> Dict[str, Any]:
    """
    优先 env，其次尝试 unified.config（若存在）；确保最小可用集：
    - bot_token（NOTIFY_BOT_TOKEN / BOT_TOKEN）
    - api_id / api_hash（TG_API_ID / API_ID, TG_API_HASH / API_HASH）
    - session 路径（BOT_SESSION / data/bot.session）
    """
    cfg: Dict[str, Any] = {}

    # 1) token
    token = _get_env("NOTIFY_BOT_TOKEN", "BOT_TOKEN")
    # 2) api_id / api_hash
    api_id = _get_env("TG_API_ID", "API_ID")
    api_hash = _get_env("TG_API_HASH", "API_HASH")
    # 3) session
    session = _get_env("BOT_SESSION", default="data/bot.session")

    # 尝试 unified.config（可选）
    if not (token and api_id and api_hash):
        try:
            from unified import config as U  # type: ignore
            token = token or getattr(U, "NOTIFY_BOT_TOKEN", None) or getattr(U, "BOT_TOKEN", None)
            api_id = api_id or str(getattr(U, "TG_API_ID", "") or getattr(U, "API_ID", ""))
            api_hash = api_hash or getattr(U, "TG_API_HASH", None) or getattr(U, "API_HASH", None)
            session = session or getattr(U, "BOT_SESSION", "data/bot.session")
        except Exception:
            pass

    # 最终校验
    if not token:
        raise RuntimeError("❌ 缺少 Bot Token（NOTIFY_BOT_TOKEN / BOT_TOKEN）")
    if not api_id or not api_hash:
        raise RuntimeError("❌ 缺少 API_ID/API_HASH（TG_API_ID/API_ID 与 TG_API_HASH/API_HASH）")

    cfg["token"] = str(token).strip()
    cfg["api_id"] = int(api_id)
    cfg["api_hash"] = str(api_hash).strip()
    cfg["session"] = session
    return cfg


# ---------- 工具：清理 SQLite 残留 ----------
def _cleanup_sqlite_sidecars(session_path: str) -> None:
    """Telethon 会话是 sqlite DB；锁死时常遗留这些 sidecar 文件。"""
    for suf in ("-journal", "-wal", "-shm"):
        p = f"{session_path}{suf}"
        if os.path.exists(p):
            try:
                os.remove(p)
                log_info("🧹 已移除遗留会话文件", extra={"file": p})
            except Exception as e:
                log_error(f"❌ 无法移除遗留文件 {p}：{e}")


def get_session_lock(session_path: str) -> asyncio.Lock:
    lk = _session_locks.get(session_path)
    if lk is None:
        lk = _session_locks[session_path] = asyncio.Lock()
    return lk


def ensure_rw_sqlite(session_path: str) -> None:
    """
    - 确保目录/文件可写（chmod）
    - 若文件不存在则创建空文件
    - 修复被意外标记为只读的情况，避免 OperationalError: attempt to write a readonly database
    """
    try:
        sdir = os.path.dirname(session_path) or "."
        os.makedirs(sdir, exist_ok=True)
        # 目录权限
        try:
            os.chmod(sdir, 0o700)
        except Exception:
            pass

        # 主文件（可能不存在）
        if not os.path.exists(session_path):
            try:
                fd = os.open(session_path, os.O_RDWR | os.O_CREAT, 0o600)
                os.close(fd)
            except Exception:
                pass

        # 主文件及 sidecar 权限修复
        for p in (
            session_path,
            f"{session_path}-wal",
            f"{session_path}-shm",
            f"{session_path}-journal",
        ):
            if os.path.exists(p):
                try:
                    os.chmod(p, 0o600)
                except Exception:
                    pass
    except Exception:
        pass


async def _start_bot(session: str, api_id: int, api_hash: str, token: str) -> TelegramClient:
    """
    内部：实例化 + 启动 Bot；设置 parse_mode；写入上下文。
    """
    client = TelegramClient(
        session=str(session),
        api_id=int(api_id),
        api_hash=str(api_hash),
        **_filter_client_kwargs(_DEFAULTS),
    )

    # 统一解析模式：AutoParseMode（官方解析器 + 自动判别 HTML/Markdown）
    try:
        client.parse_mode = AutoParseMode()
    except Exception as e:
        log_warning(f"设置 parse_mode 失败，回退 html：{e}")
        try:
            client.parse_mode = "html"
        except Exception:
            pass

    # 启动
    await client.start(bot_token=token)

    # 标注元信息（便于日志/上下文）
    try:
        me = await client.get_me()
        bot_name = getattr(me, "username", None) or getattr(me, "first_name", "") or "bot"
        setattr(client, "bot_name", bot_name)
        log_info("✅ Bot 启动完成", extra={"bot_name": bot_name, "bot_id": getattr(me, "id", None)})
    except Exception as e:
        log_warning(f"get_me 失败（不影响发送功能）：{e}")

    return client


# ---------- 对外：创建/获取 Bot ----------
async def create_bot_client(
    *,
    token: Optional[str] = None,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
    session: Optional[str] = None,
    force_restart: bool = False,
) -> TelegramClient:
    """
    幂等创建/启动通知 Bot（单例）：
    - 默认从环境/unified.config 解析配置；也可显式传参覆盖
    - force_restart=True 时，关闭旧实例后重建
    - 集成单实例锁、SQLite 只读修复与 sidecar 清理、数据库锁重试
    """
    global _BOT
    async with _BOT_LOCK:
        # 复用已存在实例
        if _BOT and not force_restart:
            try:
                return _BOT
            except Exception:
                pass

        # 若需要重启：先断开旧实例
        if _BOT and force_restart:
            try:
                await _BOT.disconnect()
            except Exception:
                pass
            _BOT = None

        # 解析配置
        cfg = _resolve_bot_config()
        if token is not None:
            cfg["token"] = token
        if api_id is not None:
            cfg["api_id"] = int(api_id)
        if api_hash is not None:
            cfg["api_hash"] = str(api_hash)
        # 统一会话路径解析
        cfg["session"] = _resolve_session_path(session or cfg.get("session"))

        # 单实例运行锁
        _acquire_run_lock()

        # SQLite 环境准备
        ensure_rw_sqlite(cfg["session"])
        _cleanup_sqlite_sidecars(cfg["session"])

        # 启动（带 SQLite “database is locked” 重试）
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                bot = await _start_bot(
                    session=cfg["session"],
                    api_id=cfg["api_id"],
                    api_hash=cfg["api_hash"],
                    token=cfg["token"],
                )
                _BOT = bot
                try:
                    set_bot(bot)  # 提供给 unified.context 的全局获取
                except Exception:
                    pass
                return bot
            except sqlite3.OperationalError as e:
                last_err = e
                if "database is locked" in str(e).lower():
                    log_warning("会话数据库被锁，尝试清理并重试", extra={"attempt": attempt + 1})
                    _cleanup_sqlite_sidecars(cfg["session"])
                    await asyncio.sleep(0.25 * (attempt + 1))
                    continue
                log_exception("启动 Bot 遇到 sqlite OperationalError", exc=e)
                break
            except Exception as e:
                last_err = e
                log_exception("❌ Bot 启动失败（非 sqlite 错误）", exc=e)
                break

        # 到这里仍未成功
        if last_err:
            raise last_err
        raise RuntimeError("启动 Bot 失败（未知原因）")


async def shutdown_bot_client() -> None:
    """优雅关闭通知 Bot。"""
    global _BOT
    async with _BOT_LOCK:
        if _BOT:
            try:
                await _BOT.disconnect()
                log_info("🛑 Bot 已断开")
            except Exception as e:
                log_warning(f"断开 Bot 失败：{e}")
            _BOT = None


# ========== 对外 API ==========
def get_notify_bot() -> TelegramClient:
    """
    获取已创建的通知 Bot 客户端。
    - 若未创建，抛错（由上层按需调用 create_bot_client 懒创建）
    """
    if _BOT is None:
        raise RuntimeError("notify bot 尚未初始化；请先调用 create_bot_client()")
    return _BOT


__all__ = [
    "create_bot_client",
    "get_notify_bot",
    "ensure_rw_sqlite",
    "get_session_lock",
]
