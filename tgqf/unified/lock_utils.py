# -*- coding: utf-8 -*-
# unified/lock_utils.py
from __future__ import annotations

import asyncio
import errno
import os
import time
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from unified.config import LOCK_DIR

__all__ = [
    "with_session_lock",
    "default_session_lock_key",
    "default_session_lock_path",
    "session_lock",
]

# —— 进程内互斥（按 key） —— #
_INPROC: Dict[str, asyncio.Lock] = {}

def _inproc_lock(key: str) -> asyncio.Lock:
    lk = _INPROC.get(key)
    if lk is None:
        lk = asyncio.Lock()
        _INPROC[key] = lk
    return lk


# —— 跨进程文件锁（POSIX/Windows 兼容） —— #
class _FileLock:
    """
    简单跨进程文件锁：
    - POSIX: fcntl.flock(EX)
    - Windows: msvcrt.locking
    """
    def __init__(self, key: str) -> None:
        os.makedirs(LOCK_DIR, exist_ok=True)
        self.path = os.path.join(LOCK_DIR, f"{_sanitize_key(key)}.lock")
        self._fh = None

    def _acquire_blocking(self) -> None:
        self._fh = open(self.path, "a+b")
        try:
            import fcntl
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        except Exception:
            try:
                import msvcrt  # type: ignore
                self._fh.seek(0)                    # <== 必加
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
            except Exception:
                raise

    def _release_blocking(self) -> None:
        if not self._fh:
            return
        try:
            import fcntl
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            try:
                import msvcrt  # type: ignore
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                pass
        try:
            self._fh.close()
        finally:
            self._fh = None

    async def acquire(self, timeout: Optional[float]) -> bool:
        start = time.monotonic()
        while True:
            try:
                await asyncio.to_thread(self._acquire_blocking)
                return True
            except OSError as e:
                # 仅对常见的资源占用重试；其他错误直接抛出
                if e.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
            except Exception:
                # 兼容某些平台/驱动抛通用异常时的重试
                pass
            if timeout is not None and (time.monotonic() - start) >= timeout:
                return False
            await asyncio.sleep(0.1)

    async def release(self) -> None:
        await asyncio.to_thread(self._release_blocking)


# —— Reentrant（同任务可嵌套）支持 —— #
# key -> owner_task_id
_HOLDERS: Dict[str, int] = {}
# (key, owner_task_id) -> recursion_count
_RECUR: Dict[Tuple[str, int], int] = {}

def _task_id() -> int:
    t = asyncio.current_task()
    return 0 if t is None else id(t)

def _sanitize_key(key: str) -> str:
    # 防止路径穿越/特殊字符；限制长度
    safe = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in str(key))[:200]
    return safe or "global"


class _SessionLockCtx:
    """
    组合锁上下文（进程内 + 跨进程），支持同一 asyncio 任务对同一 key 的“可重入”获取：
      - 第一次获取：持有 inproc 锁 + 文件锁
      - 嵌套获取：只增加计数，不重复获取
    """
    def __init__(self, key: str, timeout: Optional[float] = None):
        self.key = key
        self.timeout = timeout
        self._owner = _task_id()
        self._root = False
        self._inproc_acquired = False
        self._flock: Optional[_FileLock] = None

    async def __aenter__(self):
        # 已被当前任务持有：可重入路径（不再竞争 inproc/file lock）
        if _HOLDERS.get(self.key) == self._owner:
            _RECUR[(self.key, self._owner)] = _RECUR.get((self.key, self._owner), 0) + 1
            return self

        # 首次获取：串行化 + 文件锁
        lk = _inproc_lock(self.key)
        await lk.acquire()
        self._inproc_acquired = True

        try:
            # 双重检查（避免临界条件：锁到手后若是本任务重入，直接走计数路径）
            if _HOLDERS.get(self.key) == self._owner:
                _RECUR[(self.key, self._owner)] = _RECUR.get((self.key, self._owner), 0) + 1
                return self

            self._flock = _FileLock(self.key)
            ok = await self._flock.acquire(self.timeout)
            if not ok:
                raise TimeoutError(f"acquire file lock timeout: {self.key}")

            _HOLDERS[self.key] = self._owner
            _RECUR[(self.key, self._owner)] = 1
            self._root = True
            return self
        except Exception:
            # 失败时释放 inproc，避免死锁
            if self._inproc_acquired:
                lk.release()
                self._inproc_acquired = False
            raise

    async def __aexit__(self, exc_type, exc, tb):
        owner = _HOLDERS.get(self.key)
        if owner == self._owner:
            cnt_key = (self.key, self._owner)
            remain = max(0, _RECUR.get(cnt_key, 0) - 1)
            if remain == 0:
                _RECUR.pop(cnt_key, None)
                _HOLDERS.pop(self.key, None)
                # root 才需要释放文件锁
                if self._flock:
                    try:
                        await self._flock.release()
                    finally:
                        self._flock = None
            else:
                _RECUR[cnt_key] = remain

        # 仅首层获取持有 inproc，需要在最后释放
        if self._inproc_acquired:
            try:
                _inproc_lock(self.key).release()
            finally:
                self._inproc_acquired = False


# —— 统一锁 key 生成 —— #
def default_session_lock_key(*args, **kwargs) -> str:
    """
    支持两类：
      1) 命令/回调：传入 event（含 sender_id）→ user:{uid}
      2) 账号注册：方法签名 (self, user_id:int, phone:str, ...) → user:{uid}:phone:{phone}
      3) kwargs 显式 user_id/phone 优先
    附加增强：
      - 兼容 kwargs['event'] 或任意位置参数上带 sender_id 属性的对象
      - 更健壮的字符串化与清洗
    """
    uid = kwargs.get("user_id")
    phone = kwargs.get("phone")

    if uid is not None and phone is not None:
        return f"user:{int(uid)}:phone:{str(phone)}"

    # 尝试匹配位置参数签名: (self, user_id, phone, ...)
    if len(args) >= 3 and isinstance(args[1], int) and isinstance(args[2], str):
        return f"user:{args[1]}:phone:{args[2]}"

    # event in kwargs
    ev = kwargs.get("event")
    if ev is not None and hasattr(ev, "sender_id"):
        return f"user:{getattr(ev, 'sender_id', 0)}"

    # 任意参数含 sender_id
    for a in args:
        if hasattr(a, "sender_id"):
            return f"user:{getattr(a, 'sender_id', 0)}"

    return "global"


# —— 兼容别名（历史代码引用） —— #
default_session_lock_path = default_session_lock_key  # 向后兼容


# —— 装饰器：组合锁 —— #
def with_session_lock(
    key_builder: Callable[..., str] = default_session_lock_key,
    timeout: Optional[float] = None,
):
    """
    组合锁（进程内 + 跨进程），支持可重入：
      - 同一 asyncio 任务对同一 key 的嵌套调用不会阻塞自己
      - 其他任务/进程会被串行化
    """

    def deco(func: Callable[..., Awaitable[Any]]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            raw_key = key_builder(*args, **kwargs)
            key = _sanitize_key(raw_key)

            async with _SessionLockCtx(key, timeout=timeout):
                return await func(*args, **kwargs)

        return wrapper

    return deco


# —— 显式上下文管理器（不使用装饰器也能手动包裹临界区） —— #
def session_lock(key: str, *, timeout: Optional[float] = None):
    """
    用法：
        async with session_lock(f"user:{uid}:phone:{phone}", timeout=5):
            ...
    """
    return _SessionLockCtx(_sanitize_key(key), timeout=timeout)
