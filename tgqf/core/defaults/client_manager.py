# -*- coding: utf-8 -*-
# core/defaults/client_manager.py
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple ,Iterable
from typing import Iterable
from telethon import TelegramClient, errors as te_errors
from tg.telethon_aligned import AutoParseMode  # ✅ 统一使用 Auto 模式
import inspect
from typess.health_types import HealthState
from core.device_factory import get_api_config, patch_json_config  # noqa: F401
from unified.lock_utils import default_session_lock_path, with_session_lock
from unified.logger import log_debug, log_exception, log_info, log_warning
from unified.trace_context import generate_trace_id, get_log_context, set_log_context
from core.runtime_events import attach_runtime_event_listeners
from unified.config import (
    SESSION_DIR, get_phone_json, get_phone_session, get_user_dir,
    TG_USE_IPV6, TG_TIMEOUT, TG_REQUEST_RETRIES, TG_CONNECTION_RETRIES, TG_RETRY_DELAY,
    TG_AUTO_RECONNECT, TG_SEQUENTIAL_UPDATES, TG_ENTITY_CACHE_LIMIT,
    CLIENT_CONNECT_TIMEOUT, ME_CACHE_TTL, SESSION_LOCK_TIMEOUT,
    LOAD_ALL_CONCURRENCY, USER_COMMAND_COOLDOWN_WINDOW, FLOOD_AUTOSLEEP_THRESHOLD,TELETHON_RAISE_LAST_CALL_ERROR
)
# —— 集成 telethon_errors 能力 —— #
from core.telethon_errors import (
    call as _tg_call,                  # 统一 RPC 入口
    TE,                                # 错误类命名空间
    detect_flood_seconds,              # 提取 flood 秒数
    classify_telethon_error,           # 异常 → HealthState
)
try:
    # Telethon ≥ 1.24 一般能从这里拿到
    from telethon.errors.common import MultiError as TE_MultiError  # type: ignore
except Exception:
    TE_MultiError = None  # 优雅降级

REQUIRED_API_FIELDS = [
    "api_id",
    "api_hash",
    "device_model",
    "system_version",
    "app_version",
    "lang_code",
    "system_lang_code",
]
_BAD_TTL_SECONDS = 3600  # 坏会话隔离 TTL（秒），可按需调整

def _now() -> float:
    import time as _t
    return _t.time()


BAD_STATES = {HealthState.AUTH_EXPIRED, HealthState.BANNED}

# =========================
# 统一 TelegramClient 工厂
# =========================
_DEFAULTS = dict(
    use_ipv6=TG_USE_IPV6,
    timeout=TG_TIMEOUT,
    request_retries=TG_REQUEST_RETRIES,
    connection_retries=TG_CONNECTION_RETRIES,
    retry_delay=TG_RETRY_DELAY,
    auto_reconnect=TG_AUTO_RECONNECT,
    sequential_updates=TG_SEQUENTIAL_UPDATES,
    flood_sleep_threshold=FLOOD_AUTOSLEEP_THRESHOLD,
    raise_last_call_error=TELETHON_RAISE_LAST_CALL_ERROR,
    entity_cache_limit=TG_ENTITY_CACHE_LIMIT,
)


def _filter_client_kwargs(d: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sig = inspect.signature(TelegramClient.__init__)
        ok = set(sig.parameters)
        return {k: v for k, v in (d or {}).items() if k in ok}
    except Exception:
        return d

def create_client(
    session_path: str | Path,
    api_id: int,
    api_hash: str,
    *,
    device_model: str | None = None,
    system_version: str | None = None,
    app_version: str | None = None,
    lang_code: str = "en",
    system_lang_code: str = "en",
    proxy: dict | tuple | None = None,
    local_addr: str | tuple | None = None,
    receive_updates: bool = True,
) -> TelegramClient:
    """
    统一入口：集中 Telethon 连接/重试/短 Flood 睡眠参数。
    - receive_updates=False 可用于“纯发送工作进程”，节省带宽与内存。
    """
    return TelegramClient(
        session=str(session_path),
        api_id=int(api_id),
        api_hash=api_hash,
        device_model=device_model,
        system_version=system_version,
        app_version=app_version,
        lang_code=lang_code,
        system_lang_code=system_lang_code,
        proxy=proxy,
        local_addr=local_addr,
        receive_updates=receive_updates,
        **_filter_client_kwargs(_DEFAULTS),
    )


@dataclass
class _ClientCacheCtl:
    connection_timeout: int = CLIENT_CONNECT_TIMEOUT
    me_cache_ttl: int = ME_CACHE_TTL


class SessionClientManager:
    """
    Telegram 会话/客户端统一管理（多租户友好版）：
    - 同一手机号可被多个 user_id 使用，彼此不会覆盖/干扰
    - 账号预备：释放旧连接(仅本用户) → 清理 journal → 注册 → 校验
    - 健康状态/锁均采用 (user_id, phone) 复合键隔离
    - Flood 冷却不在此处处理（交由 scheduler/client_validator）
    """

    def __init__(self, session_dir: str = SESSION_DIR, *, receive_updates: bool = True, proxy: dict | tuple | None = None):
        self.session_dir = session_dir
        self._receive_updates = receive_updates
        self._proxy = proxy

        # —— 多视角索引 —— #
        self.clients_by_up: Dict[tuple[int, str], TelegramClient] = {}
        self.clients_user: Dict[int, Dict[str, TelegramClient]] = {}
        self.clients_by_phone: Dict[str, Dict[int, TelegramClient]] = {}

        # —— 其它状态 —— #
        self._user_cooldowns: Dict[int, float] = {}
        self._health_states: Dict[tuple[int, str], HealthState] = {}
        self._phone_locks: Dict[tuple[int, str], asyncio.Lock] = {}

        self._ctl = _ClientCacheCtl()
        os.makedirs(session_dir, exist_ok=True)
        log_debug("[SessionClientManager] 初始化完成", extra={"session_dir": self.session_dir})

    # ---------- 复合键 & 锁 ----------
    @staticmethod
    def _up_key(user_id: int, phone: str) -> tuple[int, str]:
        return (int(user_id), str(phone))

    def _get_phone_lock(self, user_id: int, phone: str) -> asyncio.Lock:
        k = self._up_key(user_id, phone)
        lock = self._phone_locks.get(k)
        if lock is None:
            lock = asyncio.Lock()
            self._phone_locks[k] = lock
        return lock

    # 坏会话隔离元数据
    def _ensure_bad_map(self):
        if not hasattr(self, "_bad_sessions"):
            self._bad_sessions = {}  # key=(user_id, phone) -> bad_until_ts

    def _bad_ttl_expired(self, user_id: int, phone: str) -> bool:
        self._ensure_bad_map()
        bad_until = self._bad_sessions.get((user_id, phone), 0)
        return _now() >= bad_until

    def _mark_bad_session(self, user_id: int, phone: str, ttl_seconds: int = _BAD_TTL_SECONDS):
        self._ensure_bad_map()
        self._bad_sessions[(user_id, phone)] = _now() + max(60, int(ttl_seconds))

    # ---------- 路径/目录 & 附属文件 ----------
    def _ensure_session_dirs(self, user_id: int) -> str:
        user_dir = get_user_dir(user_id)
        os.makedirs(user_dir, exist_ok=True)
        log_debug("确保用户目录存在", extra={"user_id": user_id, "dir": user_dir})
        return user_dir

    def _get_session_path(self, user_id: int, phone: str) -> str:
        return get_phone_session(user_id, phone)

    def _get_json_path(self, user_id: int, phone: str) -> str:
        return get_phone_json(user_id, phone)

    def _cleanup_journal(self, session_path: str) -> None:
        # 清理 Telethon sqlite 附属文件，减少 "database is locked"
        for suf in ("-journal", "-wal", "-shm"):
            p = f"{session_path}{suf}"
            if os.path.exists(p):
                try:
                    os.remove(p)
                    log_info("🧹 清理会话附属文件", extra={"path": p})
                except Exception as e:
                    log_exception("清理会话附属文件失败", exc=e)

    @with_session_lock(default_session_lock_path, timeout=SESSION_LOCK_TIMEOUT)
    async def _cleanup_all_journals_locked(self, user_id: int) -> None:
        self.cleanup_all_journals(user_id)

    def cleanup_all_journals(self, user_id: int) -> None:
        user_dir = get_user_dir(user_id)
        cleaned = 0
        for root, _, files in os.walk(user_dir):
            for fname in files:
                if fname.endswith(".session"):
                    self._cleanup_journal(os.path.join(root, fname))
                    cleaned += 1
        log_info("🧹 清理完成", extra={"user_id": user_id, "sessions_found": cleaned})

    # ---------- 注册/反注册（内部工具） ----------
    def _register_client(self, user_id: int, phone: str, client: TelegramClient) -> None:
        phone = str(phone)
        up = self._up_key(user_id, phone)
        self.clients_by_up[up] = client
        self.clients_user.setdefault(user_id, {})[phone] = client
        self.clients_by_phone.setdefault(phone, {})[user_id] = client

    def _unregister_client(self, user_id: int, phone: str) -> Optional[TelegramClient]:
        phone = str(phone)
        up = self._up_key(user_id, phone)
        cli = self.clients_by_up.pop(up, None)
        if user_id in self.clients_user:
            self.clients_user[user_id].pop(phone, None)
            if not self.clients_user[user_id]:
                self.clients_user[user_id] = {}
        if phone in self.clients_by_phone:
            self.clients_by_phone[phone].pop(user_id, None)
            if not self.clients_by_phone[phone]:
                self.clients_by_phone.pop(phone, None)
        self._health_states.pop(up, None)
        self._phone_locks.pop((user_id, phone), None)
        return cli

    # ---------- 健康状态 ----------
    def get_health_state(self, user_id: int, phone: str) -> HealthState:
        return HealthState.parse(
            self._health_states.get(self._up_key(user_id, phone), HealthState.UNKNOWN)
        )

    def set_health_state(self, user_id: int, phone: str, state) -> None:
        self._health_states[self._up_key(user_id, phone)] = HealthState.parse(state)

    # 兼容旧签名（仅 phone）：聚合 (user_id, phone) 的状态，返回第一个非 UNKNOWN
    def get_health_state_legacy(self, phone: str) -> HealthState | None:
        try:
            phone = str(phone)
        except Exception:
            pass
        # 先查显式记录
        try:
            for (uid, ph), st in (self._health_states or {}).items():
                if ph == phone:
                    s = HealthState.parse(st)
                    if s != HealthState.UNKNOWN:
                        return s
        except Exception:
            pass
        # 退化：若内存中有该手机号的客户端，取第一个用户视角
        try:
            ups = (self.clients_by_phone or {}).get(phone) or {}
            for uid in ups.keys():
                try:
                    s = self.get_health_state(uid, phone)
                    if s is not None:
                        return s
                except Exception:
                    continue
        except Exception:
            pass
        return None



    # HealthChecker 兼容：只传 phone 时，更新所有持有该 phone 的租户状态
    def update_status(self, phone: str, state) -> None:
        phone = str(phone)
        for (uid, p) in list(self.clients_by_up.keys()):
            if p == phone:
                self.set_health_state(uid, phone, state)
        log_debug("Health 状态同步(兼容旧签名)", extra={"phone": phone, "state": str(state)})

    # ---------- 查询 ----------
    def get_client(self, user_or_phone, phone: Optional[str] = None, **kwargs) -> Optional[TelegramClient]:
        if phone is None and isinstance(user_or_phone, str):
            phone = user_or_phone
            user_id = kwargs.get("user_id")
        else:
            user_id = user_or_phone
        if user_id is None or phone is None:
            return None
        return self.clients_by_up.get(self._up_key(int(user_id), str(phone)))

    def get_clients_for_user(self, user_id: int) -> Dict[str, TelegramClient]:
        return self.clients_user.get(int(user_id), {}) or {}

    def get_healthy_clients(self, user_id: int) -> Dict[str, TelegramClient]:
        out: Dict[str, TelegramClient] = {}
        user_bucket = self.clients_user.get(int(user_id), {}) or {}
        for phone, cli in user_bucket.items():
            try:
                if self.get_health_state(user_id, phone) == HealthState.OK and cli.is_connected():
                    out[phone] = cli
                elif phone not in out:
                    out[phone] = cli
            except Exception:
                continue
        return out

    def get_clients_by_phone(self, phone: str) -> Dict[int, TelegramClient]:
        return self.clients_by_phone.get(str(phone), {}) or {}

    def get_all_clients(self) -> Dict[str, TelegramClient]:
        out: Dict[str, TelegramClient] = {}
        for phone, bucket in (self.clients_by_phone or {}).items():
            best = None
            for _uid, cli in (bucket or {}).items():
                try:
                    if self.get_health_state(_uid, phone) == HealthState.OK and cli.is_connected():
                        best = cli
                        break
                except Exception:
                    continue
            if not best:
                for _uid, cli in (bucket or {}).items():
                    best = cli
                    break
            if best:
                out[phone] = best
        return out

    def health_label(self, user_id: int, phone: str, client: Optional[TelegramClient] = None) -> str:
        state = self.get_health_state(user_id, phone)
        emoji = state.emoji() if hasattr(state, "emoji") else "❔"
        zh = state.zh_name() if hasattr(state, "zh_name") else str(state)
        if client is None:
            client = self.get_clients_for_user(user_id).get(str(phone))
        try:
            online = "在线" if (client and client.is_connected()) else "离线"
        except Exception:
            online = "离线"
        return f"{emoji} {zh} / {online}"

    # ---------- 账号删除/清空 ----------
    @with_session_lock(default_session_lock_path, timeout=20.0)
    async def remove_accounts(self, user_id: int, phones: List[str]) -> Dict[str, int]:
        ok, fail = 0, 0
        if not phones:
            return {"ok": 0, "fail": 0}
        for phone in set(map(str, phones)):
            try:
                cli = self._unregister_client(user_id, phone)
                if cli:
                    try:
                        await cli.disconnect()
                    except Exception:
                        pass
                session_path = self._get_session_path(user_id, phone)
                json_path = self._get_json_path(user_id, phone)
                for p in (session_path, session_path + "-journal", session_path + "-wal", session_path + "-shm", json_path):
                    try:
                        if os.path.exists(p):
                            os.remove(p)
                    except Exception:
                        pass
                ok += 1
            except Exception as e:
                log_warning(
                    "删除账号失败",
                    extra={**(get_log_context() or {}), "user_id": user_id, "phone": phone, "err": str(e)},
                )
                fail += 1
        if not self.get_clients_for_user(user_id):
            self.clients_user[user_id] = {}
        return {"ok": ok, "fail": fail}

    async def clear_all_accounts(self, user_id: int) -> Dict[str, int]:
        memory_phones = set(self.get_clients_for_user(user_id).keys())
        file_phones = {p for p, _ in (self.list_user_phones(user_id) or [])}
        all_phones = list(memory_phones.union(file_phones))
        result = await self.remove_accounts(user_id, all_phones)
        try:
            for p, c in list(self.clients_user.get(user_id, {}).items()):
                try:
                    await c.disconnect()
                except Exception:
                    pass
                self._unregister_client(user_id, p)
            self.clients_user[user_id] = {}
        except Exception:
            pass
        return result

    # ---------- 轻微节流（命令入口使用） ----------
    def set_user_cooldown(self, user_id: int):
        self._user_cooldowns[int(user_id)] = time.monotonic()

    def is_user_in_cooldown(self, user_id: int, window_seconds: int = USER_COMMAND_COOLDOWN_WINDOW) -> bool:
        ts = self._user_cooldowns.get(int(user_id))
        return (time.monotonic() - ts) < float(window_seconds) if ts is not None else False

    def register_scheduler(self, scheduler):
        self.scheduler = scheduler

    # ---------- JSON/平台 ----------
    @staticmethod
    def get_platform_from_json(json_path: str) -> str:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                info = json.load(f)
            return info.get("platform", "android")
        except Exception:
            return "android"

    def _ensure_json_fields(self, json_path: str, user_id: int, phone: str) -> Optional[dict]:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log_warning(f"解析 JSON 失败: {json_path} | {e}")
            return None

        platform = data.get("platform", "android")
        patched, data = patch_json_config(data, user_id, phone, platform)
        if patched:
            try:
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                log_info(f"字段补全并写回: {json_path}")
            except Exception as e:
                log_exception(f"写回 JSON 失败: {json_path} | {e}", exc=e)
                return None
        return data

    # ---------- 目录扫描 ----------
    def list_user_phones(self, user_id: int, *, as_pairs: bool = True):
        try:
            trace_id = generate_trace_id()
            set_log_context({"user_id": user_id, "func": "list_user_phones", "trace_id": trace_id})
            user_dir = get_user_dir(user_id)
            log_debug("扫描用户目录", extra={"dir": user_dir})
            phones_pairs: List[Tuple[str, str]] = []
            files_found: List[str] = []
            for root, _, files in os.walk(user_dir):
                for fname in files:
                    if fname.endswith(".session"):
                        phone = fname[:-8]
                        session_path = os.path.join(root, fname)
                        files_found.append(session_path)
                        phones_pairs.append((phone, session_path))
            log_debug("递归找到 session 文件", extra={"files": files_found})
            if not as_pairs:
                seen, out = set(), []
                for p, _ in phones_pairs:
                    if p not in seen:
                        seen.add(p)
                        out.append(p)
                log_debug("提取手机号列表(as_pairs=False)", extra={"phones": out})
                return out
            log_debug("提取手机号列表", extra={"phones": phones_pairs})
            return phones_pairs
        except Exception as e:
            log_warning(
                "list_user_phones 异常",
                extra={**(get_log_context() or {}), "user_id": user_id, "err": str(e)},
            )
            return []

    async def load_all_for_user(self, user_id: int, concurrency: int = LOAD_ALL_CONCURRENCY) -> bool:
        try:
            phones = self.list_user_phones(user_id)  # 已递归
            seen = set()
            sem = asyncio.Semaphore(max(1, int(concurrency)))

            async def _one(_phone: str, sp: str):
                if (user_id, _phone) in seen:
                    return
                seen.add((user_id, _phone))
                async with sem:
                    try:
                        await self.try_create_and_register(user_id, _phone, session_path=sp)
                    except Exception as e:
                        log_warning(
                            "加载账号失败",
                            extra={**(get_log_context() or {}), "user_id": user_id, "phone": _phone, "err": str(e)},
                        )

            await asyncio.gather(*(_one(p, sp) for p, sp in phones))
            return True
        except Exception as e:
            log_warning(
                "扫描用户目录失败",
                extra={**(get_log_context() or {}), "user_id": user_id, "err": str(e)},
            )
            return False

    # ---------- 统一 RPC 封装（供内部/外部调用） ----------
    async def tg_rpc(
        self,
        client: TelegramClient,
        req: Any | List[Any],
        *,
        ordered: bool = False,
        no_updates: bool = False,
        flood_threshold: int | None = None,
    ):
        try:
            return await _tg_call(
                client,
                req,
                ordered=ordered,
                no_updates=no_updates,
                flood_threshold=flood_threshold,
            )
        except Exception as e:
            try:
                uid = getattr(client, "user_id", None)
                ph = getattr(client, "phone", None)
                st, meta = classify_telethon_error(e)
                log_warning(
                    "tg_rpc 失败",
                    extra={"user_id": uid, "phone": ph, "state": str(st), "meta": meta, "err": str(e)},
                )
            except Exception:
                pass
            raise
        except BaseException as e:
            # 统一 MultiError 解包与请求链 trace
            is_multi = bool(TE_MultiError and isinstance(e, TE_MultiError))
            if is_multi:
                try:
                    excs: Iterable = getattr(e, "exceptions", []) or []
                    reqs: Iterable = getattr(e, "requests", []) or []
                    ress: Iterable = getattr(e, "results", []) or []
                    def _fmt_req_chain(r):
                        # 尽力还原 Invoke* 嵌套链
                        names = []
                        cur = r
                        guard = 0
                        while cur is not None and guard < 10:
                            names.append(cur.__class__.__name__)
                            cur = getattr(cur, "query", None)
                            guard += 1
                        return "->".join(names)
                    items = []
                    for i, (ex, rq, rs) in enumerate(zip(excs, reqs, ress)):
                        st_i, meta_i = classify_telethon_error(ex) if isinstance(ex, BaseException) else (None, None)
                        items.append({"idx": i, "error": type(ex).__name__ if ex else None,
                                      "state": str(st_i) if st_i else None,
                                      "meta": meta_i or {}, "request": _fmt_req_chain(rq)})
                    log_warning("tg_rpc MultiError", extra={"user_id": getattr(client, "user_id", None),
                                                           "phone": getattr(client, "phone", None), "items": items})
                except Exception:
                    pass
            raise
    async def _connect_basic(self, client: TelegramClient) -> None:
        if not client.is_connected():
            await client.connect()

    async def ensure_connected(self, client: TelegramClient) -> bool:
        try:
            if not client.is_connected():
                await client.connect()
                self.ensure_parse_mode(client)
            try:
                me = await asyncio.wait_for(client.get_me(), timeout=self._ctl.connection_timeout)
                setattr(client, "_cached_me", (time.monotonic(), me))
                # 成功清零 streak
                try:
                    setattr(client, "__ec_getme_to_streak", 0)
                except Exception:
                    pass
            except asyncio.TimeoutError as te:
                # 记录连续超时计数，超过阈值直接按 NETWORK_ERROR 处理，减少抖动
                cnt = int(getattr(client, "__gm_timeout_count", 0) or 0) + 1
                setattr(client, "__gm_timeout_count", cnt)
                setattr(client, "__last_connect_error", te)
                if cnt >= 3:
                    log_warning("ensure_connected: get_me 连续超时，降级为 NETWORK_ERROR")
                    try:
                        uid = getattr(client, "user_id", None); ph = getattr(client, "phone", None)
                        if uid is not None and ph is not None:
                            self.set_health_state(int(uid), str(ph), HealthState.NETWORK_ERROR)
                    except Exception:
                        pass
                    return False
                else:
                    log_warning("ensure_connected: get_me 超时，视为连接不稳定")
                try:
                    streak = int(getattr(client, "__ec_getme_to_streak", 0) or 0) + 1
                    setattr(client, "__ec_getme_to_streak", streak)
                except Exception:
                    streak = 1
                log_warning("ensure_connected: get_me 超时", extra={"streak": streak})
                if streak >= 2:
                    # 标记网络异常，返回 False 触发上游自愈逻辑
                    try:
                        uid, ph = getattr(client, "user_id", None), getattr(client, "phone", None)
                        if uid is not None and ph is not None:
                            self.set_health_state(int(uid), str(ph), HealthState.NETWORK_ERROR)
                    except Exception:
                        pass
                    return False
            return True

        except te_errors.FloodWaitError as fe:
            try:
                uid = getattr(client, "user_id", None)
                ph = getattr(client, "phone", None)
                if uid is not None and ph is not None:
                    self.set_health_state(int(uid), str(ph), HealthState.FLOOD_WAIT)
            except Exception:
                pass
            setattr(client, "__last_connect_error", fe)
            log_warning(f"ensure_connected: FloodWait {fe.seconds}s")
            return False

        except Exception as e:
            # 解包后再分类（AuthKeyDuplicatedError 等）

            from core.telethon_errors import unwrap_error, _is_auth_expired, TE
            e0 = unwrap_error(e)
            setattr(client, "__last_connect_error", e0)
            try:
                uid = getattr(client, "user_id", None)
                ph = getattr(client, "phone", None)
            except Exception:
                uid = ph = None

            try:
                st, _meta = classify_telethon_error(e)
            except Exception:
                st = HealthState.NETWORK_ERROR


            try:
                if _is_auth_expired(e):
                    st = HealthState.AUTH_EXPIRED
                elif isinstance(e0, (TE.InputUserDeactivatedError, TE.UserDeactivatedBanError)):
                    st = HealthState.BANNED
            except Exception:
                pass

            try:
                if uid is not None and ph is not None:
                    self.set_health_state(user_id=int(uid), phone=str(ph), state=st)
                    if st in BAD_STATES or st == HealthState.AUTH_EXPIRED:
                        self._mark_bad_session(int(uid), str(ph), ttl_seconds=_BAD_TTL_SECONDS)
                        # 特判：AuthKeyDuplicated → 隔离本地会话文件，避免后续继续失败
                        try:
                            if isinstance(e0, TE.AuthKeyDuplicatedError):
                                sp = getattr(client, "session_path", None)
                                if sp:
                                    log_warning("AuthKeyDuplicated → 隔离本地会话文件",
                                                extra={"session": sp, "user_id": uid, "phone": ph})
                                    self._quarantine_session_files(sp)

                        except Exception:
                            pass
            except Exception:
                pass
            log_exception("ensure_connected 失败", exc=e)
            return False
    @staticmethod
    def ensure_parse_mode(client: TelegramClient) -> None:
        """确保客户端 parse_mode = AutoParseMode（官方解析器  自动 HTML/MD）。"""
        # 兼容：可能有外部设置自定义 parse_mode，不强改
        try:
            pm = getattr(client, "parse_mode", None)
            if pm is None or pm.__class__.__name__ != "AutoParseMode":
                client.parse_mode = AutoParseMode()
        except Exception as e:
            log_warning(f"ensure_parse_mode 失败：{e}; fallback=html")
            try:
                client.parse_mode = "html"
            except Exception:
                pass

    async def get_cached_me(self, client: TelegramClient, *, ttl: int | None = None) -> Optional[Any]:
        ttl = self._ctl.me_cache_ttl if ttl is None else ttl
        try:
            now = time.monotonic()
            ts_obj = getattr(client, "_cached_me", None)
            if isinstance(ts_obj, tuple) and len(ts_obj) == 2:
                ts, me = ts_obj
                if me and now - float(ts) <= float(ttl):
                    return me
            ok = await self.ensure_connected(client)
            if not ok:
                return None
            ts_obj = getattr(client, "_cached_me", None)
            if isinstance(ts_obj, tuple) and len(ts_obj) == 2 and ts_obj[1]:
                return ts_obj[1]
            me = await client.get_me()
            setattr(client, "_cached_me", (now, me))
            return me
        except Exception as e:
            log_warning("get_cached_me 异常", extra={**(get_log_context() or {}), "err": str(e)})
            return None

    async def safe_disconnect(self, client: TelegramClient) -> None:
        try:
            if client and client.is_connected():
                await client.disconnect()
        except Exception:
            pass

    async def _safe_disconnect(self, client: TelegramClient, *, reason: str = "") -> None:
        try:
            if client and client.is_connected():
                await client.disconnect()
            if reason:
                log_debug("client_disconnected", extra={"reason": reason})
        except Exception:
            pass

    # ---------- 对外：核心入口 ----------
    async def prepare_clients_for_user(
        self,
        user_id: int,
        *,
        release_existing: bool = True,
        cleanup_journals: bool = True,
        validator=None,
        set_cooldown: bool = True,
        deep_scan: bool = True,
        log_prefix: str = "统一入口",
        phones_whitelist: Optional[List[str]] = None,
        skip_known_bad: bool = True,
    ) -> Dict[str, TelegramClient]:
        pairs = self.list_user_phones(user_id) or []
        all_phones = [str(p) for p, _ in pairs]
        phones = [p for p in all_phones if (not phones_whitelist or p in phones_whitelist)]

        if skip_known_bad:
            filtered = []
            for ph in phones:
                try:
                    st = self.get_health_state(user_id, ph)
                except Exception:
                    st = None
                if st in BAD_STATES and not self._bad_ttl_expired(user_id, ph):
                    log_info("prepare_clients_for_user:skip_known_bad",
                             extra={"user_id": user_id, "phone": ph, "state": str(st)})
                    continue
                filtered.append(ph)
            phones = filtered

        result: Dict[str, TelegramClient] = {}
        for ph in phones:
            cli = self.get_client(user_id, ph)
            if release_existing and cli:
                try:
                    if not cli.is_connected():
                        await self._safe_disconnect(cli, reason="stale_release")
                except Exception:
                    pass

            # 统一路径：无论已注册/断连/未注册，都走 get_client_or_connect，内部确保 parse_mode
            cli2 = await self.get_client_or_connect(user_id, ph)
            if cli2:
                result[ph] = cli2
            else:
                log_warning(
                    "无法准备账号",
                    extra={
                        **(get_log_context() or {}),
                        "user_id": user_id,
                        "phone": ph,
                        "hint": "get_client_or_connect 返回 None",
                    },
                )

        log_debug("prepare_clients_for_user:done",
                  extra={"user_id": user_id, "count": len(result), "requested": len(phones)})
        return result

    async def release_all_clients(self, user_id: int) -> None:
        clients = self.clients_user.get(user_id, {})
        for phone, client in list(clients.items()):
            try:
                await client.disconnect()
                log_info("🛑 断开 client", extra={"phone": phone, "user_id": user_id})
            except Exception as e:
                log_warning("⚠️ 断开 client 失败", extra={"phone": phone, "user_id": user_id, "err": str(e)})
            self._unregister_client(user_id, phone)
        self.clients_user[user_id] = {}

    # ---------- 会话注册（幂等 & 退避） ----------
    @with_session_lock(default_session_lock_path, timeout=SESSION_LOCK_TIMEOUT)
    async def try_create_and_register(
        self,
        user_id: int,
        phone: str,
        session_path: Optional[str] = None,
        *,
        validator=None,
        cleanup_journals: bool = True,
        set_cooldown: bool = False,
        log_prefix: str = "",
    ) -> Optional[TelegramClient]:
        """
        幂等注册流程（多租户隔离）：
        - 跨进程文件锁 + (user_id, phone) 进程内 asyncio.Lock
        - 清理 SQLite 附属文件
        - create_client() 统一参数
        - 连接 + 授权 get_me 探针
        - parse_mode = AutoParseMode()
        - 失败返回 None，成功返回已注册的 TelegramClient
        - 若该 (user_id, phone) 在坏会话隔离 TTL 内，直接短路返回 None
        """
        phone = str(phone)

        try:
            st = self.get_health_state(user_id, phone)
        except Exception:
            st = None
        if st in BAD_STATES and not self._bad_ttl_expired(user_id, phone):
            log_info("try_create_and_register:skip_known_bad",
                     extra={"user_id": user_id, "phone": phone, "state": str(st)})
            return None

        async with self._get_phone_lock(user_id, phone):
            trace_id = generate_trace_id()
            set_log_context(
                {"user_id": user_id, "phone": phone, "func": "try_create_and_register", "trace_id": trace_id}
            )

            if session_path is None:
                session_path = self._get_session_path(user_id, phone)
            json_path = self._get_json_path(user_id, phone)

            log_debug("注册账号尝试", extra={
                "phone": phone,
                "session_path": session_path,
                "json_path": json_path,
                **(get_log_context() or {}),
            })

            existing = self.get_client(user_id, phone)
            if existing:
                try:
                    await existing.disconnect()
                except Exception as e:
                    log_warning("断开已存在 client 出错", extra={"phone": phone, "err": str(e), **(get_log_context() or {})})
                self._unregister_client(user_id, phone)

            if not (os.path.exists(session_path) and os.path.exists(json_path)):
                log_warning("Session/Json 文件缺失", extra={
                    "session_path": session_path, "json_path": json_path, **(get_log_context() or {})
                })
                return None

            client: Optional[TelegramClient] = None
            try:
                info = self._ensure_json_fields(json_path, user_id, phone)

                safe_info = None
                try:
                    if info:
                        safe_info = {
                            "platform": info.get("platform"),
                            "lang": f"{info.get('lang_code','en')}/{info.get('system_lang_code','en')}",
                            "device_model": bool(info.get("device_model")),
                            "system_version": bool(info.get("system_version")),
                            "app_version": bool(info.get("app_version")),
                            "api_id_present": bool(info.get("api_id")),
                            "api_hash_present": bool(info.get("api_hash")),
                        }
                except Exception:
                    pass
                log_debug("json字段内容(脱敏)", extra={"info": safe_info, **(get_log_context() or {})})

                if not info:
                    log_warning("⚠️ 跳过无效账号（JSON 不可用）", extra=get_log_context())
                    return None

                for k in REQUIRED_API_FIELDS:
                    if k not in info or not info[k]:
                        log_warning("账号字段缺失或为空", extra={"field": k, "json": json_path, **(get_log_context() or {})})
                        return None

                if cleanup_journals:
                    self._cleanup_journal(session_path)

                client = create_client(
                    session_path=session_path,
                    api_id=int(info["api_id"]),
                    api_hash=info["api_hash"],
                    device_model=info["device_model"],
                    system_version=info["system_version"],
                    app_version=info["app_version"],
                    lang_code=info.get("lang_code", "en"),
                    system_lang_code=info.get("system_lang_code", "en"),
                    proxy=self._proxy,
                    receive_updates=self._receive_updates,
                )

                try:
                    setattr(client, "user_id", user_id)
                    setattr(client, "phone", phone)
                    setattr(client, "session_path", session_path)
                except Exception:
                    pass

                log_info("✅ TelegramClient 构造完成", extra={
                    "session": session_path,
                    "lang": f"{info.get('lang_code','en')}/{info.get('system_lang_code','en')}",
                    "device_model": info["device_model"],
                    "system_version": info["system_version"],
                    "app_version": info["app_version"],
                    "trace_id": trace_id,
                })

                self.ensure_parse_mode(client)

                ok = await self.ensure_connected(client)
                if not ok:
                    log_warning("账号未授权或 get_me 失败", extra=get_log_context())
                    await self.safe_disconnect(client)
                    return None

                try:
                    me_ts, me_obj = getattr(client, "_cached_me", (None, None))
                    setattr(client, "self_id", getattr(me_obj, "id", None))
                except Exception:
                    pass

                self._register_client(user_id, phone, client)
                self.set_health_state(user_id, phone, HealthState.OK)

                log_info("✅ 账号注册成功", extra={
                    "me_id": getattr(getattr(client, "_cached_me", (None, None))[1], "id", None),
                    "phone": phone,
                    "username": getattr(getattr(client, "_cached_me", (None, None))[1], "username", ""),
                    "trace_id": trace_id,
                })

                try:
                    attach_runtime_event_listeners(client, user_id, scheduler=getattr(self, "scheduler", None))
                except Exception as _e:
                    log_warning("attach_runtime_event_listeners 失败（忽略）", extra={"err": str(_e), **(get_log_context() or {})})

                return client

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                log_exception("注册客户端失败", exc=e, extra={"phone": phone, "trace": tb, **(get_log_context() or {})})
                self.set_health_state(user_id, phone, HealthState.NETWORK_ERROR)
                try:
                    if client:
                        await client.disconnect()
                except Exception as ee:
                    log_warning(f"二次断开 client 失败: {phone} | {ee}", extra=get_log_context())
                return None

    async def get_client_or_connect(self, user_id: int, phone: str) -> Optional[TelegramClient]:
        """
        获取已注册的客户端；如果不存在或已断开则（重）连，并再次确保 parse_mode。
        若完全不存在，则走 try_create_and_register 的统一路径。
        """
        phone = str(phone)
        cli = self.get_client(user_id, phone)
        if cli is None:
            return await self.try_create_and_register(
                user_id=user_id,
                phone=phone,
                validator=None,
                cleanup_journals=False,
                set_cooldown=False,
                log_prefix="get_client_or_connect",
        )
            # 已注册：确保连接 + 轻探针
        try:
            if not cli.is_connected():
                await cli.connect()
            self.ensure_parse_mode(cli)
            try:
                me = await asyncio.wait_for(cli.get_me(), timeout=self._ctl.connection_timeout)
                setattr(cli, "_cached_me", (time.monotonic(), me))
            except Exception:
                pass
            return cli
        except Exception as e:
            log_exception("get_client_or_connect: reconnect 失败", exc=e,
                          extra={"user_id": user_id, "me_phone": phone})
            # 兜底：完整重建
            return await self.try_create_and_register(
                user_id=user_id,
                phone=phone,
                validator=None,
                cleanup_journals=True,
                set_cooldown=False,
                log_prefix="get_client_or_connect.rebuild",
            )
    # —— 新增：会话隔离工具（不删除，只重命名到 .dup.bak，便于人工排查） —— #
    def _quarantine_session_files(self, session_path: str) -> None:
        import shutil, time, os
        base = session_path
        suffix = f".dup.{int(time.time())}.bak"
        for suf in ("", "-journal", "-wal", "-shm"):
            p = f"{base}{suf}"
            if os.path.exists(p):
                try:
                    shutil.move(p, f"{p}{suffix}")
                except Exception:
                    pass
