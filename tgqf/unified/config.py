# unified/config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
统一配置入口（aligned）
- 以环境变量为主，统一常量命名；为兼容历史代码，保留必要的别名（完全等值）。
- 注释对齐到各模块的实际用法：link_utils / scheduler / client_manager / sender / group_assigner 等。
"""

import os
from pathlib import Path
import signal
import time
from typing import Iterable, List, Set, Tuple

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError

# ── 环境变量加载（支持 SIGHUP 热重载）
_config_last_loaded_time = 0.0
_config_reload_interval = 10.0  # 秒：最短重载间隔（避免频繁 I/O）


def _load_env() -> None:
    """从 .env 及系统环境加载（系统优先），更新最后加载时间。"""
    global _config_last_loaded_time
    load_dotenv(override=True)
    _config_last_loaded_time = time.time()


def _reload_env_if_needed() -> None:
    if time.time() - _config_last_loaded_time > _config_reload_interval:
        _load_env()


try:
    # SIGHUP 触发热重载：kill -HUP <pid>
    signal.signal(signal.SIGHUP, lambda *_: _load_env())
except Exception:
    pass

_load_env()


def get_config(key: str, default: str = "") -> str:
    _reload_env_if_needed()
    return os.getenv(key, str(default))


def get_bool(key: str, default: bool = False) -> bool:
    """布尔解析：1/true/yes/on → True；0/false/no/off → False。大小写不敏感。"""
    v = get_config(key, "1" if default else "0").strip().lower()
    return v in ("1", "true", "yes", "on", "y", "t")


def _first_env(*keys: str, default: str = "") -> str:
    """返回第一个存在的环境变量值（用于向后兼容多名称），否则 default。"""
    _reload_env_if_needed()
    for k in keys:
        if k in os.environ and os.environ[k] != "":
            return os.environ[k]
    return str(default)


# ───────────────────────────
# 管理员 ID（数字/用户名）缓存与解析
# ───────────────────────────
_cached_ids: Set[int] = set()
_cached_usernames_resolved: Set[str] = set()
_cached_loaded: bool = False

# 推荐新变量名：ADMIN_IDS
# 兼容旧变量：DEFAULT_ADMIN_IDS（逗号分隔，支持纯数字或 @username）
_ADMIN_IDS_RAW = get_config("ADMIN_IDS", "")
_DEFAULT_ADMIN_IDS_RAW = get_config("DEFAULT_ADMIN_IDS", "")  # 仅兼容老项目；新项目请写 ADMIN_IDS
_DEFAULT_ADMIN_LIST: List[int] = []  # 代码内静态补充（通常保持空）


def _parse_admin_tokens(raw: str | Iterable[str | int]) -> Tuple[Set[int], List[str]]:
    ids: Set[int] = set()
    usernames: List[str] = []

    def _feed(token: str):
        s = (str(token) or "").strip()
        if not s:
            return
        if s.lstrip("-").isdigit():
            try:
                ids.add(int(s))
            except Exception:
                pass
        elif s.startswith("@"):
            usernames.append(s)

    if isinstance(raw, str):
        for item in raw.split(","):
            _feed(item)
    else:
        for item in (raw or []):
            _feed(item)

    return ids, usernames


def get_admin_ids(client: TelegramClient | None = None, force_reload: bool = False) -> Set[int]:
    """
    同步返回“已知数字ID + 已缓存解析成功的用户名”的并集。
    - 不触发网络解析（用户名→ID），仅读取缓存与数字。
    - 若存在待解析的用户名且尚未 ensure_admin_ids()，会打一条警告日志（仅一次）。
    ENV: ADMIN_IDS（推荐）或 DEFAULT_ADMIN_IDS（兼容旧）
    """
    from unified.logger import log_warning

    global _cached_ids, _cached_usernames_resolved, _cached_loaded

    if force_reload:
        _load_env()
        _cached_ids.clear()
        _cached_usernames_resolved.clear()
        _cached_loaded = False

    static_ids_env, usernames_env = _parse_admin_tokens(_ADMIN_IDS_RAW)
    static_ids_def, usernames_def = _parse_admin_tokens(_DEFAULT_ADMIN_IDS_RAW or _DEFAULT_ADMIN_LIST)

    result = set(static_ids_env) | set(static_ids_def) | set(_cached_ids)

    pending = [u for u in (usernames_env + usernames_def) if u not in _cached_usernames_resolved]
    if pending and not _cached_loaded and client is None:
        log_warning(f"以下管理员用户名尚未解析（等待 ensure_admin_ids）：{pending}")
        _cached_loaded = True

    return result


async def ensure_admin_ids(client: TelegramClient, force_reload: bool = False) -> Set[int]:
    """
    异步解析 ADMIN_IDS 中的 @用户名 → 数字 ID，并写入缓存。
    - 调用时机：Bot 启动后、或 Telethon client 就绪后。
    - 解析失败会记录日志；下次仍会重试（除非明确判定用户名无效）。
    """
    from unified.logger import log_error, log_warning

    global _cached_ids, _cached_usernames_resolved, _cached_loaded

    if force_reload:
        _load_env()
        _cached_ids.clear()
        _cached_usernames_resolved.clear()
        _cached_loaded = False

    static_ids_env, usernames_env = _parse_admin_tokens(_ADMIN_IDS_RAW)
    _cached_ids |= static_ids_env

    static_ids_def, usernames_def = _parse_admin_tokens(_DEFAULT_ADMIN_IDS_RAW or _DEFAULT_ADMIN_LIST)
    _cached_ids |= static_ids_def

    to_resolve = [u for u in (usernames_env + usernames_def) if u not in _cached_usernames_resolved]
    if not to_resolve:
        return set(_cached_ids)

    for uname in to_resolve:
        try:
            ent = await client.get_entity(uname)
            uid = int(getattr(ent, "id", 0) or 0)
            if uid:
                _cached_ids.add(uid)
                _cached_usernames_resolved.add(uname)
        except (UsernameNotOccupiedError, UsernameInvalidError):
            log_warning(f"无效管理员用户名：{uname}")
            _cached_usernames_resolved.add(uname)
        except Exception as e:
            log_error(f"管理员用户名解析失败：{uname} → {e}")
            # 不标记失败，允许下次重试

    return set(_cached_ids)


def is_admin(user_id: int, client: TelegramClient | None = None) -> bool:
    try:
        return int(user_id) in get_admin_ids()
    except Exception:
        return False


# ───────────────────────────
# 目录/文件路径（可被各自 ENV 覆盖）
# ───────────────────────────
BASE_DIR    = os.path.abspath(get_config("DATA_DIR", "./data"))
LOCK_DIR    = os.path.abspath(get_config("LOCK_DIR", "./locks"))         # 进程/文件锁目录
SESSION_DIR = os.path.abspath(get_config("SESSION_DIR", "./sessions"))   # 会话文件根目录
CONF_DIR    = os.path.abspath(get_config("CONF_DIR", "./conf"))          # 配置文件目录
MEDIA_DIR   = os.path.abspath(get_config("MEDIA_DIR", "./media"))        # 媒体文件目录
LOG_DIR     = os.path.abspath(get_config("LOG_DIR", "./logs"))           # 日志目录

# 日志配置
LOG_FILE    = get_config("LOG_FILE", "app.log")                          # 日志文件名
LOG_PATH    = os.path.join(LOG_DIR, LOG_FILE)                            # 日志完整路径
LOG_FORMAT  = "[%(asctime)s][%(levelname)s] %(message)s"                 # 日志格式
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _ensure_dirs_and_files() -> None:
    """
    导入时确保关键目录和必要文件存在：
    - 创建 BASE/LOCK/SESSION/CONF/MEDIA/LOG 目录
    - 创建（touch）日志文件 LOG_PATH
    - （不主动创建 .session 文件，交给 Telethon 在连接时生成）
    """
    dirs = (BASE_DIR, LOCK_DIR, SESSION_DIR, CONF_DIR, MEDIA_DIR, LOG_DIR)
    for p in dirs:
        try:
            Path(p).mkdir(parents=True, exist_ok=True)
        except Exception:
            # 某些受限环境（如只读挂载）可能失败，忽略以便后续重试或外部准备
            pass

    # 日志文件：若不可写或目录不存在则静默忽略，避免阻塞启动
    try:
        Path(LOG_PATH).touch(exist_ok=True)
    except Exception:
        pass


# 模块导入即确保路径/文件就绪
_ensure_dirs_and_files()


# ───────────────────────────
# Bot 会话/鉴权
# ───────────────────────────
BOT_SESSION_NAME = get_config("BOT_SESSION_NAME", "kcdbot.session")      # 机器人会话文件名
BOT_SESSION      = os.path.join(SESSION_DIR, BOT_SESSION_NAME)           # 机器人会话路径

# UI 回调旁路调试开关（默认 False；ENV 可覆盖）
DEBUG_BYPASS_CALLBACKS = get_bool("DEBUG_BYPASS_CALLBACKS", False)


def get_bot_session_path() -> str:
    # 兜底确保会话目录存在
    os.makedirs(SESSION_DIR, exist_ok=True)
    return BOT_SESSION


# ── Telegram API 鉴权 ─────────────────────────────────────────
API_ID    = int(get_config("API_ID", "123456"))       # Telegram API ID
API_HASH  = get_config("API_HASH", "your_hash")     # Telegram API HASH
BOT_TOKEN = get_config("BOT_TOKEN", "your_token")     # Bot Token


# ───────────────────────────
# 全局并发/超时/重试（调度 & 执行）
# ───────────────────────────
# ENV: MAX_CONCURRENCY（默认 12）
# 用途：账号级并行阈值（调度器、链接检查、任务执行等统一引用）
MAX_CONCURRENCY = int(get_config("MAX_CONCURRENCY", "12"))

# ENV: RUNNER_TIMEOUT（默认 35）— 单账号串行发送器外层保护性超时
RUNNER_TIMEOUT = int(get_config("RUNNER_TIMEOUT", "35"))

# ENV: ATTEMPTS（默认 1）— 通用重试基线（模块可自行细化）
ATTEMPTS = int(get_config("ATTEMPTS", "1"))

# ENV: PAGE_SIZE（默认 10）— 通用分页条数（管理面板等）
PAGE_SIZE = int(get_config("PAGE_SIZE", "10"))

# ENV: TASK_ROUND_INTERVAL_DEFAULT（默认 60）— 调度器轮次间隔
TASK_ROUND_INTERVAL_DEFAULT = int(get_config("TASK_ROUND_INTERVAL_DEFAULT", "60"))

# ENV: ASSIGN_TIMEOUT_DEFAULT_SECONDS（默认 20）— 入群公开链接超时
ASSIGN_TIMEOUT_DEFAULT_SECONDS = int(get_config("ASSIGN_TIMEOUT_DEFAULT_SECONDS", "20"))

# ENV: ASSIGN_TIMEOUT_INVITE_SECONDS（默认 35）— 入群邀请链接超时
ASSIGN_TIMEOUT_INVITE_SECONDS = int(get_config("ASSIGN_TIMEOUT_INVITE_SECONDS", "35"))

# ENV: JOINED_USERNAMES_TTL_SECONDS（默认 86400）— 已加入用户名缓存 TTL
JOINED_USERNAMES_TTL_SECONDS = int(get_config("JOINED_USERNAMES_TTL_SECONDS", "86400"))

# ENV: LINKS_CHECK_MAX_CONCURRENCY（默认=MAX_CONCURRENCY）— 链接预检并发
LINKS_CHECK_MAX_CONCURRENCY = int(get_config("LINKS_CHECK_MAX_CONCURRENCY", str(MAX_CONCURRENCY)))

# ENV: PARSE_LINK_INTERVAL（默认 0.2）— 链接解析节流
PARSE_LINK_INTERVAL = float(get_config("PARSE_LINK_INTERVAL", "0.2"))
# ENV: PARSE_LINK_INTERVAL_MIN（默认 0.2）— 下限保护
PARSE_LINK_INTERVAL_MIN = float(get_config("PARSE_LINK_INTERVAL_MIN", "0.2"))

# ENV: REASSIGN_LOOP_INTERVAL（默认 60）— 申请制目标重分配主循环间隔
REASSIGN_LOOP_INTERVAL = int(get_config("REASSIGN_LOOP_INTERVAL", "60"))
# ENV: REASSIGN_JITTER_MIN/MAX（默认 0.5/1.2）— 重分配抖动范围
REASSIGN_JITTER_MIN = float(get_config("REASSIGN_JITTER_MIN", "0.5"))
REASSIGN_JITTER_MAX = float(get_config("REASSIGN_JITTER_MAX", "1.0"))
# ENV: REASSIGN_MAX_TRY_PER_ROUND（默认 2）— 每轮最大重试目标数
REASSIGN_MAX_TRY_PER_ROUND = int(get_config("REASSIGN_MAX_TRY_PER_ROUND", "2"))

# ENV: FLOOD_RESTORATION_INTERVAL（默认 60）— FLOOD 状态自动恢复轮询间隔
FLOOD_RESTORATION_INTERVAL = int(get_config("FLOOD_RESTORATION_INTERVAL", "600"))
# ENV: FLOOD_WAIT_KEY_PREFIX（默认 cooldown:floodwait:）— FLOOD 冷却键前缀
FLOOD_WAIT_KEY_PREFIX = get_config("FLOOD_WAIT_KEY_PREFIX", "cooldown:floodwait:")

# ENV: FLOOD_AUTOSLEEP_THRESHOLD（默认 600）
# 用途：Telethon 报 Flood/Slowmode 且 seconds <= 阈值时自动 sleep（见 client_manager / sender）
FLOOD_AUTOSLEEP_THRESHOLD = int(get_config("FLOOD_AUTOSLEEP_THRESHOLD", "600"))

# ENV: CLIENT_SCAN_CONCURRENCY（默认 10）— ClientManager 扫描并发
CLIENT_SCAN_CONCURRENCY = int(get_config("CLIENT_SCAN_CONCURRENCY", "12"))

# ENV: INVITE_COOL_MAX_SECONDS（默认 30）— 邀请链接冷却上限（message_sender）
INVITE_COOL_MAX_SECONDS = int(get_config("INVITE_COOL_MAX_SECONDS", "30"))

# ENV: ENTITY_WARM_DIALOGS_LIMIT（默认 200）— dialogs 预热上限（BaseScheduler.ensure_entity_seen）
ENTITY_WARM_DIALOGS_LIMIT = int(get_config("ENTITY_WARM_DIALOGS_LIMIT", "200"))


# ───────────────────────────
# Telethon 客户端默认参数（client_factory / client_manager 统一取值）
# ───────────────────────────
# 是否启用 IPv6；用于 TelegramClient(...) 的 use_ipv6
TG_USE_IPV6 = get_bool("TG_USE_IPV6", False)
# Telethon 层的全局超时（秒）；用于 TelegramClient(...) 的 timeout
TG_TIMEOUT = int(get_config("TG_TIMEOUT", "15"))
# RPC 请求重试次数；用于 TelegramClient(...) 的 request_retries
TG_REQUEST_RETRIES = int(get_config("TG_REQUEST_RETRIES", "3"))
# 连接重试次数；用于 TelegramClient(...) 的 connection_retries
TG_CONNECTION_RETRIES = int(get_config("TG_CONNECTION_RETRIES", "3"))
# 重试间隔（秒）；用于 TelegramClient(...) 的 retry_delay
TG_RETRY_DELAY = int(get_config("TG_RETRY_DELAY", "3"))
# 掉线自动重连；用于 TelegramClient(...) 的 auto_reconnect
TG_AUTO_RECONNECT = get_bool("TG_AUTO_RECONNECT", True)
# 是否按顺序推送 updates；用于 TelegramClient(...) 的 sequential_updates
TG_SEQUENTIAL_UPDATES = get_bool("TG_SEQUENTIAL_UPDATES", False)
# 实体缓存上限；用于 TelegramClient(...) 的 entity_cache_limit
TG_ENTITY_CACHE_LIMIT = int(get_config("TG_ENTITY_CACHE_LIMIT", "3000"))
# 是否向上传递最后一次调用错误（Telethon 参数）；用于 client_manager._DEFAULTS.raise_last_call_error
TELETHON_RAISE_LAST_CALL_ERROR = get_bool("TELETHON_RAISE_LAST_CALL_ERROR", True)

# get_me 探针超时 & 缓存 TTL（client_manager.ensure_connected / get_cached_me）
# - CLIENT_CONNECT_TIMEOUT：ensure_connected 内部对 get_me 的探针超时
# - ME_CACHE_TTL：get_cached_me 的缓存有效期（秒）
CLIENT_CONNECT_TIMEOUT = int(get_config("CLIENT_CONNECT_TIMEOUT", "10"))
ME_CACHE_TTL = int(get_config("ME_CACHE_TTL", "300"))
# 会话文件级互斥锁的等待超时（秒），用于 with_session_lock
SESSION_LOCK_TIMEOUT = float(get_config("SESSION_LOCK_TIMEOUT", "20.0"))
# 用户命令操作的轻微节流窗口（秒），用于 SessionClientManager.is_user_in_cooldown
USER_COMMAND_COOLDOWN_WINDOW = int(get_config("USER_COMMAND_COOLDOWN_WINDOW", "10"))
# load_all_for_user 的并发扫描度
LOAD_ALL_CONCURRENCY = int(get_config("LOAD_ALL_CONCURRENCY", "12"))
# ENV: SCHEDULE_MAX_WEEKS_AHEAD（默认 30）— 计划任务可接受的最远时间（防止超大 RRULE）
SCHEDULE_MAX_WEEKS_AHEAD = int(get_config("SCHEDULE_MAX_WEEKS_AHEAD", "30"))

# ENV: LOG_SAMPLE_COUNT（默认 10）— 日志采样条数（打印样本时）
LOG_SAMPLE_COUNT = int(get_config("LOG_SAMPLE_COUNT", "10"))

# 固定阶梯延迟（入群间隔规则），与业务约定相关，保留为常量：
# 解释：第1个群后延迟1.5~3.0秒进入第2群；之后阶梯：20,60,180,360,600 秒
JOIN_STAIR_DELAYS = [(1.5, 3.0), 10, 15, 20, 30, 30]


# ───────────────────────────
# 上传/解包安全阈值（ZIP/RAR）
# ───────────────────────────
# ENV: UPLOAD_MAX_ZIP_ENTRIES（默认 5000）
UPLOAD_MAX_ZIP_ENTRIES = int(get_config("UPLOAD_MAX_ZIP_ENTRIES", "5000"))
# 单条目字节上限（默认 200MB）
UPLOAD_MAX_ENTRY_SIZE = int(get_config("UPLOAD_MAX_ENTRY_SIZE", str(200 * 1024 * 1024)))
# 总大小字节上限（默认 2GB）
UPLOAD_MAX_TOTAL_SIZE = int(get_config("UPLOAD_MAX_TOTAL_SIZE", str(2 * 1024 * 1024 * 1024)))


# ───────────────────────────
# 路径工具（sessions 结构）
# ───────────────────────────
def get_user_dir(user_id: int) -> str:
    """返回某用户的根目录：sessions/<user_id>/"""
    path = os.path.join(SESSION_DIR, str(user_id))
    os.makedirs(path, exist_ok=True)
    return path

def get_phone_dir(user_id: int, phone: str) -> str:
    """返回某用户-手机号的目录：sessions/<user_id>/<phone>/"""
    path = os.path.join(get_user_dir(user_id), phone)
    os.makedirs(path, exist_ok=True)
    return path

def get_phone_session(user_id: int, phone: str) -> str:
    """返回会话文件路径：.../<phone>.session"""
    return os.path.join(get_phone_dir(user_id, phone), f"{phone}.session")

def get_phone_json(user_id: int, phone: str) -> str:
    """返回账号 JSON 配置路径：.../<phone>.json"""
    return os.path.join(get_phone_dir(user_id, phone), f"{phone}.json")

def get_tdata_dir(user_id: int) -> str:
    """临时 tdata 解包目录：sessions/<user_id>/tdata_tmp/"""
    path = os.path.join(get_user_dir(user_id), "tdata_tmp")
    os.makedirs(path, exist_ok=True)
    return path

def get_zip_dir(user_id: int) -> str:
    """临时 zip 目录：sessions/<user_id>/zip_tmp/"""
    path = os.path.join(get_user_dir(user_id), "zip_tmp")
    os.makedirs(path, exist_ok=True)
    return path

def ensure_all_dirs() -> None:
    """确保关键目录存在（被启动或安装脚本调用）"""
    for path in (SESSION_DIR, CONF_DIR, MEDIA_DIR, LOG_DIR, LOCK_DIR, BASE_DIR):
        os.makedirs(path, exist_ok=True)
    # 可选：确保日志文件存在
    try:
        Path(LOG_PATH).touch(exist_ok=True)
    except Exception:
        pass