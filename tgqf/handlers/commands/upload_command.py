# handlers/commands/upload\_command.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any, Awaitable, Callable, List, Optional, Tuple, Iterable, Set

from opentele.api import UseCurrentSession
from opentele.exception import (
    OpenTeleException,
    TDataBadDecryptKey,
    TDataInvalidMagic,
    TelethonUnauthorized,
    TFileNotFound,
)
from opentele.td import TDesktop
from telethon import Button, events
from telethon.errors.common import AlreadyInConversationError
from telethon.tl.custom.conversation import Conversation
from telethon.tl.types import MessageMediaDocument

from core.decorators import super_command
from unified.config import (
    get_phone_json,
    get_phone_session,
    get_tdata_dir,
    get_user_dir,
    get_zip_dir,
    UPLOAD_MAX_ZIP_ENTRIES,
    UPLOAD_MAX_ENTRY_SIZE,
    UPLOAD_MAX_TOTAL_SIZE,
    RUNNER_TIMEOUT,
)
from unified.context import get_client_manager
from unified.lock_utils import default_session_lock_path, with_session_lock
from unified.logger import log_error, log_exception, log_info, log_warning, log_debug
from unified.trace_context import (
    generate_trace_id,
    get_log_context,
    inject_trace_context,
    set_log_context,
)
from ui.constants import (
    UPLOAD_OPEN, UPLOAD_DONE, UPLOAD_CANCEL,
)
from core.defaults.bot_utils import BotUtils

# ===================== 压缩包安全阈值（防炸弹） =====================
MAX_ZIP_ENTRIES = UPLOAD_MAX_ZIP_ENTRIES
MAX_ENTRY_SIZE  = UPLOAD_MAX_ENTRY_SIZE
MAX_TOTAL_SIZE  = UPLOAD_MAX_TOTAL_SIZE

# ===================== 维护当前聊天的活动会话 =====================
ACTIVE_CONVS: dict[int, Conversation] = {}


def set_active_conversation(chat_id: int, conv: Conversation | None):
    """在进入/退出上传会话时注册或清理句柄。"""
    if conv is None:
        ACTIVE_CONVS.pop(chat_id, None)
    else:
        ACTIVE_CONVS[chat_id] = conv


def cancel_active_conversation(chat_id: int) -> bool:
    """同步取消当前聊天的会话；返回是否取消到句柄。"""
    conv = ACTIVE_CONVS.pop(chat_id, None)
    if conv:
        try:
            conv.cancel()  # 同步方法，不需要 await
            return True
        except Exception as e:
            log_warning("取消会话失败", extra={**(get_log_context() or {}), "err": str(e)})
    return False


# ✅ 新增：按 chat_id 记录“本次会话转换成功的手机号集合”
_CONVERTED_BY_CHAT: dict[int, Set[str]] = {}


# ===================== 内联按钮（upload:*） =====================
upload_menu_buttons = [
    [
        Button.inline("✅ 完成上传", data=UPLOAD_DONE),
        Button.inline("❌ 取消上传", data=UPLOAD_CANCEL),
    ],
]


def _upload_markup(client) -> Any:
    """仅返回二维按钮数组；由 send_message(..., buttons=...) 负责渲染。"""
    return upload_menu_buttons


async def _notify_rar_runtime_if_needed(conv) -> None:
    """会话入口提示 RAR 依赖缺失（不中断 ZIP 流程）。"""
    try:
        if _rar_runtime_ok():
            return
        msg = (
            "⚠️ 当前环境未检测到 `unrar/unar`，RAR 解压不可用，仅支持 ZIP。\n"
            "• Ubuntu/Debian:  `sudo apt-get update && sudo apt-get install -y unar`  或 `unrar`\n"
            "• CentOS/RHEL:    `sudo yum install -y unar`  或 `unrar`\n"
            "• macOS (Homebrew): `brew install unar`\n"
            "你仍可继续上传 ZIP 压缩包。"
        )
        await conv.send_message(msg)
    except Exception:
        pass


# ===================== 工具函数 =====================

def get_user_dirs(user_id: int) -> Tuple[str, str, str]:
    """返回三元组: (用户根目录, 压缩包缓存目录, tdata 工作目录)。仅返回路径，创建动作在调用处统一处理。"""
    return get_user_dir(user_id), get_zip_dir(user_id), get_tdata_dir(user_id)


def clean_dir(dir_path: str):
    """清理目录下所有内容，但保留目录本身。"""
    os.makedirs(dir_path, exist_ok=True)
    try:
        with os.scandir(dir_path) as it:
            for entry in it:
                path = entry.path
                try:
                    if entry.is_symlink():
                        os.unlink(path)
                    elif entry.is_dir(follow_symlinks=False):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
                except FileNotFoundError:
                    continue
                except Exception as e:
                    log_warning("清理目录项失败", extra={**(get_log_context() or {}), "path": path, "err": str(e)})
    except Exception as e:
        log_warning("扫描目录失败", extra={**(get_log_context() or {}), "path": dir_path, "err": str(e)})


def clean_tdata_dir(tdata_dir: str):
    clean_dir(tdata_dir)


def _rar_runtime_ok() -> bool:
    """检查 rarfile 运行时依赖（系统 unrar/unar）。不满足时给出提示但不中断 ZIP 流程。"""
    try:
        import rarfile
        return bool(rarfile.UNRAR_TOOL or rarfile.UNAR_TOOL)
    except Exception:
        return False


def _zip_meta_guard(zf: zipfile.ZipFile):
    infos = zf.infolist()
    if len(infos) > MAX_ZIP_ENTRIES:
        raise OpenTeleException("压缩包文件数过多，已拒绝。")
    total = 0
    for i in infos:
        if i.file_size > MAX_ENTRY_SIZE:
            raise OpenTeleException(f"发现超大文件：{i.filename}")
        total += i.file_size
        if total > MAX_TOTAL_SIZE:
            raise OpenTeleException("压缩包总体积过大，已拒绝。")


def _rar_meta_guard(rf):
    infos = rf.infolist()
    if len(infos) > MAX_ZIP_ENTRIES:
        raise OpenTeleException("压缩包文件数过多，已拒绝。")
    total = 0
    for i in infos:
        size = getattr(i, "file_size", 0)
        if size > MAX_ENTRY_SIZE:
            raise OpenTeleException(f"发现超大文件：{getattr(i, 'filename', '')}")
        total += size
        if total > MAX_TOTAL_SIZE:
            raise OpenTeleException("压缩包总体积过大，已拒绝。")


def _safe_extract_zip(zf: zipfile.ZipFile, dest_dir: str):
    """避免 Zip Slip 的安全解压实现。"""
    dest_dir = os.path.realpath(dest_dir)
    for member in zf.infolist():
        name = member.filename
        if name.startswith(("/", "\\")):
            continue
        target_path = os.path.realpath(os.path.join(dest_dir, name))
        if not target_path.startswith(dest_dir + os.sep):
            continue
        if member.is_dir():
            os.makedirs(target_path, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with zf.open(member) as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)


def _safe_extract_rar(rf, dest_dir: str):
    """避免路径穿越的 RAR 安全解压。"""
    dest_dir = os.path.realpath(dest_dir)
    for member in rf.infolist():
        name = getattr(member, "filename", "")
        if name.startswith(("/", "\\")):
            continue
        target_path = os.path.realpath(os.path.join(dest_dir, name))
        if not target_path.startswith(dest_dir + os.sep):
            continue
        if getattr(member, "is_dir", None) and member.is_dir():
            os.makedirs(target_path, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with rf.open(member) as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)


def extract_archive(archive_path: str, dest_dir: str) -> bool:
    """解压 zip/rar 到目标目录。失败抛异常（含格式校验与炸弹防护）。"""
    try:
        fn = archive_path.lower()
        if fn.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as z:
                _zip_meta_guard(z)
                _safe_extract_zip(z, dest_dir)
        elif fn.endswith(".rar"):
            try:
                import rarfile
            except ImportError:
                raise OpenTeleException("服务器未安装 rarfile，请先 `pip install rarfile`。")
            if not _rar_runtime_ok():
                raise OpenTeleException("服务器缺少解压 RAR 的系统依赖（unrar/unar）。")
            try:
                with rarfile.RarFile(archive_path, "r") as r:
                    _rar_meta_guard(r)
                    _safe_extract_rar(r, dest_dir)
            except rarfile.Error as e:
                log_error("压缩包格式错误", extra={**(get_log_context() or {}), "err": str(e)})
                raise TDataInvalidMagic("压缩包格式不正确或已损坏")
        else:
            raise OpenTeleException("只支持 ZIP 或 RAR")
        return True
    except zipfile.BadZipFile as e:
        log_error("压缩包格式错误", extra={**(get_log_context() or {}), "err": str(e)})
        raise TDataInvalidMagic("压缩包格式不正确或已损坏")
    except FileNotFoundError:
        log_error("文件未找到", extra={**(get_log_context() or {}), "file": archive_path})
        raise TFileNotFound(f"文件未找到: {archive_path}")
    except Exception as e:
        log_error("解压未知异常", extra={**(get_log_context() or {}), "err": str(e)})
        raise


def find_all_tdata_dirs(SESSION_DIR: str) -> List[str]:
    """递归查找所有包含 tdata 的目录，去重。"""
    results = set()
    SKIP_DIRS = {"__MACOSX", ".DS_Store", ".git", ".svn", ".hg"}
    for root, dirs, _files in os.walk(SESSION_DIR, topdown=True, followlinks=False):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        if "tdata" in dirs:
            results.add(os.path.join(root, "tdata"))
            try:
                dirs.remove("tdata")
            except ValueError:
                pass
    return sorted(results)


async def download_zip_with_progress(msg, save_path: str, progress_cb: Optional[Callable[[str], Awaitable[Any]]] = None):
    """下载 ZIP/RAR（仅维持 chat action）"""
    if not isinstance(msg.media, MessageMediaDocument):
        raise ValueError("不是有效媒体 ZIP/RAR 文件")

    parent = os.path.dirname(save_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    chat = getattr(msg, "chat_id", None) or getattr(msg, "peer_id", None)
    async with msg.client.action(chat, "document") as action:
        pc = getattr(action, "progress", None)
        try:
            await msg.download_media(file=save_path, progress_callback=pc)
        except TypeError:
            await msg.download_media(file=save_path)


# ===================== 在会话里按需询问 Passcode =====================
async def ask_passcode(
    conv: Conversation, user_id: int, label: str, timeout: int = 300
) -> str | None:
    """询问用户 tdata 的二次密码（可选）。返回明文或 None（超时/取消）。"""
    try:
        await conv.send_message(
            f"🔑 检测到 <b>{label}</b> 需要二次密码。\n"
            f"请在 <b>{timeout}s</b> 内回复 passcode。\n"
            f"（为安全起见，读取后将尝试撤回你的密码消息）",
            parse_mode="html",
        )
        resp = await conv.get_response(timeout=timeout)
        passwd = (getattr(resp, "message", "") or "").strip()
        try:
            await resp.delete()
        except Exception:
            pass
        return passwd or None
    except asyncio.CancelledError:
        try:
            await conv.send_message("🧹 上传流程已取消，跳过当前 tdata。")
        except Exception:
            pass
        return None
    except asyncio.TimeoutError:
        await conv.send_message("⌛ 等待 passcode 超时，已跳过该 tdata。")
        return None


# ===================== tdata -> session（加锁） =====================
@with_session_lock(default_session_lock_path)
async def tdata_to_session(
    user_id: int,
    tdata_dir: str,
    zip_name: str = "",
    platform: str = "android",
    passcode_getter: Optional[Callable[[], Awaitable[Optional[str]]]] = None,
    *,
    phone: Optional[str] = None,  # 用于锁（调用方传入 phone_guess）
) -> Optional[str]:
    """
    将 tdata 目录转为 Telethon session + 配套 json，并返回手机号(作为基名)。
    如遇到 tdata 受二次密码保护，且提供了 passcode_getter，则会向用户询问。
    """
    from core.device_factory import get_api_config, get_device_info

    session = None
    phone_guess = Path(tdata_dir).parent.name  # 目录名作为兜底手机号
    session_name = phone_guess
    session_path = get_phone_session(user_id, phone_guess)
    json_path = get_phone_json(user_id, phone_guess)

    context = {
        **(get_log_context() or {}),
        "zip_name": zip_name,
        "tdata_dir": tdata_dir,
        "session_path": session_path,
        "json_path": json_path,
    }
    log_info("📥 开始转换 tdata", extra=context)

    if not os.path.isdir(tdata_dir):
        log_warning("❌ tdata 目录不存在", extra=context)
        return None

    # ① 先尝试无密码加载
    tdesk = TDesktop(tdata_dir, passcode="")
    if not tdesk.isLoaded():
        # ② 需要密码时，存在回调则向用户询问一次
        if callable(passcode_getter):
            passwd = await passcode_getter()
            if not passwd:
                log_warning("⛔ 用户未提供 passcode，跳过该 tdata", extra=context)
                return None
            tdesk = TDesktop(tdata_dir, passcode=passwd)
        # ③ 仍失败则放弃
        if not tdesk.isLoaded():
            log_warning("❌ tdata 加载失败（空密码+回调后仍失败）", extra=context)
            return None

    device_info = get_device_info(user_id, session_name, platform)
    api = get_api_config(user_id, session_name, platform)

    try:
        session = await tdesk.ToTelethon(session=session_path, flag=UseCurrentSession)
        await session.connect()

        if not await session.is_user_authorized():
            await session.disconnect()
            if os.path.exists(session_path):
                os.remove(session_path)
            log_warning("⚠️ 会话未授权，删除 session 文件", extra=context)
            return None

        me = await session.get_me()
        if not me:
            await session.disconnect()
            if os.path.exists(session_path):
                os.remove(session_path)
            log_warning("❌ 无法获取账号信息", extra=context)
            return None

        first_name = getattr(me, "first_name", "") or ""
        last_name = getattr(me, "last_name", "") or ""
        username = getattr(me, "username", "") or ""
        phone_real = getattr(me, "phone", "") or phone_guess
        log_info("✅ 授权成功", extra={**context, "phone": phone_real, "username": username})

        # 若真实手机号与目录名不同，则重命名产物
        if phone_real != phone_guess:
            new_session_path = get_phone_session(user_id, phone_real)
            new_json_path = get_phone_json(user_id, phone_real)
            try:
                if os.path.exists(session_path):
                    os.makedirs(os.path.dirname(new_session_path), exist_ok=True)
                    shutil.move(session_path, new_session_path)
                session_path = new_session_path
                json_path = new_json_path
            except Exception as e:
                log_warning("重命名会话文件失败（继续使用原始名）", extra={**context, "err": str(e)})

        config = {
            "phone": phone_real,
            "api_id": getattr(api, "api_id", ""),
            "api_hash": getattr(api, "api_hash", ""),
            "device_model": getattr(device_info, "model", ""),
            "system_version": getattr(device_info, "version", ""),
            "app_version": getattr(device_info, "app_version", ""),
            "lang_code": getattr(api, "lang_code", "en-US"),
            "system_lang_code": getattr(api, "system_lang_code", "en-US"),
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
            "zip_name": zip_name,
            "platform": platform,
        }
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

        await session.disconnect()
        return phone_real

    except Exception as e:
        log_exception("❌ tdata 转换异常", exc=e, extra=context)
        try:
            if session and hasattr(session, "disconnect"):
                await session.disconnect()
        except Exception:
            pass
        # 清理半成品
        try:
            if os.path.exists(session_path):
                os.remove(session_path)
        except Exception:
            pass
        return None


# ===================== 批量转换：返回“成功手机号列表” =====================
async def convert_batch(
    user_id: int,
    tdata_dir: str,
    user_dir: str,
    conv: Conversation,
    zip_name: str = "",
    client_manager=None,
    passcode_getter_factory: Optional[Callable[[str], Awaitable[str | None]]] = None,
    *,
    chat_id: Optional[int] = None,
) -> List[str]:
    """
    批量将 tdata 目录转换为 Telethon session/json 并（可选）注册到 client_manager。
    返回：本次成功转换的手机号列表。
    """
    phones_ok: List[str] = []

    trace_id = generate_trace_id()
    set_log_context({"user_id": user_id, "trace_id": trace_id, "func": "convert_batch"})
    tdata_paths = find_all_tdata_dirs(tdata_dir)
    log_info("开始批量转换", extra={**(get_log_context() or {}), "tdata_paths": tdata_paths})

    for tdata_path in tdata_paths:
        if not os.path.isdir(tdata_path):
            continue

        phone_guess = Path(tdata_path).parent.name
        base_ctx = {
            **(get_log_context() or {}),
            "zip_name": zip_name,
            "tdata_path": tdata_path,
            "phone_guess": phone_guess,
        }

        try:
            getter = (lambda: passcode_getter_factory(phone_guess)) if callable(passcode_getter_factory) else None

            phone = await tdata_to_session(
                user_id,
                tdata_path,
                zip_name,
                platform="android",
                passcode_getter=getter,
                phone=phone_guess,
            )
            if not phone:
                log_warning("⚠️ 未能从 tdata 获取有效账号", extra=base_ctx)
                continue

            session_path = get_phone_session(user_id, phone)
            json_path = get_phone_json(user_id, phone)
            file_ctx = {**base_ctx, "phone": phone, "session_path": session_path, "json_path": json_path}

            if not (os.path.exists(session_path) and os.path.exists(json_path)):
                try:
                    if os.path.exists(session_path):
                        os.remove(session_path)
                    if os.path.exists(json_path):
                        os.remove(json_path)
                except Exception as e:
                    log_warning("二次清理失败", extra={**file_ctx, "err": str(e)})

                log_warning("会话/配置文件缺失，跳过该账号", extra=file_ctx)
                continue

            if client_manager is not None:
                try:
                    registered_ok = True
                    if hasattr(client_manager, "try_create_and_register"):
                        registered_ok = await client_manager.try_create_and_register(user_id, phone)
                    elif hasattr(client_manager, "create_and_register"):
                        registered_ok = await client_manager.create_and_register(user_id, phone)
                    elif hasattr(client_manager, "ensure_client"):
                        res = await client_manager.ensure_client(user_id, phone)
                        registered_ok = bool(res) if not isinstance(res, bool) else res
                    elif hasattr(client_manager, "register_session"):
                        registered_ok = await client_manager.register_session(user_id, phone, session_path)
                    else:
                        log_warning("client_manager 未提供显式注册方法，跳过注册步骤", extra=file_ctx)
                        registered_ok = True

                    if registered_ok:
                        log_debug("✅ 账号注册成功或已就绪", extra=file_ctx)
                    else:
                        log_warning("账号注册返回失败（会话已生成，照常计入成功）", extra=file_ctx)
                except Exception as e:
                    log_warning("注册阶段异常（不影响本账号转换）", extra={**file_ctx, "err": str(e)})
            else:
                log_warning("client_manager is None，跳过注册", extra=file_ctx)

            phones_ok.append(phone)

        except Exception as e:
            log_warning("⚠️ 跳过失败账号（转换阶段异常）", extra={**base_ctx, "err": str(e)})
            continue

    if chat_id is not None:
        _CONVERTED_BY_CHAT.setdefault(chat_id, set()).update(phones_ok)

    return phones_ok


# =============== “📥 上传账号”入口回调：进入上传流程 ===============
async def handle_upload_open(event: events.CallbackQuery.Event):
    extra_ctx = {
        "function": "handle_upload_open",
        "phase": "handler",
        "status": "start",
        "user_id": getattr(event, "sender_id", None),
        "chat_id": getattr(event, "chat_id", None),
    }
    log_info("进入上传入口回调", extra=extra_ctx)
    try:
        try:
            if hasattr(event, "answer"):
                await event.answer(cache_time=0)
        except Exception as e:
            log_warning("answer 回调失败（忽略）", extra={**extra_ctx, "error": str(e)})

        cancelled = cancel_active_conversation(event.chat_id)
        if cancelled:
            log_info("已取消同会话残留会话", extra=extra_ctx)

        await handle_upload_command(event)
        log_info("上传入口回调完成", extra={**extra_ctx, "status": "success"})
    except Exception as e:
        log_exception("上传入口回调异常", e, extra={**extra_ctx, "status": "error"})


# ===================== 交互入口 =====================
@super_command(trace_action="上传账号", white_only=True)
async def handle_upload_command(event):
    user_id = event.sender_id

    # ✅ 重置本次会话的“成功手机号集合”
    _CONVERTED_BY_CHAT[event.chat_id] = set()

    # 会话外的兜底回信
    async def _say(text: str, **kwargs):
        await BotUtils.safe_respond(event, event.client, text, **kwargs)

    # 目录初始化
    user_dir, zip_dir, tdata_dir = get_user_dirs(user_id)
    os.makedirs(user_dir, exist_ok=True)
    os.makedirs(zip_dir, exist_ok=True)
    os.makedirs(tdata_dir, exist_ok=True)

    client_manager = get_client_manager()
    inject_trace_context(func_name="handle_upload_command", user_id=user_id, chat_id=event.chat_id, command="/h", trace_name="上传账号")

    try:
        async with event.client.conversation(
            event.chat_id,
            timeout=RUNNER_TIMEOUT,
            exclusive=True,
        ) as conv:
            set_active_conversation(event.chat_id, conv)
            log_info("会话创建成功", extra={
                "function": "handle_upload_command",
                "module": "handlers.commands.upload_command",
                "chat_id": event.chat_id,
            })

            async def _conv_say(text: str, **kwargs):
                try:
                    return await conv.send_message(text, **kwargs)
                except Exception as e:
                    log_warning("conv.send_message 失败，回退 BotUtils.safe_respond", extra={"err": str(e)})
                    return await BotUtils.safe_respond(event, event.client, text, **kwargs)

            # 入口提示
            await _conv_say(
                "📥 发送 tdata 压缩包（ZIP/RAR），可连续发送多个；上传完毕请点击下方按钮：",
                buttons=_upload_markup(event.client),
            )

            await _notify_rar_runtime_if_needed(conv)

            MAX_ATTEMPTS = 50
            processed = 0

            for _ in range(MAX_ATTEMPTS):
                try:
                    try:
                        msg = await conv.get_response()
                    except ValueError:
                        log_warning(
                            "get_response 遭遇 No message was sent previously，补发提示并重试",
                            extra={"function": "handle_upload_command", "chat_id": event.chat_id},
                        )
                        await _conv_say(
                            "📥 请发送 ZIP 或 RAR 文件（或点击下方按钮完成/取消）。",
                            buttons=_upload_markup(event.client),
                        )
                        msg = await conv.get_response()

                except asyncio.CancelledError:
                    await _conv_say("⏹️ 已结束上传会话。")
                    break
                except asyncio.TimeoutError:
                    await _conv_say("⌛ 会话超时，自动退出上传模式。")
                    break

                # 非文件 → 提示后继续
                if not (getattr(msg, "file", None) and getattr(msg.file, "name", None)):
                    await _conv_say(
                        "ℹ️ 请发送 ZIP 或 RAR 文件（或点击下方按钮完成/取消）。",
                        buttons=_upload_markup(event.client),
                    )
                    continue

                fname_lower = (msg.file.name or "").lower()
                if not (fname_lower.endswith(".zip") or fname_lower.endswith(".rar")):
                    await _conv_say("⚠️ 只支持 ZIP 或 RAR 文件", buttons=_upload_markup(event.client))
                    continue

                safe_name = f"{msg.id}_{msg.file.name}"
                zip_path = os.path.join(zip_dir, safe_name)

                try:
                    await _conv_say(
                        f"📩 已收到文件：<code>{msg.file.name}</code>，开始下载与校验…",
                        parse_mode="html",
                        buttons=_upload_markup(event.client),
                    )

                    await download_zip_with_progress(msg, zip_path)
                    log_info("文件下载完成", extra={"zip_path": zip_path, "file": msg.file.name})

                    # 基本合法检
                    if fname_lower.endswith(".zip"):
                        from zipfile import is_zipfile

                        if not is_zipfile(zip_path):
                            await _conv_say("❌ 非法 ZIP 文件", buttons=_upload_markup(event.client))
                            continue
                    else:
                        try:
                            import rarfile
                        except ImportError:
                            await _conv_say("⚠️ 当前环境未安装 rarfile，请先 `pip install rarfile`。", buttons=_upload_markup(event.client))
                            continue
                        if not _rar_runtime_ok():
                            await _conv_say("⚠️ 服务器缺少解压 RAR 的系统依赖（unrar/unar）。", buttons=_upload_markup(event.client))
                            continue
                        try:
                            if not rarfile.is_rarfile(zip_path):
                                await _conv_say("❌ 非法 RAR 文件", buttons=_upload_markup(event.client))
                                continue
                        except Exception:
                            await _conv_say("⚠️ rarfile 检测异常，请确认系统 unrar/unar 是否安装。", buttons=_upload_markup(event.client))
                            continue

                    # 解压
                    clean_tdata_dir(tdata_dir)
                    try:
                        extract_archive(zip_path, tdata_dir)
                    except OpenTeleException as oe:
                        log_warning("解压失败", extra={"err": str(oe), "zip": msg.file.name})
                        await _conv_say(f"❌ 解压失败: {oe}", buttons=_upload_markup(event.client))
                        continue

                    await _conv_say("🚀 转换中…")
                    try:
                        phones_ok = await convert_batch(
                            user_id,
                            tdata_dir,
                            user_dir,
                            conv,
                            zip_name=msg.file.name,
                            client_manager=client_manager,
                            passcode_getter_factory=lambda phone_guess: ask_passcode(
                                conv, user_id, label=f"{phone_guess} | {msg.file.name}"
                            ),
                            chat_id=event.chat_id,
                        )
                    except TDataBadDecryptKey:
                        await _conv_say("❌ 该 tdata 受密码保护或加密，暂不支持自动转化。", buttons=_upload_markup(event.client))
                        continue
                    except TDataInvalidMagic:
                        await _conv_say("❌ tdata 文件格式错误或已损坏。", buttons=_upload_markup(event.client))
                        continue
                    except TelethonUnauthorized:
                        await _conv_say("❌ 会话未授权，转换失败。", buttons=_upload_markup(event.client))
                        continue
                    except Exception as e:
                        log_error("批量转换异常", extra={"err": str(e)})
                        await _conv_say(f"❌ 批量转换异常: {e}", buttons=_upload_markup(event.client))
                        continue

                    processed += len(phones_ok or [])
                    if not phones_ok:
                        await _conv_say("⚠️ 未成功转换任何账号", buttons=_upload_markup(event.client))
                    else:
                        try:
                            await _post_upload_summary(event, user_id, only_phones=phones_ok)
                        except Exception as e:
                            log_warning("中途汇总失败（忽略）", extra={"err": str(e)})
                        await _conv_say("可以继续发送压缩包，或点击下方按钮完成/取消。", buttons=_upload_markup(event.client))
                        log_info("转换完成", extra={"count": len(phones_ok), "processed": processed})

                except Exception as e:
                    log_error("上传流程异常", extra={"err": str(e), "zip": getattr(msg.file, "name", "unknown")})
                    await _conv_say(f"❌ 转换失败: {e}", buttons=_upload_markup(event.client))
                finally:
                    try:
                        if os.path.exists(zip_path):
                            os.remove(zip_path)
                    except Exception:
                        pass
            else:
                await _conv_say("⚠️ 已超出最大尝试次数，自动退出上传模式。")

    except AlreadyInConversationError:
        await _say("⚠️ 当前已有进行中的上传会话，请先完成或稍后再试。")
    except Exception as e:
        log_error("上传命令处理异常", extra={"err": str(e), "chat_id": event.chat_id})
        await _say(f"❌ 转换失败: {e}")
    finally:
        set_active_conversation(event.chat_id, None)
        log_info("会话结束（清理活动会话标记）", extra={"function": "handle_upload_command", "chat_id": event.chat_id})

    # ✅ 统一汇总：只展示本次会话累计成功的账号
    try:
        only = list(_CONVERTED_BY_CHAT.get(event.chat_id, set()))
        await _post_upload_summary(event, user_id, only_phones=only)
    finally:
        _CONVERTED_BY_CHAT.pop(event.chat_id, None)


# ===================== 汇总准备（兼容多版本 client_manager） =====================
async def _prepare_clients_for_summary(cm, user_id: int, validator=None, only_phones: Optional[Iterable[str]] = None):
    """
    统一准备可展示的 {phone: client|None} 映射，兼容旧版 client_manager：
    - 优先：prepare_clients_for_user()（如支持，带 phones_whitelist 子集）
    - 兼容：load_all_for_user() + get_clients_for_user()
    - 兜底：仅从文件系统列出手机号，client 置 None
    - 当 only_phones 提供时，只为这些手机号做最小准备，避免全量扫描与断连。
    """
    phones_list = list(map(str, only_phones)) if only_phones else None

    if hasattr(cm, "prepare_clients_for_user"):
        try:
            if phones_list:
                return await cm.prepare_clients_for_user(
                    user_id,
                    release_existing=False,
                    cleanup_journals=True,
                    validator=validator,
                    set_cooldown=False,
                    log_prefix="上传汇总(子集)",
                    phones_whitelist=phones_list,
                )
            else:
                return await cm.prepare_clients_for_user(
                    user_id,
                    release_existing=False,
                    cleanup_journals=True,
                    validator=validator,
                    set_cooldown=False,
                    log_prefix="上传汇总",
                )
        except TypeError:
            pass
        except Exception:
            pass

    if phones_list:
        out = {}
        for ph in phones_list:
            cli = None
            try:
                if hasattr(cm, "get_client_or_connect"):
                    cli = await cm.get_client_or_connect(user_id, ph)
                else:
                    if hasattr(cm, "get_client"):
                        cli = cm.get_client(ph, user_id=user_id)
            except Exception:
                cli = None
            out[ph] = cli
        return out

    try:
        if hasattr(cm, "load_all_for_user"):
            await cm.load_all_for_user(user_id, concurrency=4)
    except Exception:
        pass

    clients = {}
    try:
        if hasattr(cm, "get_clients_for_user"):
            clients = cm.get_clients_for_user(user_id) or {}
    except Exception:
        clients = {}

    if not clients:
        try:
            if hasattr(cm, "list_user_phones"):
                pairs = cm.list_user_phones(user_id) or []
                return {p: None for p, _sp in pairs}
        except Exception:
            return {}

    if validator and callable(validator):
        try:
            result = await validator(user_id, clients)
            if isinstance(result, dict) and "ok" in result:
                return result.get("ok", {}) or {}
            elif isinstance(result, dict):
                return result
        except Exception:
            pass

    return clients


# ===================== 上传后汇总 =====================
async def _post_upload_summary(event, user_id: int, only_phones: Optional[Iterable[str]] = None):
    """
    汇总当前可用账号（根据文件对），给用户一个完成提示。
    若提供 only_phones，则只展示这些手机号（即本批/本次会话成功账号）。
    """
    from unified.context import get_client_manager, get_scheduler

    base_ctx = {
        "function": "_post_upload_summary",
        "phase": "summary",
        "user_id": user_id,
        "chat_id": getattr(event, "chat_id", None),
        "status": "start",
    }
    log_info("开始汇总上传结果", extra=base_ctx)

    cm = get_client_manager()
    from unified.selection import unified_prepare_and_select_clients

    validator = None
    try:
        scheduler = get_scheduler()
        validator = getattr(scheduler, "validator", None)
    except Exception as e:
        log_debug("get_scheduler 失败或无 validator（忽略）", extra={**base_ctx, "error": str(e)})

    async def _send_long(text: str, chunk: int = 3500):
        send_ctx = {**base_ctx, "function": "_send_long"}
        if len(text) <= chunk:
            log_debug("发送汇总文本（单片）", extra={**send_ctx, "length": len(text)})
            await BotUtils.safe_respond(event, event.client, text, link_preview=False)
            return
        i = 0
        part = 0
        while i < len(text):
            part += 1
            piece = text[i : i + chunk]
            log_debug("发送汇总文本分片", extra={**send_ctx, "part": part, "length": len(piece)})
            await BotUtils.safe_respond(event, event.client, piece, link_preview=False)
            i += chunk

    try:
        clients = await _prepare_clients_for_summary(cm, user_id, validator, only_phones=only_phones)
        phone_to_client = dict(clients or {})

        if only_phones is not None:
            only_set = {str(p) for p in only_phones}
            phone_to_client = {p: phone_to_client.get(p) for p in only_set}
            phones = sorted(only_set)
            log_info("按本次成功账号过滤汇总", extra={**base_ctx, "phones_filtered": len(phones)})
        else:
            phones = list(phone_to_client.keys())

        log_info("汇总客户端准备完成", extra={**base_ctx, "phones": len(phones)})

        if phones:
            MAX_SHOW = 50
            shown = phones[:MAX_SHOW]

            async def _line_for(phone: str):
                li_ctx = {**base_ctx, "phone": phone}
                cli = phone_to_client.get(phone)
                if not cli:
                    return f"• <code>{phone}</code>"
                try:
                    me_cached = getattr(cli, "_cached_me", None)
                    if me_cached is None:
                        me = await cli.get_me()
                        try:
                            setattr(cli, "_cached_me", me)
                        except Exception:
                            pass
                    else:
                        me = me_cached[1] if isinstance(me_cached, tuple) else me_cached

                    username = (getattr(me, "username", "") or "").strip()
                    first = (getattr(me, "first_name", "") or "").strip()
                    last = (getattr(me, "last_name", "") or "").strip()
                    name = (f"{first} {last}").strip()
                    uname_disp = f"@{username}" if username else ""
                    tail = " ".join([x for x in [uname_disp, name] if x]).strip()
                    return f"• <code>{phone}</code>" + (f" {tail}" if tail else "")
                except Exception as e:
                    log_debug("构造汇总行失败（忽略）", extra={**li_ctx, "error": str(e)})
                    return f"• <code>{phone}</code>"

            lines = await asyncio.gather(*[_line_for(p) for p in shown])
            more = f"\n……共 {len(phones)} 个账号" if len(phones) > MAX_SHOW else ""
            title = "🎉 本次转换成功的账号：" if only_phones is not None else "🎉 上传完成可用账号："
            text = title + "\n" + "\n".join(lines) + more
            log_info("发送汇总消息", extra={**base_ctx, "shown": len(shown), "total": len(phones)})
            await _send_long(text)

            if only_phones is not None:
                try:
                    sendable, stats, _excluded = await unified_prepare_and_select_clients(
                        user_id,
                        phones_whitelist=phones,
                        intent="ASSIGN",
                        validator=validator,
                    )
                    ok_n = len(sendable or {})
                    cd_n = int(stats.get("cooldown_hit", 0))
                    ex_n = int(stats.get("excluded", 0))
                    total_n = int(stats.get("total", len(phones)))
                    preview = (
                        f"\n\n<b>📊 可分配预览（ASSIGN）</b>\n"
                        f"• 本次成功：{total_n}\n"
                        f"• 可分配就绪：{ok_n}\n"
                        f"• 冷却命中：{cd_n}\n"
                        f"• 其它排除：{ex_n}\n"
                    )
                    await BotUtils.safe_respond(event, event.client, preview, parse_mode="html", link_preview=False)
                except Exception as e:
                    log_warning("ASSIGN 预览失败（忽略）", extra={**base_ctx, "err": str(e)})
        else:
            log_info("无可用账号文件", extra=base_ctx)
            await BotUtils.safe_respond(event, event.client, "⚠️ 暂无可用账号文件，请确认上传是否成功。")

        log_info("汇总完成", extra={**base_ctx, "status": "success"})
    except Exception as e:
        log_warning("汇总可用账号失败", extra={**base_ctx, "status": "error", "err": str(e)})
        await BotUtils.safe_respond(event, event.client, "⚠️ 汇总账号信息失败，但不影响后续操作。")


# ===================== 按钮回调（不做汇总） =====================
async def handle_upload_done(event: events.CallbackQuery.Event):
    base_ctx = {
        "function": "handle_upload_done",
        "phase": "handler",
        "user_id": getattr(event, "sender_id", None),
        "chat_id": getattr(event, "chat_id", None),
        "status": "start",
    }
    log_info("收到完成上传按钮", extra=base_ctx)
    try:
        try:
            await event.answer("✅ 已完成上传", alert=False)
        except Exception as e:
            log_debug("answer 失败（忽略）", extra={**base_ctx, "error": str(e)})
        cancelled = cancel_active_conversation(event.chat_id)
        if cancelled:
            log_info("已取消活动会话", extra=base_ctx)
        await BotUtils.safe_respond(event, event.client, "✅ 已完成上传。", buttons=None)
        log_info("完成上传按钮处理结束", extra={**base_ctx, "status": "success"})
    except Exception as e:
        log_exception("完成上传按钮处理异常", e, extra={**base_ctx, "status": "error"})
        await BotUtils.safe_respond(event, event.client, "⚠️ 操作完成，但出现了轻微异常。")


async def handle_upload_cancel(event: events.CallbackQuery.Event):
    base_ctx = {
        "function": "handle_upload_cancel",
        "phase": "handler",
        "user_id": getattr(event, "sender_id", None),
        "chat_id": getattr(event, "chat_id", None),
        "status": "start",
    }
    log_info("收到取消上传按钮", extra=base_ctx)
    try:
        try:
            await event.answer("❌ 已取消上传", alert=False)
        except Exception as e:
            log_debug("answer 失败（忽略）", extra={**base_ctx, "error": str(e)})
        cancelled = cancel_active_conversation(event.chat_id)
        if cancelled:
            log_info("已取消活动会话", extra=base_ctx)
        await BotUtils.safe_respond(event, event.client, "❌ 上传已取消。如需重新开始，请发送 /h")
        log_info("取消上传按钮处理结束", extra={**base_ctx, "status": "success"})
    except Exception as e:
        log_exception("取消上传按钮处理异常", e, extra={**base_ctx, "status": "error"})


# ===================== 注册 =====================

def register_commands():
    from router.command_router import main_router

    main_router.command("/h", trace_name="上传账号", white_only=True)(handle_upload_command)
    return True


def register_callbacks(bot=None):
    """使用 safe_add_event_handler 做幂等回调注册，避免重复挂载。"""
    from core.registry_guard import safe_add_event_handler
    from unified.context import get_bot

    bot = bot or get_bot()
    if not bot:
        log_warning("⚠️ register_callbacks: bot 未注入，回调未注册")
        return False

    safe_add_event_handler(
        bot,
        handle_upload_open,
        events.CallbackQuery(data=UPLOAD_OPEN),
        tag="upload:open",
    )
    safe_add_event_handler(
        bot,
        handle_upload_done,
        events.CallbackQuery(data=UPLOAD_DONE),
        tag="upload:done",
    )
    safe_add_event_handler(
        bot,
        handle_upload_cancel,
        events.CallbackQuery(data=UPLOAD_CANCEL),
        tag="upload:cancel",
    )

    return True
