# core/redis_fsm.py 
# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from redis.asyncio import Redis
from typess.ttl_types import TTL
from core.defaults.bot_utils import BotUtils
from typess.fsm_keys import USERNAME, WHITELIST, INTERVAL, MESSAGE, GROUP_LIST, STATUS, USERNAME_LOG, FSMStage
from typess.link_types import ParsedLink
from typess.message_enums import MessageContent, MessageType, SendTask, TaskStatus
from unified.config import get_admin_ids
from unified.logger import log_debug, log_error, log_exception, log_info, log_warning
from unified.trace_context import generate_trace_id, get_log_context, set_log_context

# ========================== Redis 键常量 ==========================
WHITELIST_KEY = "global:whitelist"
ACTIVE_TASKS_KEY = "fsm:active_tasks"
STATUS_KEY_FMT = "status:{user_id}"
INTERVAL_KEY_FMT = "interval:{user_id}"
STATE_KEY_FMT = "fsm:{user_id}"

def _cooldown_key(phone: str) -> str:
    return f"cooldown:{phone}"

def _flood_key(phone: str) -> str:
    return f"floodwait:{phone}"

def _slowmode_key(user_id: int, chat_key: str) -> str:
    return f"tg:slowmode:{user_id}:{chat_key}"

def _payment_block_key(user_id: int, chat_key: str) -> str:
    return f"tg:blocked:payment:{user_id}:{chat_key}"

def _username_invalid_key(username: str) -> str:
    return f"tg:username:invalid:{username}"

def _sendlock_key(user_id: int, chat_key: str) -> str:
    return f"tg:sendlock:{user_id}:{chat_key}"

def _b2s(v) -> str:
    return v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else str(v or "")

__all__ = [
    "WHITELIST_KEY",
    "ACTIVE_TASKS_KEY",
    "STATUS_KEY_FMT",
    "INTERVAL_KEY_FMT",
    "STATE_KEY_FMT",
    "RedisCommandFSM",
]


class RedisCommandFSM:
    """
    Redis 驱动的发送任务 FSM：
    - 维护 SendTask（JSON 序列化）
    - 状态/间隔镜像键（兼容旧 UI）
    - 慢速模式、支付阻断、同群锁等运行时边界控制
    - 元数据/任意 KV 支持（模板映射、emoji_ids 等）
    - 与调度/执行/健康模块的接口完全对齐
    """

    def __init__(self, redis_client: Redis):
        self.redis: Redis = redis_client
        self._locks: Dict[int, asyncio.Lock] = {}

    # ---------- 基础键名 ----------

    def _key(self, user_id: int) -> str:
        return STATE_KEY_FMT.format(user_id=user_id)

    def _status_key(self, user_id: int) -> str:
        return STATUS_KEY_FMT.format(user_id=user_id)

    def _interval_key(self, user_id: int) -> str:
        return INTERVAL_KEY_FMT.format(user_id=user_id)

    def _phone_key(self, phone: str) -> str:
        return f"phone:{phone}"

    def _user_info_key(self, user_id: int) -> str:
        return f"user:{user_id}:info"

    def _kv_key(self, user_id: int, field: str) -> str:
        return f"{self._key(user_id)}:kv:{field}"

    def _meta_key(self, user_id: int) -> str:
        return f"{self._key(user_id)}:kv:metadata"

    def _get_lock(self, user_id: int) -> asyncio.Lock:
        if user_id not in self._locks:
            self._locks[user_id] = asyncio.Lock()
        return self._locks[user_id]


    # ---------- 默认任务 ----------
    def _default_task(self, user_id: int) -> SendTask:
        return SendTask(
            task_id=str(uuid.uuid4()),
            user_id=user_id,
            group_list=[],
            message=MessageContent.build_text(""),
            interval=60,  # 秒
            status=TaskStatus.PENDING,
            stats={},
            whitelist=[],
            username="",
        )

    # ---------- 状态存取（SendTask） ----------
    async def get_state(self, user_id: int) -> SendTask:
        raw = await self.redis.get(self._key(user_id))
        if not raw:
            return self._default_task(user_id)
        try:
            return SendTask.from_json(_b2s(raw))
        except Exception as e:
            log_exception("状态反序列化失败，返回默认任务", exc=e, extra={"user_id": user_id})
            return self._default_task(user_id)

    async def set_state(self, user_id: int, task: SendTask) -> None:
        # 更稳健地注入 trace_id
        try:
            trace = task.get_trace() if hasattr(task, "get_trace") else {}
        except Exception:
            trace = {}
        trace_id = (trace or {}).get("trace_id") or generate_trace_id()

        set_log_context({
            "user_id": user_id,
            "task_id": getattr(task, "task_id", None),
            "trace_id": trace_id,
        })

        try:
            await self.redis.set(self._key(user_id), task.to_json())
            log_debug("✅ 状态已保存", extra={
                "user_id": user_id,
                "trace_id": trace_id
            })
        except Exception as e:
            log_exception("❌ 状态保存失败", exc=e, extra={
                "user_id": user_id,
                "task_id": getattr(task, "task_id", None),
                "trace_id": trace_id,
            })

    async def set_data(self, user_id: int, key: str, value: Any) -> None:
        async with self._get_lock(user_id):
            task = await self.get_state(user_id)
            setattr(task, key, value)
            await self.set_state(user_id, task)

    async def reset(self, user_id: int, event=None) -> None:
        try:
            # 主状态
            await self.redis.delete(self._key(user_id))
            # 关联状态与镜像
            try:
                await self.redis.delete(self._status_key(user_id))
                await self.redis.delete(self._interval_key(user_id))
            except Exception:
                pass
            # metadata
            try:
                await self.redis.delete(self._meta_key(user_id))
            except Exception:
                pass
            # 清理该用户 KV
            try:
                pattern = f"{self._key(user_id)}:kv:*"
                async for k in self.redis.scan_iter(match=pattern, count=200):
                    try:
                        await self.redis.delete(k)
                    except Exception:
                        pass
            except Exception:
                pass

            log_debug("✅ 状态已重置", extra={"user_id": user_id, **(get_log_context() or {})})
            if event:
                await BotUtils.safe_respond(event, event.client, "🧹 已重置任务状态")

            # 同步阶段
            try:
                await self.auto_update_stage(user_id)
            except Exception:
                pass
        except Exception as e:
            log_exception(
                "❌ 状态重置失败",
                exc=e,
                extra={"user_id": user_id, **(get_log_context() or {})},
            )

    async def to_task(self, user_id: int) -> SendTask:
        return await self.get_state(user_id)

    # ---------- 通用字段读取 ----------
    async def get_data(self, user_id: int, key: str) -> Optional[Any]:
        task = await self.get_state(user_id)
        return getattr(task, key, None)

    # ---------- 用户信息/映射 ----------
    async def register_user_info(self, user_id: int, phone: str, name: str = "", username: str = "") -> None:
        await self.redis.set(self._phone_key(phone), user_id)
        data = {"name": name, USERNAME: username, "phone": phone}
        await self.redis.set(self._user_info_key(user_id), json.dumps(data, ensure_ascii=False))
        log_debug("✅ 用户信息已注册", extra={"user_id": user_id, "phone": phone, "username": username})

    async def index_user_phone(self, phone: str, user_id: int) -> None:
        await self.redis.set(f"phone:{phone}", user_id)
        await self.redis.sadd(f"phone:{user_id}", phone)

    async def get_user_id(self, phone: str) -> Optional[int]:
        raw = await self.redis.get(self._phone_key(phone))
        return int(_b2s(raw)) if raw else None

    async def get_user_info(self, phone: str) -> Optional[dict]:
        user_id = await self.get_user_id(phone)
        if not user_id:
            return None
        raw = await self.redis.get(self._user_info_key(user_id))
        return json.loads(_b2s(raw)) if raw else None

    async def get_phone_by_user_id(self, user_id: int) -> Optional[str]:
        raw = await self.redis.get(self._user_info_key(user_id))
        if not raw:
            return None
        try:
            data = json.loads(_b2s(raw))
            return data.get("phone")
        except Exception:
            return None

    async def unregister_user(self, user_id: int) -> None:
        info = await self.redis.get(self._user_info_key(user_id))
        if info:
            try:
                data = json.loads(_b2s(info))
                phone = data.get("phone")
                if phone:
                    await self.redis.delete(self._phone_key(phone))
            except Exception:
                pass
        await self.redis.delete(self._user_info_key(user_id))
        log_debug("🗑️ 用户映射已清除", extra={"user_id": user_id})

    # ================== 白名单逻辑 ==================
    async def add_to_whitelist(self, operator_id: int, target_id: int, username: Optional[str] = None) -> bool:
        """
        添加用户到全局白名单。
        统一以 str(target_id) 存储，避免 sadd/sismember 类型不一致。
        """
        added = await self.redis.sadd(WHITELIST_KEY, str(target_id))
        if added:
            log_info(
                "✅ 添加到白名单",
                extra={"operator": operator_id, "target": target_id, **(get_log_context() or {})},
            )
            if username:
                try:
                    await self.set_data(target_id, USERNAME, username)
                except Exception as e:
                    log_exception(
                        "写入白名单用户名失败（不影响主流程）",
                        exc=e,
                        extra={"target": target_id, USERNAME_LOG: username},
                    )
            return True
        log_debug("⚠️ 白名单已存在", extra={"target": target_id, **(get_log_context() or {})})
        return False

    async def remove_from_whitelist(self, operator_id: int, target_id: int) -> bool:
        """
        从全局白名单移除。
        """
        removed = await self.redis.srem(WHITELIST_KEY, str(target_id))
        if removed:
            log_info("🗑️ 从白名单移除", extra={"operator": operator_id, "target": target_id, **(get_log_context() or {})})
        else:
            log_debug("⚠️ 从白名单移除：不存在", extra={"target": target_id, **(get_log_context() or {})})
        return bool(removed)

    async def is_in_whitelist(self, owner_id: int, target_id: int) -> bool:
        """
        判断用户是否在白名单：
        - 管理员始终视为白名单
        - Redis 集合统一存储 str(target_id)
        """
        try:
            if int(target_id) in (get_admin_ids() or []):
                return True
        except Exception:
            pass
        try:
            ok = await self.redis.sismember(WHITELIST_KEY, str(target_id))
            return bool(ok)
        except Exception as e:
            log_exception("❌ is_in_whitelist 检查失败", exc=e, extra={"target": target_id})
            return False


    async def get_whitelist(self, _: int = 0) -> List[int]:
        ids = await self.redis.smembers(WHITELIST_KEY)
        out: List[int] = []
        for i in ids:
            try:
                out.append(int(_b2s(i)))
            except Exception:
                continue
        return sorted(out)

    async def get_whitelist_all(self) -> List[int]:
        return await self.get_whitelist(0)


    async def is_admin(self, sender_id: int) -> bool:
        try:
            return sender_id in (get_admin_ids() or [])
        except Exception:
            return False

    # ---------- 状态/间隔 ----------
    async def _set_status_and_mirror(self, user_id: int, status: TaskStatus | str) -> None:
        """
        统一写入：SendTask.status 与独立镜像键 status:{uid}
        """
        if isinstance(status, TaskStatus):
            status_val = status
        else:
            try:
                status_val = TaskStatus(status)
            except Exception:
                status_val = TaskStatus.PENDING
        await self.set_data(user_id, STATUS, status_val)
        try:
            await self.redis.set(self._status_key(user_id), status_val.value)
        except Exception as e:
            log_exception("独立状态写入失败", exc=e, extra={"user_id": user_id, "status": status_val.value})

    async def update_status(self, user_id: int, status: str | TaskStatus) -> None:
        await self._set_status_and_mirror(user_id, status)

    async def get_status_text(self, user_id: int) -> Optional[str]:
        s = await self.get_task_status(user_id)
        return s.value if isinstance(s, TaskStatus) else None

    async def get_task_status(self, user_id: int) -> Optional[TaskStatus]:
        try:
            raw = await self.redis.get(self._status_key(user_id))
            if raw:
                return TaskStatus(_b2s(raw))
        except Exception:
            pass
        task = await self.get_state(user_id)
        try:
            return TaskStatus(task.status) if isinstance(task.status, str) else task.status
        except Exception:
            return None

    async def pause_task(self, user_id: int) -> None:
        await self._set_status_and_mirror(user_id, TaskStatus.PAUSED)

    async def resume_task(self, user_id: int) -> None:
        """将状态恢复为 PENDING，交由调度器 start_user_task() 拉起后置为 RUNNING。"""
        await self._set_status_and_mirror(user_id, TaskStatus.PENDING)

    async def stop_task(self, user_id: int) -> None:
        await self._set_status_and_mirror(user_id, TaskStatus.STOPPED)

    async def set_interval(self, user_id: int, minutes: int, event=None) -> None:
        """
        设置每轮间隔：
        - 外部以“分钟”为单位传入
        - 内部以“秒”存储 SendTask.interval
        - 另存独立 interval:{user_id} （分钟）以兼容旧 UI
        """
        minutes = max(1, int(minutes))
        await self.set_data(user_id, INTERVAL, minutes * 60)
        try:
            await self.redis.set(self._interval_key(user_id), str(minutes))
        except Exception:
            pass
        log_debug(
            "⏱️ 设置群发间隔(分钟)",
            extra={"user_id": user_id, "interval": minutes, **(get_log_context() or {})},
        )
        if event:
            await BotUtils.safe_respond(event, event.client, f"⏱️ 每轮间隔设置为 <b>{minutes}</b> 分钟")

        try:
            await self.auto_update_stage(user_id)
        except Exception:
            pass

    async def get_interval(self, user_id: int) -> int:
        """
        获取每轮间隔（秒）：
        - 优先读 SendTask.interval（秒）
        - 如无则读独立键（分钟）并换算为秒
        - 最后兜底 60 秒
        """
        try:
            task = await self.get_state(user_id)
            iv = int(getattr(task, "interval", 0) or 0)
            if iv > 0:
                return iv
        except Exception:
            pass
        val = await self.redis.get(self._interval_key(user_id))
        try:
            mins = int(_b2s(val)) if val else 1
            return max(1, mins) * 60
        except Exception:
            return 60

    # ================== Slowmode / Payment / UsernameInvalid / 同群锁 ==================
    async def set_slowmode(self, user_id: int, chat_key: str, seconds: int, phone: str = "") -> None:
        seconds = max(1, int(seconds))
        payload = {"until_ts": int(time.time()) + seconds, "seconds": seconds, "set_by_phone": phone}
        # 额外 padding 改为 TTL.SLOWMODE_PAD_SECONDS
        await self.redis.setex(_slowmode_key(user_id, chat_key), seconds + TTL.SLOWMODE_PAD_SECONDS,
                           json.dumps(payload, ensure_ascii=False))
    async def in_slowmode(self, user_id: int, chat_key: str) -> bool:
        return bool(await self.redis.exists(_slowmode_key(user_id, chat_key)))

    async def mark_payment_required(self, user_id: int, chat_key: str) -> None:
        payload = {"reason": "payment_required", "first_seen_ts": int(time.time())}
        await self.redis.setex(_payment_block_key(user_id, chat_key), TTL.PAYMENT_BLOCK_SECONDS, json.dumps(payload))

    async def mark_username_invalid(self, username: str) -> None:
        payload = {"reason": "username_not_occupied", "first_seen_ts": int(time.time())}
        await self.redis.setex(_username_invalid_key(username), TTL.USERNAME_INVALID_SECONDS, json.dumps(payload, ensure_ascii=False))
        
    async def is_payment_blocked(self, user_id: int, chat_key: str) -> bool:
        return bool(await self.redis.exists(_payment_block_key(user_id, chat_key)))


    async def is_username_invalid(self, username: str) -> bool:
        return bool(await self.redis.exists(_username_invalid_key(username)))

    async def acquire_send_lock(self, user_id: int, chat_key: str, phone: str, ttl: int | None = None) -> bool:
        """
        分布式同群锁，TTL 统一：默认 TTL.SENDLOCK_SECONDS，最小 60s；允许外部显式传入覆盖
        """
        effective_ttl = max(60, int(ttl or TTL.SENDLOCK_SECONDS))
        return bool(await self.redis.set(_sendlock_key(user_id, chat_key), phone, nx=True, ex=effective_ttl))

    async def release_send_lock(self, user_id: int, chat_key: str, phone: Optional[str] = None) -> None:
        key = _sendlock_key(user_id, chat_key)
        if not phone:
            try:
                await self.redis.delete(key)
            except Exception:
                pass
            return
        try:
            script = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            """
            await self.redis.eval(script, 1, key, phone)
        except Exception:
            pass

    # ---------- 群组/消息字段 ----------
    async def set_groups(self, user_id: int, groups: List[str], event=None) -> None:
        await self.set_data(user_id, "group_list", groups)
        log_debug(
            "📥 群组链接已更新",
            extra={"user_id": user_id, "count": len(groups or []), **(get_log_context() or {})},
        )
        if event:
            await BotUtils.safe_respond(event, event.client, f"📥 设置了 <b>{len(groups)}</b> 个群组")
        try:
            await self.auto_update_stage(user_id)
        except Exception:
            pass

    async def get_parsed_groups(self, user_id: int) -> List[ParsedLink]:
        try:
            raw_list = await self.get_data(user_id, GROUP_LIST)
            if not raw_list or not isinstance(raw_list, list):
                return []
            links: List[ParsedLink] = []
            for s in raw_list:
                try:
                    links.append(ParsedLink.from_storage(str(s)))
                except Exception:
                    continue
            return links
        except Exception as e:
            log_exception("❌ 获取群组失败", exc=e, extra={"user_id": user_id})
            return []

    async def reset_groups(self, user_id: int, event=None) -> None:
        task = await self.get_state(user_id)
        task.group_list = []
        await self.set_state(user_id, task)
        log_debug("✅ 群组链接清除成功", extra={"user_id": user_id, **(get_log_context() or {})})
        if event:
            await BotUtils.safe_respond(event, event.client, "🧹 群组链接已清除")
        try:
            await self.auto_update_stage(user_id)
        except Exception as e:
            log_exception("❌ 自动更新阶段失败", exc=e, extra={"user_id": user_id})

    async def set_message_content(self, user_id: int, content: MessageContent, event=None) -> None:
        """保存消息内容 + 衍生 metadata（emoji_ids）+ 推进阶段。"""
        await self.set_data(user_id, MESSAGE, content)
        log_debug("✉️ 消息内容已设置", extra={"user_id": user_id, **(get_log_context() or {})})

        # 写入 emoji_ids
        try:
            emoji_ids: List[int] = []
            if getattr(content, "emoji_ids", None):
                emoji_ids = [int(i) for i in content.emoji_ids if i]
            else:
                from telethon.tl.types import MessageEntityCustomEmoji
                for ent in getattr(content, "entities", []) or []:
                    try:
                        if isinstance(ent, MessageEntityCustomEmoji):
                            did = int(getattr(ent, "document_id", 0) or 0)
                            if did:
                                emoji_ids.append(did)
                    except Exception:
                        continue
                # 去重保持顺序
                seen: Dict[int, None] = {}
                emoji_ids = [x for x in emoji_ids if not (x in seen or seen.setdefault(x, None) is not None)]
            await self.set_metadata(user_id, {"emoji_ids": emoji_ids})
        except Exception as e:
            log_exception(
                "写入 emoji_ids metadata 失败（不影响主流程）",
                exc=e,
                extra={"user_id": user_id, **(get_log_context() or {})},
            )

        if event:
            await BotUtils.safe_respond(event, event.client, "✉️ 消息内容设置成功")

        try:
            await self.auto_update_stage(user_id)
        except Exception:
            pass

    # --- 模板映射：顺序池与命名映射 ---
    async def set_template_maps(
        self,
        user_id: int,
        *,
        seq_ids: Optional[List[int]] = None,
        name_map: Optional[Dict[str, int]] = None,
        merge: bool = True,
    ) -> None:
        data: Dict[str, Any] = {}
        if seq_ids is not None:
            data["emoji_seq_ids"] = [int(i) for i in seq_ids if str(i).isdigit()]
        if name_map is not None:
            clean_map: Dict[str, int] = {}
            for k, v in (name_map or {}).items():
                try:
                    clean_map[str(k)] = int(v)
                except Exception:
                    continue
            data["emoji_name_map"] = clean_map
        if data:
            await self.set_metadata(user_id, data, merge=merge)

    async def get_template_maps(self, user_id: int) -> Dict[str, Any]:
        meta = await self.get_metadata(user_id)
        return {
            "emoji_seq_ids": list(meta.get("emoji_seq_ids") or []),
            "emoji_name_map": dict(meta.get("emoji_name_map") or {}),
        }

    async def clear_template_maps(self, user_id: int) -> None:
        meta = await self.get_metadata(user_id)
        meta.pop("emoji_seq_ids", None)
        meta.pop("emoji_name_map", None)
        await self.redis.set(self._meta_key(user_id), json.dumps(meta, ensure_ascii=False))
        await self.del_kv(user_id, "emoji_seq_ids")
        await self.del_kv(user_id, "emoji_name_map")



    # ================== 阶段推进 / 就绪检测 ==================
    def _is_message_ready(self, msg: Any) -> bool:
        if msg is None:
            return False
        try:
            is_ready = getattr(msg, "is_ready", None)
            if callable(is_ready):
                ok = is_ready()
                if isinstance(ok, bool):
                    return ok
        except Exception:
            pass

        try:
            t_ = getattr(msg, "type", None)
            tname = t_.value if isinstance(t_, MessageType) else (str(t_) if t_ else "")
            tname = (tname or "").lower()
            if tname == "text":
                return bool((getattr(msg, "content", None) or "").strip())
            if tname == "media":
                return bool(getattr(msg, "media", None))
            if tname == "forward":
                return bool(getattr(msg, "forward_peer", None) and getattr(msg, "forward_id", None))
            if tname == "album":
                mg = getattr(msg, "media_group", None)
                return bool(mg and isinstance(mg, (list, tuple)) and len(mg) > 0)
        except Exception:
            return False
        return False

    async def auto_update_stage(self, user_id: int) -> None:
        task = await self.get_state(user_id)
        interval_ready = int(getattr(task, "interval", 0) or 0) > 0
        message_ready = self._is_message_ready(task.message) if getattr(task, "message", None) else False
        group_ready = bool(getattr(task, "group_list", []) or [])

        old_stage = getattr(task, "stage", FSMStage.INIT)
        if interval_ready and message_ready and group_ready:
            task.stage = FSMStage.READY_TO_RUN
        elif interval_ready:
            task.stage = FSMStage.INTERVAL_READY
        elif message_ready:
            task.stage = FSMStage.MESSAGE_READY
        elif group_ready:
            task.stage = FSMStage.GROUP_READY
        else:
            task.stage = FSMStage.INIT

        if task.stage != old_stage:
            await self.set_state(user_id, task)
            log_debug(
                "🚦 阶段更新",
                extra={"user_id": user_id, "stage": str(task.stage), **(get_log_context() or {})},
            )

    # ================== 冷却管理 ==================
    async def set_cooldown(self, phone: str, seconds: int) -> None:
        seconds = max(1, int(seconds))
        await self.redis.setex(_cooldown_key(phone), seconds, "1")
        await self.redis.setex(_flood_key(phone), seconds, "1")

    async def clear_cooldown(self, phone: str) -> None:
        try:
            await self.redis.delete(_cooldown_key(phone))
            await self.redis.delete(_flood_key(phone))
            log_debug("🧹 冷却已清除", extra={"phone": phone, **(get_log_context() or {})})
        except Exception as e:
            log_error("❌ 冷却清除失败", extra={"phone": phone, "err": str(e), **(get_log_context() or {})})

    # ================== 活动任务管理 ==================
    
    async def add_active_task(self, user_id: int) -> bool:
        """
        将用户加入活动任务集合；返回 True 表示本实例成功占有。
        """
        try:
            return bool(await self.redis.sadd(ACTIVE_TASKS_KEY, int(user_id)))
        except Exception as e:
            log_warning("add_active_task 失败", extra={"user_id": user_id, "err": str(e)})
            return False

    async def has_active_task(self, user_id: int) -> bool:
        """
        查询用户是否在活动任务集合中。
        """
        try:
            return bool(await self.redis.sismember(ACTIVE_TASKS_KEY, int(user_id)))
        except Exception:
            return False

    async def remove_active_task(self, user_id: int) -> None:
        await self.redis.srem(ACTIVE_TASKS_KEY, user_id)

    async def get_all_active_tasks(self) -> List[int]:
        ids = await self.redis.smembers(ACTIVE_TASKS_KEY)
        out: List[int] = []
        for i in ids:
            try:
                out.append(int(_b2s(i)))
            except Exception:
                continue
        return sorted(out)

    # ---------- 任意 KV ----------
    async def set_kv(self, user_id: int, field: str, value: Any) -> None:
        try:
            await self.redis.set(self._kv_key(user_id, field), json.dumps(value, ensure_ascii=False))
            log_debug("✅ KV 已保存", extra={"user_id": user_id, "field": field, **(get_log_context() or {})})
        except Exception as e:
            log_exception("❌ KV 保存失败", exc=e, extra={"user_id": user_id, "field": field, **(get_log_context() or {})})

    async def get_kv(self, user_id: int, field: str) -> Optional[Any]:
        try:
            raw = await self.redis.get(self._kv_key(user_id, field))
            if not raw:
                return None
            return json.loads(_b2s(raw))
        except Exception as e:
            log_exception("❌ KV 读取失败", exc=e, extra={"user_id": user_id, "field": field, **(get_log_context() or {})})
            return None

    async def del_kv(self, user_id: int, field: str) -> None:
        try:
            await self.redis.delete(self._kv_key(user_id, field))
            log_debug("🗑️ KV 已删除", extra={"user_id": user_id, "field": field, **(get_log_context() or {})})
        except Exception as e:
            log_exception("❌ KV 删除失败", exc=e, extra={"user_id": user_id, "field": field, **(get_log_context() or {})})

    async def _write_metadata(self, user_id: int, data: Dict[str, Any]) -> None:
        """内部落盘并镜像到 KV"""
        if not isinstance(data, dict):
            data = {}
        try:
            await self.redis.set(self._meta_key(user_id), json.dumps(data, ensure_ascii=False))
        except Exception as e:
            log_exception("❌ _write_metadata 保存失败", exc=e, extra={"user_id": user_id})
            return

        # 镜像到 KV（便于单键读取）
        try:
            for k, v in data.items():
                await self.set_kv(user_id, k, v)
        except Exception as e:
            log_exception("❌ KV 镜像保存失败", exc=e, extra={"user_id": user_id})

    async def set_metadata(self, user_id: int, data: dict | list | tuple | Any, merge: bool = True) -> None:
        """
        将少量结构化元数据（如 emoji_ids）存到独立 metadata 区域。
        默认 merge 合并写；merge=False 则覆盖写。
        同时把每个字段镜像到通用 KV，便于 get_kv 读取。
        """
        try:
            old = await self.get_metadata(user_id) or {}
        except Exception:
            old = {}

        def _coerce_to_dict(obj):
            if obj is None:
                return {}
            if isinstance(obj, dict):
                return dict(obj)
            # list[dict] -> merge
            if isinstance(obj, (list, tuple)) and obj and all(isinstance(x, dict) for x in obj):
                merged = {}
                for x in obj:
                    merged.update(x)
                return merged
            try:
                return dict(obj)
            except Exception:
                raise TypeError(f"metadata must be dict or iterable of (key, value), got {type(obj).__name__}: {obj!r}")

        try:
            coerced = _coerce_to_dict(data)
        except TypeError as ex:
            log_warning(f"set_metadata: invalid data ignored -> {ex}")
            coerced = {}

        new_data = dict(old)
        if merge:
            new_data.update(coerced)
        else:
            new_data = coerced

        await self._write_metadata(user_id, new_data)

    async def get_metadata(self, user_id: int, field: Optional[str] = None) -> dict | Any:
        """
        读取 metadata；支持 field 指定返回单个字段（若不存在返回 {} 或 None）。
        """
        try:
            raw = await self.redis.get(self._meta_key(user_id))
            data = json.loads(_b2s(raw) or "{}") if raw else {}
            if not isinstance(data, dict):
                data = {}
        except Exception as e:
            log_exception("❌ get_metadata 失败", exc=e, extra={"user_id": user_id, **(get_log_context() or {})})
            data = {}

        if field is None:
            return data
        return data.get(field, {} if field in ("emoji_ids", "chat_cooldown", "floodwait") else None)


# ---- 附加：这些方法在类外定义但需绑定 self（保持原接口兼容） ----
def _write_forbidden_key(user_id: int, chat_key: str) -> str:
    return f"tg:blocked:write:{user_id}:{chat_key}"


def _banned_in_ch_key(user_id: int, chat_key: str) -> str:
    return f"tg:blocked:banned:{user_id}:{chat_key}"


async def mark_write_forbidden(self, user_id: int, chat_key: str, hours: int = None) -> None:
    hrs = int(hours if hours is not None else TTL.WRITE_FORBIDDEN_HOURS)
    await self.redis.setex(_write_forbidden_key(user_id, chat_key), max(60, hrs * 3600), "1")

async def mark_banned_in_channel(self, user_id: int, chat_key: str, hours: int = None) -> None:
    hrs = int(hours if hours is not None else TTL.BANNED_IN_CHANNEL_HOURS)
    await self.redis.setex(_banned_in_ch_key(user_id, chat_key), max(60, hrs * 3600), "1")
    

async def is_write_forbidden(self, user_id: int, chat_key: str) -> bool:
    return bool(await self.redis.exists(_write_forbidden_key(user_id, chat_key)))

async def is_banned_in_channel(self, user_id: int, chat_key: str) -> bool:
    return bool(await self.redis.exists(_banned_in_ch_key(user_id, chat_key)))

RedisCommandFSM.mark_write_forbidden = mark_write_forbidden
RedisCommandFSM.is_write_forbidden = is_write_forbidden
RedisCommandFSM.mark_banned_in_channel = mark_banned_in_channel
RedisCommandFSM.is_banned_in_channel = is_banned_in_channel
