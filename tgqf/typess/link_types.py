# -*- coding: utf-8 -*-
# typess/link_types.py
from __future__ import annotations
import json
import re
from enum import Enum
from typing import Optional, Tuple, Union
from telethon.utils import VALID_USERNAME_RE, USERNAME_RE, TG_JOIN_RE
from pydantic import BaseModel, field_validator

class LinkType(str, Enum):
    INVITE = "invite"
    PUBLIC = "public"
    USERID = "userid"
    CHANNEL_MSG = "channel_msg"
    PUBLIC_MSG = "public_msg"
    CHAT_ID = "chat_id"

ParsedValue = Union[str, Tuple[str, int], Tuple[int, int], int]

class ParsedLink(BaseModel):
    type: LinkType
    value: ParsedValue

    def __repr__(self) -> str:
        return f"<ParsedLink {self.type.value} -> {self.value}>"

    def display(self) -> str:
        return f"[{self.type.value}] {self.short()}"

    def log_repr(self) -> str:
        return f"{self.type.value}:{self.short()}"

    def as_tuple(self) -> tuple:
        return (self.type.value, self.value)


    def cache_key(self) -> str:
        """稳定缓存键：`{type}|{short}`，提供跨模块一致性（缓存/黑名单/挂起）。"""
        return f"{self.type.value}|{self.short()}"

    def is_valid(self) -> bool:
        try:
            if self.is_public():
                return bool(re.fullmatch(r"[A-Za-z0-9_]{4,32}", str(self.value)))
            if self.is_invite():
                return bool(re.fullmatch(r"[A-Za-z0-9_-]{10,}", str(self.value))) 
            if self.is_userid():
                return isinstance(self.value, int)
            if self.is_public_msg():
                return (isinstance(self.value, tuple)
                        and isinstance(self.value[0], str)
                        and isinstance(self.value[1], int))
            if self.is_channel_msg():
                return (isinstance(self.value, tuple)
                        and isinstance(self.value[0], int)
                        and isinstance(self.value[1], int))
            if self.is_chat_id():
                return isinstance(self.value, int)
            return False
        except Exception as e:
            return False

    @classmethod
    def auto_parse(cls, s: str) -> "ParsedLink":
        try:
            return cls.parse(s)
        except Exception:
            return ParsedLink(type=LinkType.PUBLIC, value=s.strip().lower())


    def is_invite(self) -> bool:
        return self.type == LinkType.INVITE

    def is_public(self) -> bool:
        return self.type == LinkType.PUBLIC

    def is_userid(self) -> bool:
        return self.type == LinkType.USERID

    def is_public_msg(self) -> bool:
        return self.type == LinkType.PUBLIC_MSG

    def is_channel_msg(self) -> bool:
        return self.type == LinkType.CHANNEL_MSG

    def is_chat_id(self) -> bool:
        return self.type == LinkType.CHAT_ID

    def to_link(self) -> str:
        if self.type == LinkType.INVITE:
            return f"https://t.me/+{self.value}"
        if self.type == LinkType.PUBLIC:
            return f"https://t.me/{self.value}"
        if self.type == LinkType.PUBLIC_MSG:
            u, m = self.value  # type: ignore[misc]
            return f"https://t.me/{u}/{int(m)}"
        if self.type == LinkType.CHANNEL_MSG:
            c, m = self.value  # type: ignore[misc]
            return f"https://t.me/c/{int(c)}/{int(m)}"
        return str(self.value)

    def short(self) -> str:
        t = self.type
        v = self.value
        if t == LinkType.PUBLIC:
            return str(v).lower()
        if t == LinkType.PUBLIC_MSG:
            u, m = v  # type: ignore[misc]
            return f"{u}/{int(m)}".lower()
        if t == LinkType.CHANNEL_MSG:
            c, m = v  # type: ignore[misc]
            return f"c/{int(c)}/{int(m)}"
        if t == LinkType.INVITE:
            return f"+{str(v)}"
        if t in (LinkType.USERID, LinkType.CHAT_ID):
            return str(int(v))
        return "unknown"

    @property
    def raw(self) -> str:
        return self.to_link()

    @property
    def username(self) -> Optional[str]:
        if self.is_public():
            return str(self.value)
        if self.is_public_msg():
            return self.value[0]
        return None

    @property
    def msg_id(self) -> Optional[int]:
        if self.is_public_msg() or self.is_channel_msg():
            return int(self.value[1])
        return None

    @property
    def chat_id(self) -> Optional[int]:
        if self.is_channel_msg():
            return int(self.value[0])
        return None

    @property
    def peer_id(self) -> Optional[int]:
        if self.is_channel_msg():
            cid = int(self.value[0])
            return int(f"-100{cid}")
        if self.is_userid():
            return int(self.value)
        if self.is_chat_id():
            return int(self.value)
        return None

    def to_dict(self) -> dict:
        if self.is_public_msg():
            u, m = self.value
            return {"type": self.type.value, "value": [str(u).lower(), int(m)]}
        if self.is_channel_msg():
            c, m = self.value
            return {"type": self.type.value, "value": [int(c), int(m)]}
        if self.is_userid():
            return {"type": self.type.value, "value": int(self.value)}
        if self.is_public():
            return {"type": self.type.value, "value": str(self.value).lower()}
        if self.is_chat_id():
            return {"type": self.type.value, "value": int(self.value)}
        return {"type": self.type.value, "value": self.value}

    @classmethod
    def from_dict(cls, d: dict) -> "ParsedLink":
        link_type = LinkType(d["type"])
        value = d["value"]
        if link_type == LinkType.PUBLIC_MSG and isinstance(value, list):
            value = (str(value[0]).lower(), int(value[1]))
        elif link_type == LinkType.CHANNEL_MSG and isinstance(value, list):
            value = (int(value[0]), int(value[1]))
        elif link_type == LinkType.USERID:
            value = int(value)
        elif link_type == LinkType.PUBLIC:
            value = str(value).lower()
        elif link_type == LinkType.INVITE:
            value = str(value)
        elif link_type == LinkType.CHAT_ID:
            value = int(value)
        return cls(type=link_type, value=value)

    def to_storage(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_storage(cls, s: Union[str, dict]) -> "ParsedLink":
        if isinstance(s, dict):
            return cls.from_dict(s)
        try:
            d = json.loads(s)
            return cls.from_dict(d)
        except json.JSONDecodeError as e:
            return cls.parse(str(s))

    @classmethod
    def parse(cls, s: str) -> "ParsedLink":
        raw = (s or "").strip()
        if not raw:
            raise ValueError("empty link")

        text = raw.replace("https://", "").replace("http://", "")
        if text.startswith("t.me/"):
            path = text[len("t.me/"):]
            # /c/<cid>/<mid>
            m = re.fullmatch(r"c/(\d{1,20})/(\d{1,20})/?", path)
            if m:
                return cls(type=LinkType.CHANNEL_MSG, value=(int(m.group(1)), int(m.group(2))))
            # /joinchat/<code> 或 /+<code>
            m = re.fullmatch(r"(?:joinchat/|\+)([A-Za-z0-9_-]{10,})/?", path) 
            if m:
                return cls(type=LinkType.INVITE, value=m.group(1))
            # /s/<username>/<mid> 或 /<username>/<mid>
            m = re.fullmatch(r"(?:s/)?([A-Za-z0-9_]{4,32})/(\d{1,20})/?", path)
            if m:
                return cls(type=LinkType.PUBLIC_MSG, value=(m.group(1).lower(), int(m.group(2))))
            # /s/<username> 或 /<username>
            m = re.fullmatch(r"(?:s/)?([A-Za-z0-9_]{4,32})/?", path)
            if m:
                return cls(type=LinkType.PUBLIC, value=m.group(1).lower())
        elif raw.startswith("+"):
            return cls(type=LinkType.INVITE, value=raw[1:])

        elif raw.startswith("-100") and raw[4:].isdigit():
            return cls(type=LinkType.CHAT_ID, value=int(raw))
        elif raw.isdigit():
            return cls(type=LinkType.USERID, value=int(raw))
        return cls(type=LinkType.PUBLIC, value=raw.lower())

    @field_validator("value", mode="before")
    @classmethod
    def _normalize_value(cls, v, info):
        link_type = info.data.get("type")
        if link_type == LinkType.PUBLIC:
            return str(v).lower()
        if link_type == LinkType.USERID:
            return int(v)
        if link_type == LinkType.INVITE:
            return str(v)
        if link_type == LinkType.PUBLIC_MSG:
            u, m = v
            return (str(u).lower(), int(m))
        if link_type == LinkType.CHANNEL_MSG:
            c, m = v
            return (int(c), int(m))
        if link_type == LinkType.CHAT_ID:
            return int(v)
        return v


# 额外优化的辅助方法
def parse_username(username: str) -> Tuple[Optional[str], bool]:
    """解析 Telegram 用户名"""
    username = username.strip()
    match = USERNAME_RE.match(username) or TG_JOIN_RE.match(username)
    if match:
        username = username[match.end():]
        is_invite = bool(match.group(1))
        if is_invite:
            return username, True
        else:
            username = username.rstrip('/')
    if VALID_USERNAME_RE.match(username):
        return username.lower(), False
    else:
        return None, False
