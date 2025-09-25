# unified/callback_proto.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hmac
import struct
from hashlib import sha256
from typing import Dict, List, Optional, Tuple

# =========================================================
# Callback Token 协议（v2）
# V(1) | NS(1) | ACT(1) | UID(8,BE) | ARGLEN(1) | ARGS | SIG(6)
# =========================================================

_CB_VER = 2
_SEP = "\x1f"  # ARGS 分隔符

NS: Dict[str, bytes] = {
    "acct": b"a",
    "task": b"t",
}

ACT: Dict[str, bytes] = {
    "page":          b"p",
    "toggle":        b"t",
    "refresh":       b"r",
    "close":         b"x",
    "confirm_del":   b"c",
    "do_del":        b"d",
    "confirm_clear": b"C",
    "do_clear":      b"D",
    "sel_all":       b"A",
    "sel_invert":    b"I",
    "set_msg_type":  b"m",
    "set_fwd_mode":  b"f",
    "health":        b"h",
    "health_bulk":   b"H",
}

_NS_INV: Dict[int, str] = {v[0]: k for k, v in NS.items()}
_ACT_INV: Dict[int, str] = {v[0]: k for k, v in ACT.items()}

_MAX_CB_LEN = 64  # Telegram callback_data 上限

def ensure_cb_len(data: bytes, *, label: str = "cb") -> bytes:
    if not isinstance(data, (bytes, bytearray)):
        data = str(data or "").encode("utf-8", errors="ignore")
    if len(data) > _MAX_CB_LEN:
        raise ValueError(f"{label} too long: {len(data)} bytes > 64")
    return bytes(data)

def _assert_ns_consistency() -> None:
    try:
        from ui.constants import NS_ACCT, NS_TASK  # type: ignore
        if NS.get("acct") != NS_ACCT[:1]:
            raise AssertionError(f'NS("acct") != NS_ACCT[:1]: {NS.get("acct")} vs {NS_ACCT[:1]}')
        if NS.get("task") != NS_TASK[:1]:
            raise AssertionError(f'NS("task") != NS_TASK[:1]: {NS.get("task")} vs {NS_TASK[:1]}')
    except Exception:
        pass
_assert_ns_consistency()

def _secret() -> bytes:
    try:
        from unified.config import CALLBACK_SECRET  # type: ignore
        s = CALLBACK_SECRET
        if isinstance(s, str):
            s = s.encode("utf-8", errors="ignore")
        return s
    except Exception:
        return b"default-insecure-secret-change-me"

def _b64url_decode_padded(token: bytes) -> bytes:
    if not isinstance(token, (bytes, bytearray)):
        token = str(token or "").encode("utf-8", errors="ignore")
    pad = (-len(token)) % 4
    if pad:
        token += b"=" * pad
    return base64.urlsafe_b64decode(token)

def pack(ns: str, action: str, owner_uid: int, *args: str) -> bytes:
    nsb = NS.get(ns)
    actb = ACT.get(action)
    if nsb is None or actb is None:
        raise ValueError(f"unknown ns/action: {ns}/{action}")

    args = tuple("" if a is None else str(a) for a in args)
    args_raw = (_SEP.join(args)).encode("utf-8")
    if len(args_raw) > 255:
        raise ValueError("args too long (max 255 bytes after utf-8 join)")

    uid_val = int(owner_uid)
    if not (0 <= uid_val <= 0xFFFFFFFFFFFFFFFF):
        raise ValueError("owner_uid out of range for 64-bit unsigned int")

    head = struct.pack(">BBBQB", _CB_VER, nsb[0], actb[0], uid_val, len(args_raw))
    core = head + args_raw

    sig = hmac.new(_secret(), core, sha256).digest()[:6]
    token = base64.urlsafe_b64encode(core + sig)
    return token

def _unpack_v1(raw: bytes) -> Optional[Tuple[str, str, int, List[str]]]:
    if len(raw) < 14 or raw[0] != 1:
        return None
    nsb = raw[1]
    actb = raw[2]
    uid = struct.unpack(">I", raw[3:7])[0]
    arglen = raw[7]
    core_end = 8 + arglen
    if len(raw) < core_end + 6:
        return None
    core = raw[:core_end]
    sig = raw[core_end : core_end + 6]
    expect = hmac.new(_secret(), core, sha256).digest()[:6]
    if not hmac.compare_digest(sig, expect):
        return None
    ns = _NS_INV.get(nsb)
    act = _ACT_INV.get(actb)
    if not ns or not act:
        return None
    args_raw = raw[8 : 8 + arglen]
    args = args_raw.decode("utf-8").split(_SEP) if arglen else []
    return ns, act, int(uid), args

def _unpack_v2(raw: bytes) -> Optional[Tuple[str, str, int, List[str]]]:
    if len(raw) < 18 or raw[0] != 2:
        return None
    nsb = raw[1]
    actb = raw[2]
    uid = struct.unpack(">Q", raw[3:11])[0]
    arglen = raw[11]
    core_end = 12 + arglen
    if len(raw) < core_end + 6:
        return None
    core = raw[:core_end]
    sig = raw[core_end : core_end + 6]
    expect = hmac.new(_secret(), core, sha256).digest()[:6]
    if not hmac.compare_digest(sig, expect):
        return None
    ns = _NS_INV.get(nsb)
    act = _ACT_INV.get(actb)
    if not ns or not act:
        return None
    args_raw = raw[12 : 12 + arglen]
    args = args_raw.decode("utf-8").split(_SEP) if arglen else []
    return ns, act, int(uid), args

def unpack(token: bytes) -> Optional[Tuple[str, str, int, List[str]]]:
    try:
        raw = _b64url_decode_padded(token)
        if not raw:
            return None
        ver = raw[0]
        if ver == 1:
            return _unpack_v1(raw)
        if ver == 2:
            return _unpack_v2(raw)
        return None
    except Exception:
        return None

def register_ns(name: str, code: bytes) -> None:
    if not isinstance(code, (bytes, bytearray)) or len(code) != 1:
        raise ValueError("ns code must be exactly 1 byte")
    if name in NS or code[0] in _NS_INV:
        raise ValueError("ns name/code already occupied")
    NS[name] = bytes(code)
    _NS_INV[code[0]] = name

def register_action(name: str, code: bytes) -> None:
    if not isinstance(code, (bytes, bytearray)) or len(code) != 1:
        raise ValueError("action code must be exactly 1 byte")
    if name in ACT or code[0] in _ACT_INV:
        raise ValueError("action name/code already occupied")
    ACT[name] = bytes(code)
    _ACT_INV[code[0]] = name

def pack_acct(action: str, owner_uid: int, *args: str) -> bytes:
    return pack("acct", action, owner_uid, *args)

def pack_task(action: str, owner_uid: int, *args: str) -> bytes:
    return pack("task", action, owner_uid, *args)

__all__ = [
    "pack", "unpack",
    "pack_acct", "pack_task",
    "register_ns", "register_action",
    "NS", "ACT",
    "ensure_cb_len",
]
