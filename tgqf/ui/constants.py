# ui/constants.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from typing import Any

# ========= 顶层入口（菜单/面板） =========
TASKMENU_OPEN: bytes = b"taskmenu:open"
ACCOUNTS_OPEN: bytes = b"accounts:open"

# 上传命名空间各动作
UPLOAD_OPEN:   bytes = b"upload:open"
UPLOAD_DONE:   bytes = b"upload:done"
UPLOAD_CANCEL: bytes = b"upload:cancel"

STATUS_OPEN:   bytes = b"status:open"

# ========= 群发配置菜单项（task:*） =========
TASK_SET_GROUPS:   bytes = b"task:a"
TASK_SET_MESSAGE:  bytes = b"task:b"
TASK_SET_INTERVAL: bytes = b"task:c"
TASK_START:        bytes = b"task:ok"
TASK_STOP:         bytes = b"task:stop"

# ========= 命名空间前缀 =========
NS_TASK: bytes     = b"task:"
NS_ACCT: bytes     = b"a:"
NS_MSG:  bytes     = b"msg:"
NS_MSG_TYPE: bytes = b"msg:type:"
NS_MSG_FWD: bytes  = b"msg:forward:"
NS_WHITE: bytes    = b"white:"

# ========= 预编译正则（bytes） =========
PATTERN_TASK     = re.compile(rb"^task:")
PATTERN_ACCT     = re.compile(rb"^a:")
PATTERN_WHITE    = re.compile(rb"^white:(?:see|remove):")


__all__ = [
    # 入口
    "TASKMENU_OPEN", "ACCOUNTS_OPEN",
    "UPLOAD_OPEN", "UPLOAD_DONE", "UPLOAD_CANCEL",
    "STATUS_OPEN",
    # 任务菜单项
    "TASK_SET_GROUPS","TASK_SET_MESSAGE","TASK_SET_INTERVAL","TASK_START","TASK_STOP",
    # 命名空间/正则
    "NS_TASK","NS_ACCT","NS_MSG","NS_MSG_TYPE","NS_MSG_FWD","NS_WHITE",
    "PATTERN_TASK","PATTERN_ACCT","PATTERN_WHITE",

]
