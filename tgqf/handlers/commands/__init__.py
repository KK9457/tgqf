# handlers/commands/__init__.py
"""
命令模块索引（不做副作用注册）。
在 handlers/registry.py 中集中调用各模块的 register_commands()。
"""
# 仅保留导入路径提示，避免误用
__all__ = [
    "admin_command",
    "control_command",
    "report_command",
    "group_command",
    "help_command",
    "interval_command",
    "message_command",
    "start_command",
    "accounts_command",
    "upload_command",
    "channel_command", 
]
