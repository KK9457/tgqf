# handlers/commands/help_command.py
from core.decorators import super_command
from unified.config import get_admin_ids
from unified.context import get_fsm


@super_command(trace_action="帮助", white_only=True)
async def handle_help(event):
    sender_id = event.sender_id
    lines = [
        "📚 可用命令：",
        "/a - 设置群组",
        "/b - 设置消息",
        "/c - 设置间隔",
        "/d - 查看状态",
        "/ok - 启动任务",
        "/p - 重置配置",
        "/h - 上传账号",
        "/stop - 停止任务",
        "/help - 查看帮助",
    ]

    # 仅当“存在管理员列表且当前用户是管理员”时追加管理员命令
    try:
        _ = get_fsm()  # 仅确保已初始化；不使用也不报错
        admin_ids = get_admin_ids()
        if admin_ids and sender_id in admin_ids:
            lines += [
                "",
                "👮‍♂️ 管理员命令：",
                "/ad - 添加白名单",
                "/re - 移除白名单",
                "/see - 查看白名单",
                "/jq - 创建频道/超级群",
                "/fixjq - 补偿邀请与权限",
            ]
    except Exception:
        lines.append("⚠️ 管理员身份检查失败")

    await event.respond("\n".join(lines))


def register_commands():
    from router.command_router import main_router

    main_router.command("/help", trace_name="帮助", white_only=True)(handle_help)
    return True
