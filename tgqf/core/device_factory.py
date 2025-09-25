# core/device_factory.py
from __future__ import annotations

from opentele.api import API
from opentele.devices import AndroidDevice, WindowsDevice


def get_device_info(user_id: int, session_name: str = "", platform: str = "android"):
    """
    获取指定平台的随机设备信息。
    - user_id + session_name 会影响生成结果（保证同账号一致）
    - 支持平台：android / windows
    """
    unique_id = f"{user_id}-{session_name or 'NO_PHONE'}"
    plat = (platform or "android").lower()
    if plat == "windows":
        return WindowsDevice.RandomDevice(unique_id)
    elif plat == "android":
        return AndroidDevice.RandomDevice(unique_id)
    else:
        raise ValueError(f"未知平台: {platform}")


def get_api_config(
    user_id: int,
    session_name: str = "",
    platform: str = "android",
    lang_code: str = "en",
    system_lang_code: str = "en-US",
):
    """
    返回 APIData（OpenTele），使用官方 Generate(unique_id) 生成稳定设备数据
    并强制覆盖语言默认：lang_code='en'，system_lang_code='en-US'
    """
    unique_id = f"{user_id}-{(session_name or 'default')}"
    plat = (platform or "android").lower()

    if plat == "android":
        api = API.TelegramAndroid.Generate(unique_id=unique_id)
    elif plat == "windows":
        api = API.TelegramDesktop.Generate(system="windows", unique_id=unique_id)
    else:
        raise ValueError(f"未知平台: {platform}")

    # 显式覆盖语言，统一默认
    api.lang_code = lang_code or "en"
    api.system_lang_code = system_lang_code or "en-US"
    return api


def patch_json_config(
    json_data: dict, user_id: int, phone: str, platform: str = "android"
):
    """
    - 仅补缺，不覆盖
    - 设备/系统字段来自 OpenTele APIData.Generate(unique_id)
    - 统一语言默认：lang_code='en'，system_lang_code='en-US'
    - api_id 确保为 int
    """
    REQUIRED_API_FIELDS = [
        "api_id",
        "api_hash",
        "device_model",
        "system_version",
        "app_version",
        "lang_code",
        "system_lang_code",
    ]
    patched = False
    plat = (platform or "android").lower()

    if any(not json_data.get(k) for k in REQUIRED_API_FIELDS):
        api = get_api_config(user_id, phone, plat)  # 已经是 APIData
        # 设备/系统来自 APIData（Generate 已注入）
        json_data.setdefault("device_model", getattr(api, "device_model", None))
        json_data.setdefault("system_version", getattr(api, "system_version", None))
        json_data.setdefault("app_version", getattr(api, "app_version", None))
        # 语言默认
        json_data.setdefault("lang_code", getattr(api, "lang_code", "en"))
        json_data.setdefault(
            "system_lang_code", getattr(api, "system_lang_code", "en-US")
        )
        # API id/hash
        api_id = getattr(api, "api_id", None)
        try:
            api_id = int(api_id) if api_id is not None else None
        except Exception:
            pass
        if api_id is not None:
            json_data.setdefault("api_id", api_id)
        if getattr(api, "api_hash", None):
            json_data.setdefault("api_hash", api.api_hash)

        json_data.setdefault("platform", plat)
        patched = True

    return patched, json_data
