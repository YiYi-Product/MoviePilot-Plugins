"""以插件边界替身隔离 MoviePilot 数据库、单例和外部网络"""

import copy
import importlib.util
import sys
import types
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def plugin_module(monkeypatch):
    """逐个测试装载插件，所有 sys.modules 替身均由 monkeypatch 恢复"""
    class SystemConfigKey(Enum):
        """模拟 MoviePilot 配置枚举"""
        MediaServers = "MediaServers"

    class ConfigChangeEventData:
        """保留配置事件的结构"""
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class PluginBase:
        """插件持久化边界替身"""
        def __init__(self):
            self.data = {}
            self.config = {}
            self.servers = []
            self.events = []
            self.systemconfig = SimpleNamespace(get=lambda key: copy.deepcopy(self.servers), set=self._set_servers)
            self.eventmanager = SimpleNamespace(send_event=lambda *args: self.events.append(args))

        def _set_servers(self, key, servers):
            self.servers = copy.deepcopy(servers)
            return True

        def save_data(self, key, value):
            """复制保存，模拟独立数据库记录"""
            self.data[key] = copy.deepcopy(value)

        def get_data(self, key):
            """读取隔离状态"""
            return copy.deepcopy(self.data.get(key))

        def update_config(self, config):
            """只记录当前插件配置"""
            self.config = copy.deepcopy(config)

    class RequestUtils:
        """未显式 mock 的 HTTP 调用应立即失败，避免测试访问外网"""
        def __init__(self, **kwargs):
            raise AssertionError("测试禁止真实网络请求")

    modules = {
        "app": {}, "app.core": {}, "app.utils": {},
        "app.core.config": {"settings": SimpleNamespace(APP_DOMAIN="https://mp.example.com")},
        "app.log": {"logger": SimpleNamespace(info=lambda *args: None)},
        "app.plugins": {"_PluginBase": PluginBase},
        "app.schemas": {"ConfigChangeEventData": ConfigChangeEventData},
        "app.schemas.types": {"SystemConfigKey": SystemConfigKey,
                              "EventType": SimpleNamespace(ConfigChanged="ConfigChanged")},
        "app.utils.http": {"RequestUtils": RequestUtils},
    }
    for name, attrs in modules.items():
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).parents[1] / "plugins.v2" / "yiyimedia" / "__init__.py"
    spec = importlib.util.spec_from_file_location("yiyimedia_test_plugin", path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def plugin(plugin_module):
    """创建未启动调度器的启用插件"""
    instance = plugin_module.YiYiMedia()
    instance._enabled = True
    instance._config = {"enabled": True, "yiyi_url": "https://yiyi.example.com", "pairing_code": "unit-test-code"}
    return instance
