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

    class MediaType(Enum):
        """模拟 MoviePilot 媒体类型枚举"""
        MOVIE = "电影"
        TV = "电视剧"

    class ConfigChangeEventData:
        """保留配置事件的结构"""
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Event:
        """模拟宿主事件对象"""
        def __init__(self, event_type, event_data=None):
            self.event_type = event_type
            self.event_data = event_data or {}

    registered_handlers = []

    def register_event(types_or_type):
        def decorator(func):
            registered_handlers.append((types_or_type, func))
            return func
        return decorator

    subscribes_store = {}
    subscribe_calls = []
    next_sub_id = [100]
    search_contexts = []
    search_calls = []
    download_single_calls = []
    batch_download_calls = []

    class NotExistMediaInfo:
        """隔离的缺集元数据替身"""
        def __init__(self, season=None, episodes=None, total_episode=0, start_episode=0):
            self.season = season
            self.episodes = list(episodes or [])
            self.total_episode = total_episode
            self.start_episode = start_episode

    class SearchChain:
        """隔离的资源搜索业务链替身"""
        search_error = None

        def recognize_media(self, **kwargs):
            return SimpleNamespace(tmdb_id=kwargs.get("tmdbid"), type=kwargs.get("mtype"))

        def search_by_id(self, **kwargs):
            search_calls.append(("search_by_id", copy.deepcopy(kwargs)))
            if self.search_error is not None:
                raise self.search_error
            return list(search_contexts)

        def process(self, **kwargs):
            search_calls.append(("process", kwargs))
            if self.search_error is not None:
                raise self.search_error
            return list(search_contexts)

    class DownloadChain:
        """隔离的下载执行业务链替身"""
        single_return = "hash-abc-123"
        single_error = None
        batch_lefts = None
        batch_downloaded_override = None
        downloading_items = []
        list_torrents_items = []
        list_torrents_calls = []

        def download_single(self, **kwargs):
            download_single_calls.append(kwargs)
            if self.single_error is not None:
                raise self.single_error
            return self.single_return

        def batch_download(self, **kwargs):
            batch_download_calls.append(kwargs)
            downloaded = list(self.batch_downloaded_override) if self.batch_downloaded_override is not None else list(kwargs.get("contexts") or [])
            lefts = self.batch_lefts if self.batch_lefts is not None else {}
            return downloaded, lefts

        def downloading(self, name=None):
            return list(self.downloading_items)

        def list_torrents(self, status=None, hashs=None, downloader=None, include_all_tags=False):
            self.list_torrents_calls.append({
                "status": status,
                "hashs": list(hashs) if isinstance(hashs, list) else hashs,
                "downloader": downloader,
                "include_all_tags": include_all_tags,
            })
            if not self.list_torrents_items:
                return []
            if hashs:
                wanted = {str(h).lower() for h in (hashs if isinstance(hashs, list) else [hashs])}
                return [item for item in self.list_torrents_items if str(getattr(item, "hash", "")).lower() in wanted]
            return list(self.list_torrents_items)

    history_store = {}

    class DownloadHistoryOper:
        """隔离的下载历史查询替身"""
        def get_by_hash(self, download_hash):
            return history_store.get(download_hash)

    class SubscribeOper:
        """隔离的订阅数据库操作替身"""
        def list(self):
            return list(subscribes_store.values())

        def get(self, sid):
            return subscribes_store.get(int(sid))

        def delete(self, sid):
            return subscribes_store.pop(int(sid), None)

    class SubscribeChain:
        """隔离的订阅业务链替身"""
        add_error = None
        fail_message = None

        def add(self, **kwargs):
            subscribe_calls.append(copy.deepcopy(kwargs))
            if self.add_error is not None:
                raise self.add_error
            if self.fail_message is not None:
                return None, self.fail_message
            sid = next_sub_id[0]
            next_sub_id[0] += 1
            item = SimpleNamespace(
                id=sid,
                type=kwargs["mtype"],
                tmdbid=kwargs["tmdbid"],
                season=kwargs.get("season"),
                name=kwargs["title"],
                year=kwargs.get("year"),
                best_version=kwargs.get("best_version", 0),
                state="R",
                total_episode=12 if kwargs["mtype"] == MediaType.TV else None,
                lack_episode=4 if kwargs["mtype"] == MediaType.TV else None,
            )
            subscribes_store[sid] = item
            return sid, "订阅添加成功"

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

    event_types = SimpleNamespace(
        ConfigChanged="ConfigChanged",
        SubscribeComplete="subscribe.complete",
        SubscribeModified="subscribe.modified",
        SubscribeDeleted="subscribe.deleted",
    )

    modules = {
        "app": {}, "app.core": {}, "app.utils": {}, "app.chain": {}, "app.db": {},
        "app.core.config": {"settings": SimpleNamespace(APP_DOMAIN="https://mp.example.com")},
        "app.core.event": {"Event": Event, "eventmanager": SimpleNamespace(register=register_event)},
        "app.chain.subscribe": {"SubscribeChain": SubscribeChain},
        "app.chain.search": {"SearchChain": SearchChain},
        "app.chain.download": {"DownloadChain": DownloadChain},
        "app.db.subscribe_oper": {"SubscribeOper": SubscribeOper},
        "app.db.downloadhistory_oper": {"DownloadHistoryOper": DownloadHistoryOper},
        "app.log": {"logger": SimpleNamespace(info=lambda *args: None)},
        "app.plugins": {"_PluginBase": PluginBase},
        "app.schemas": {"ConfigChangeEventData": ConfigChangeEventData, "NotExistMediaInfo": NotExistMediaInfo},
        "app.schemas.types": {
            "SystemConfigKey": SystemConfigKey,
            "MediaType": MediaType,
            "EventType": event_types,
        },
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
    module._test_doubles = SimpleNamespace(
        subscribes_store=subscribes_store,
        subscribe_calls=subscribe_calls,
        SubscribeChain=SubscribeChain,
        SearchChain=SearchChain,
        DownloadChain=DownloadChain,
        NotExistMediaInfo=NotExistMediaInfo,
        search_contexts=search_contexts,
        search_calls=search_calls,
        download_single_calls=download_single_calls,
        batch_download_calls=batch_download_calls,
        history_store=history_store,
        MediaType=MediaType,
        Event=Event,
        EventType=event_types,
        registered_handlers=registered_handlers,
    )
    return module


@pytest.fixture
def plugin(plugin_module):
    """创建未启动调度器的启用插件"""
    instance = plugin_module.YiYiMedia()
    instance._enabled = True
    instance._config = {"enabled": True, "yiyi_url": "https://yiyi.example.com", "pairing_code": "unit-test-code"}
    return instance
