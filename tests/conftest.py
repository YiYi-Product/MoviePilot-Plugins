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


@pytest.fixture
def load_v3_host_plugin(monkeypatch):
    """构造严格模拟 MoviePilot V3 契约的宿主替身，可分别装载 plugins.v3 或 plugins.v2 插件"""
    def _loader(plugin_rel_path: str = "plugins.v3/yiyimedia/__init__.py"):
        class SystemConfigKey(Enum):
            MediaServers = "MediaServers"

        class MediaType(Enum):
            MOVIE = "电影"
            TV = "电视剧"

        class MediaSource(str, Enum):
            TMDB = "themoviedb"
            Douban = "douban"

        class EventType(str, Enum):
            ConfigChanged = "config.updated"
            SubscribeComplete = "subscribe.complete"
            SubscribeModified = "subscribe.modified"
            SubscribeDeleted = "subscribe.deleted"

        class ConfigChangeEventData:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class Event:
            def __init__(self, event_type, event_data=None):
                self.event_type = event_type
                self.event_data = event_data if event_data is not None else {}

        class NotExistMediaInfo:
            def __init__(self, season=None, episodes=None, total_episode=0, start_episode=0):
                self.season = season
                self.episodes = list(episodes or [])
                self.total_episode = total_episode
                self.start_episode = start_episode

        def normalize_source(src):
            if src is None:
                return None
            if isinstance(src, MediaSource):
                return src
            raw = str(src).strip().lower()
            if raw in ("tmdb", "themoviedb"):
                return MediaSource.TMDB
            if raw == "douban":
                return MediaSource.Douban
            return None

        def resolve_media_identity(media=None, media_source=None, media_id=None):
            if media_source is not None or media_id is not None:
                src = normalize_source(media_source)
                mid = str(media_id).strip() if media_id is not None else ""
                if src and mid and mid != "0":
                    return src, mid
                return None, None
            if media is None:
                return None, None
            src = normalize_source(getattr(media, "media_source", None) if not isinstance(media, dict) else media.get("media_source"))
            raw_mid = getattr(media, "media_id", None) if not isinstance(media, dict) else media.get("media_id")
            mid = str(raw_mid).strip() if raw_mid is not None else ""
            if src and mid and mid != "0":
                return src, mid
            return None, None

        def build_media_key(media_source, media_id):
            src = normalize_source(media_source)
            mid = str(media_id).strip() if media_id is not None else ""
            if not src or not mid or mid == "0":
                return ""
            prefix = "tmdb" if src == MediaSource.TMDB else src.value
            return f"{prefix}:{mid}"

        allowed_sub_fields = {"best_version", "sites", "downloader", "save_path", "quality", "resolution"}
        subscribes_store = {}
        subscribe_calls = []
        next_sub_id = [200]
        search_contexts = []
        search_calls = []
        recognize_calls = []
        download_single_calls = []
        batch_download_calls = []
        history_store = {}
        registered_handlers = []

        def register_event(types_or_type):
            def decorator(func):
                registered_handlers.append((types_or_type, func))
                return func
            return decorator

        class MediaChain:
            return_none = False

            def recognize_media(self, meta=None, mtype=None, media_source=None, media_id=None,
                                episode_group=None, cache=True):
                recognize_calls.append(("MediaChain", {
                    "mtype": mtype,
                    "media_source": media_source,
                    "media_id": media_id,
                }))
                if self.return_none:
                    return None
                return SimpleNamespace(
                    media_source=normalize_source(media_source),
                    media_id=str(media_id) if media_id is not None else None,
                    tmdb_id=int(media_id) if normalize_source(media_source) == MediaSource.TMDB and media_id else None,
                    type=mtype,
                    title="Recognized V3 Media",
                )

        class SearchChain:
            search_error = None

            def recognize_media(self, meta=None, mtype=None, media_source=None, media_id=None,
                                episode_group=None, cache=True):
                recognize_calls.append(("SearchChain", {
                    "mtype": mtype,
                    "media_source": media_source,
                    "media_id": media_id,
                }))
                return SimpleNamespace(
                    media_source=normalize_source(media_source),
                    media_id=str(media_id) if media_id is not None else None,
                    tmdb_id=int(media_id) if normalize_source(media_source) == MediaSource.TMDB and media_id else None,
                    type=mtype,
                    title="Recognized V3 Search Media",
                )

            def search_by_id(self, media_source, media_id, mtype=None, area="title", season=None,
                             sites=None, cache_local=False):
                search_calls.append(("search_by_id", {
                    "media_source": media_source,
                    "media_id": media_id,
                    "mtype": mtype,
                    "season": season,
                }))
                if self.search_error is not None:
                    raise self.search_error
                return list(search_contexts)

            def process(self, mediainfo, keyword=None, no_exists=None, **kwargs):
                search_calls.append(("process", {"mediainfo": mediainfo, "no_exists": no_exists}))
                if self.search_error is not None:
                    raise self.search_error
                return list(search_contexts)

        class DownloadChain:
            single_return = ("hash-v3-1", "")
            single_error = None
            batch_lefts = None
            batch_downloaded_override = None
            downloading_items = []
            list_torrents_items = []
            list_torrents_calls = []

            def download_single(self, context, torrent_file=None, torrent_content=None, episodes=None,
                                channel=None, source=None, downloader=None, save_path=None,
                                userid=None, username=None, label=None, return_detail=False):
                download_single_calls.append({
                    "context": context,
                    "torrent_content": torrent_content,
                    "episodes": episodes,
                    "username": username,
                    "source": source,
                    "downloader": downloader,
                    "save_path": save_path,
                    "label": label,
                    "return_detail": return_detail,
                })
                if self.single_error is not None:
                    raise self.single_error
                return self.single_return if return_detail else self.single_return[0]

            def batch_download(self, contexts, no_exists=None, save_path=None, username=None, source=None, **kwargs):
                batch_download_calls.append({
                    "contexts": contexts,
                    "no_exists": no_exists,
                    "username": username,
                    "source": source,
                })
                downloaded = list(self.batch_downloaded_override) if self.batch_downloaded_override is not None else list(contexts or [])
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

        class DownloadHistoryOper:
            def get_by_hash(self, download_hash):
                return history_store.get(download_hash)

        class SubscribeOper:
            def list(self, state=None):
                return list(subscribes_store.values())

            def get(self, sid):
                return subscribes_store.get(int(sid))

            def delete(self, sid):
                return subscribes_store.pop(int(sid), None)

        class SubscribeChain:
            add_error = None
            fail_message = None

            def add(self, title, year, mtype=None, episode_group=None, season=None, channel=None,
                    source=None, userid=None, username=None, message=True, exist_ok=False,
                    media_source=None, media_id=None, **kwargs):
                subscribe_calls.append({
                    "title": title,
                    "year": year,
                    "mtype": mtype,
                    "season": season,
                    "username": username,
                    "exist_ok": exist_ok,
                    "media_source": media_source,
                    "media_id": media_id,
                    "kwargs": copy.deepcopy(kwargs),
                })
                if self.add_error is not None:
                    raise self.add_error
                unknown = sorted(set(kwargs) - allowed_sub_fields)
                if unknown:
                    # 严格复现 MoviePilot V3 create.py:640-644 捕获 SubscriptionPatch ValueError 后的返回
                    return None, "订阅分类设置无效，请重新选择分类后重试"
                if self.fail_message is not None:
                    return None, self.fail_message
                src, mid = resolve_media_identity(media_source=media_source, media_id=media_id)
                if not src or not mid:
                    # 允许无身份时模拟按标题识别
                    src, mid = MediaSource.TMDB, "298546"
                sid = next_sub_id[0]
                next_sub_id[0] += 1
                # V3 Subscribe 表仅有 media_source / media_id，无 tmdbid 字段
                item = SimpleNamespace(
                    id=sid,
                    type=mtype.value if hasattr(mtype, "value") else mtype,
                    media_source=src.value,
                    media_id=mid,
                    season=season,
                    name=title,
                    year=year,
                    best_version=kwargs.get("best_version", 0),
                    state="R",
                    total_episode=12 if mtype == MediaType.TV else None,
                    lack_episode=4 if mtype == MediaType.TV else None,
                )
                subscribes_store[sid] = item
                return sid, "新增订阅成功"

        class MediaInfo:
            def __init__(self):
                self.type = None
                self.media_source = None
                self.media_id = None
                self.tmdb_id = None
                self.title = None
                self.year = None
                self.imdb_id = None

        class TorrentInfo:
            def __init__(self, **kwargs):
                self.media_source = None
                self.media_id = None
                self.__dict__.update(kwargs)

        class Context:
            def __init__(self, meta_info=None, media_info=None, torrent_info=None):
                self.meta_info = meta_info
                self.media_info = media_info
                self.torrent_info = torrent_info

        class MetaInfoObj:
            def __init__(self, title=""):
                self.title = title
                self.type = None
                self.year = None
                self.media_source = None
                self.media_id = None
                self.begin_season = None
                self.end_season = None
                self.begin_episode = None
                self.end_episode = None

        class PluginBase:
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
                self.data[key] = copy.deepcopy(value)

            def get_data(self, key):
                return copy.deepcopy(self.data.get(key))

            def update_config(self, config):
                self.config = copy.deepcopy(config)

        class RequestUtils:
            def __init__(self, **kwargs):
                raise AssertionError("测试禁止真实网络请求")

        v3_settings = SimpleNamespace(APP_DOMAIN="https://mp-v3.example.com", TORRENT_TAG="MOVIEPILOT")
        v3_logger = SimpleNamespace(info=lambda *args: None)
        v3_eventmgr = SimpleNamespace(register=register_event)

        modules = {
            "app": {}, "app.sdk": {}, "app.chain": {}, "app.db": {}, "app.db.oper": {},
            "app.core": {}, "app.utils": {},
            "app.sdk.config": {"settings": v3_settings},
            "app.sdk.events": {"Event": Event, "eventmanager": v3_eventmgr},
            "app.sdk.logging": {"logger": v3_logger},
            "app.sdk.plugin": {"_PluginBase": PluginBase},
            "app.sdk.network": {"RequestUtils": RequestUtils},
            "app.sdk.media": {
                "Context": Context,
                "MediaInfo": MediaInfo,
                "MetaInfo": MetaInfoObj,
                "TorrentInfo": TorrentInfo,
                "build_media_key": build_media_key,
                "resolve_media_identity": resolve_media_identity,
            },
            # 模拟 V3 兼容映射，使 plugins.v2 也能在 V3 宿主中加载验证
            "app.core.config": {"settings": v3_settings},
            "app.core.event": {"Event": Event, "eventmanager": v3_eventmgr},
            "app.core.context": {"Context": Context, "MediaInfo": MediaInfo, "TorrentInfo": TorrentInfo},
            "app.core.metainfo": {"MetaInfo": MetaInfoObj},
            "app.log": {"logger": v3_logger},
            "app.plugins": {"_PluginBase": PluginBase},
            "app.utils.http": {"RequestUtils": RequestUtils},
            "app.utils.media": {
                "build_media_key": build_media_key,
                "resolve_media_identity": resolve_media_identity,
            },
            "app.chain.subscribe": {"SubscribeChain": SubscribeChain},
            "app.chain.search": {"SearchChain": SearchChain},
            "app.chain.download": {"DownloadChain": DownloadChain},
            "app.chain.media": {"MediaChain": MediaChain},
            "app.db.oper.subscribe": {"SubscribeOper": SubscribeOper},
            "app.db.oper.downloadhistory": {"DownloadHistoryOper": DownloadHistoryOper},
            "app.db.subscribe_oper": {"SubscribeOper": SubscribeOper},
            "app.db.downloadhistory_oper": {"DownloadHistoryOper": DownloadHistoryOper},
            "app.schemas": {"ConfigChangeEventData": ConfigChangeEventData, "NotExistMediaInfo": NotExistMediaInfo},
            "app.schemas.types": {
                "SystemConfigKey": SystemConfigKey,
                "MediaSource": MediaSource,
                "MediaType": MediaType,
                "EventType": EventType,
            },
        }
        for name, attrs in modules.items():
            mod = types.ModuleType(name)
            mod.__dict__.update(attrs)
            monkeypatch.setitem(sys.modules, name, mod)

        path = Path(__file__).parents[1] / plugin_rel_path
        mod_name = "yiyimedia_v3_test_" + plugin_rel_path.replace("/", "_").replace(".", "_")
        spec = importlib.util.spec_from_file_location(mod_name, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        module._test_doubles = SimpleNamespace(
            subscribes_store=subscribes_store,
            subscribe_calls=subscribe_calls,
            SubscribeChain=SubscribeChain,
            SearchChain=SearchChain,
            MediaChain=MediaChain,
            DownloadChain=DownloadChain,
            NotExistMediaInfo=NotExistMediaInfo,
            search_contexts=search_contexts,
            search_calls=search_calls,
            recognize_calls=recognize_calls,
            download_single_calls=download_single_calls,
            batch_download_calls=batch_download_calls,
            history_store=history_store,
            MediaSource=MediaSource,
            MediaType=MediaType,
            Event=Event,
            EventType=EventType,
            registered_handlers=registered_handlers,
        )
        instance = module.YiYiMedia()
        instance._enabled = True
        instance._config = {"enabled": True, "yiyi_url": "https://yiyi.example.com", "pairing_code": "unit-test-code"}
        return module, instance

    return _loader
