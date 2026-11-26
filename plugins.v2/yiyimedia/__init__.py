"""YiYi Media 与 MoviePilot 的双向配对与求片订阅插件"""

import base64
import copy
import hashlib
import hmac
import inspect
import re
import secrets
import threading
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Body, Depends, Header, HTTPException

from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import ConfigChangeEventData
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.utils.http import RequestUtils

try:
    from app.chain.search import SearchChain
except ImportError:  # pragma: no cover - 兼容精简宿主环境
    SearchChain = None

try:
    from app.chain.download import DownloadChain
except ImportError:  # pragma: no cover - 兼容精简宿主环境
    DownloadChain = None

try:
    from app.chain.media import MediaChain
except ImportError:  # pragma: no cover - 兼容精简宿主环境
    MediaChain = None

try:
    from app.core.context import Context, MediaInfo, TorrentInfo
except ImportError:  # pragma: no cover - 兼容精简宿主环境
    Context = None
    MediaInfo = None
    TorrentInfo = None

try:
    from app.core.metainfo import MetaInfo
except ImportError:  # pragma: no cover - 兼容精简宿主环境
    MetaInfo = None

try:
    from app.db.downloadhistory_oper import DownloadHistoryOper
except ImportError:  # pragma: no cover - 兼容精简宿主环境
    DownloadHistoryOper = None

try:
    from app.schemas import NotExistMediaInfo
except ImportError:  # pragma: no cover - 兼容旧版 schemas 导出路径
    try:
        from app.schemas.mediaserver import NotExistMediaInfo
    except ImportError:
        NotExistMediaInfo = None

try:
    from app.utils.media import build_media_key, resolve_media_identity
except ImportError:  # pragma: no cover - 兼容旧版 MoviePilot v2（键为 int tmdbid）
    build_media_key = None
    resolve_media_identity = None

try:
    from app.db.subscribe_oper import SubscribeOper
except ImportError:  # pragma: no cover - 兼容不同 MoviePilot 宿主模块路径
    from app.db.oper.subscribe import SubscribeOper

API_BASE = "/api/media/integrations/moviepilot"
STATE_KEY = "connection"
SUBSCRIPTIONS_KEY = "subscriptions"
PROTOCOL_VERSION = 4
CAPABILITIES = ["media_server", "subscribe", "download", "torrent_upload"]
MAX_TRACKED_SUBSCRIPTIONS = 2000
MAX_SEARCH_SESSIONS = 32
SEARCH_SESSION_TTL_SECONDS = 900
MAX_SEARCH_RESULTS = 100
MAX_TORRENT_UPLOAD_BYTES = 10 * 1024 * 1024


class IntegrationError(ValueError):
    """可直接展示给管理员的固定错误信息"""


class YiYiMedia(_PluginBase):
    """管理独立集成凭据、插件创建的媒体服务器与求片订阅/缺集直接下载/用户种子投递联动，不改动其它服务器"""

    plugin_name = "YiYi Media"
    plugin_desc = "双向配对授权、自动添加 YiYi Media 媒体服务器并联动处理求片订阅、缺集直连下载与审核种子投递。"
    plugin_icon = "Emby_A.png"
    plugin_version = "1.3.0"
    plugin_author = "YiYi-Product"
    author_url = "https://github.com/YiYi-Product"
    plugin_config_prefix = "yiyimedia_"
    plugin_order = 30
    auth_level = 1

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._config = {}
        self._scheduler = None
        self._lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._generation = 0
        self._search_cache: Dict[str, Dict[str, Any]] = {}

    def init_plugin(self, config: Optional[dict] = None):
        """延后建立连接，确保插件回调路由已完成注册"""
        self.stop_service()
        with self._state_lock:
            self._generation += 1
            self._config = dict(config or {})
            self._enabled = bool(self._config.get("enabled"))
        if not self._enabled and not self._config.get("disconnect"):
            self._disable_server()
            return
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._scheduler.add_job(self.synchronize, "interval", seconds=60,
                                next_run_time=datetime.now(timezone.utc) + timedelta(seconds=3),
                                max_instances=1, coalesce=True)
        self._scheduler.start()

    def get_state(self) -> bool:
        """返回插件开关状态"""
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        """本期不提供聊天命令"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """配对和连通检查由插件自己的调度器执行"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """仅豁免全局 API_TOKEN 检查，所有回调必须通过独立鉴权依赖"""
        def ping_callback(authorization=Depends(self.verify_integration_key)):
            """把鉴权时的连接身份传给回调，避免旧请求作用于新绑定"""
            return self.ping(authorization)

        def disconnect_callback(authorization=Depends(self.verify_integration_key)):
            """仅解除本次鉴权所对应的绑定"""
            return self.disconnect(authorization)

        def create_subscribe_callback(payload: Dict[str, Any] = Body(...),
                                      authorization=Depends(self.verify_integration_key)):
            """按已验证连接创建或幂等关联 MoviePilot 订阅"""
            return self.create_subscribe(payload, authorization)

        def query_subscribes_callback(payload: Dict[str, Any] = Body(...),
                                      authorization=Depends(self.verify_integration_key)):
            """批量读取关联订阅的当前状态与剩余集数"""
            return self.query_subscribes(payload, authorization)

        def cancel_subscribe_callback(payload: Dict[str, Any] = Body(...),
                                      authorization=Depends(self.verify_integration_key)):
            """取消指定的 MoviePilot 订阅"""
            return self.cancel_subscribe(payload, authorization)

        def search_downloads_callback(payload: Dict[str, Any] = Body(...),
                                      authorization=Depends(self.verify_integration_key)):
            """按 TMDB ID 与季/集检索候选资源，返回脱敏列表并缓存会话句柄"""
            return self.search_downloads(payload, authorization)

        def trigger_download_callback(payload: Dict[str, Any] = Body(...),
                                      authorization=Depends(self.verify_integration_key)):
            """触发 MoviePilot 直接下载（支持 AUTO 自动优选补全与 SELECTED 手动选种下载）"""
            return self.trigger_download(payload, authorization)

        def upload_torrent_download_callback(payload: Dict[str, Any] = Body(...),
                                             authorization=Depends(self.verify_integration_key)):
            """接收 YiYi 管理员审核通过的用户种子文件，按指定 TMDB ID 与季/集推送 MoviePilot 下载"""
            return self.upload_torrent_download(payload, authorization)

        def query_downloads_callback(payload: Dict[str, Any] = Body(...),
                                     authorization=Depends(self.verify_integration_key)):
            """按下载任务 hash 批量查询 MoviePilot 下载进度与状态"""
            return self.query_downloads(payload, authorization)

        return [
            {"path": "/ping", "endpoint": ping_callback, "methods": ["GET"],
             "summary": "YiYi 双向连通验证", "allow_anonymous": True},
            {"path": "/disconnect", "endpoint": disconnect_callback, "methods": ["POST"],
             "summary": "YiYi 解除配对", "allow_anonymous": True},
            {"path": "/subscribes", "endpoint": create_subscribe_callback, "methods": ["POST"],
             "summary": "YiYi 创建或关联求片订阅", "allow_anonymous": True},
            {"path": "/subscribes/query", "endpoint": query_subscribes_callback, "methods": ["POST"],
             "summary": "YiYi 批量查询订阅进度", "allow_anonymous": True},
            {"path": "/subscribes/cancel", "endpoint": cancel_subscribe_callback, "methods": ["POST"],
             "summary": "YiYi 取消求片订阅", "allow_anonymous": True},
            {"path": "/downloads/search", "endpoint": search_downloads_callback, "methods": ["POST"],
             "summary": "YiYi 检索可下载候选资源", "allow_anonymous": True},
            {"path": "/downloads", "endpoint": trigger_download_callback, "methods": ["POST"],
             "summary": "YiYi 直接触发 MoviePilot 下载", "allow_anonymous": True},
            {"path": "/downloads/torrent", "endpoint": upload_torrent_download_callback, "methods": ["POST"],
             "summary": "YiYi 投递已审核种子文件至 MoviePilot 下载", "allow_anonymous": True},
            {"path": "/downloads/query", "endpoint": query_downloads_callback, "methods": ["POST"],
             "summary": "YiYi 批量查询种子下载任务状态", "allow_anonymous": True},
        ]

    def verify_integration_key(self, key: Optional[str] = Header(default=None, alias="X-YiYi-Integration-Key")):
        """以常量时间验证插件专属凭据，不接受 MoviePilot 全局密钥"""
        with self._state_lock:
            state = self.get_data(STATE_KEY) or {}
            expected = state.get("callback_key", "")
            if (not self._enabled or not state.get("connection_id") or not key or not expected or len(key) > 128
                    or not hmac.compare_digest(key.encode("utf-8"), expected.encode("utf-8"))):
                raise HTTPException(status_code=401, detail="YiYi 集成授权无效")
            return self._generation, state["connection_id"]

    def ping(self, authorization: Optional[tuple] = None) -> dict:
        """只返回连接标识与能力声明，不返回配置或凭据"""
        with self._state_lock:
            self._validate_callback(authorization)
            state = self.get_data(STATE_KEY) or {}
            return {
                "instanceId": state.get("instance_id"),
                "connectionId": state.get("connection_id"),
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": list(CAPABILITIES),
            }

    def disconnect(self, authorization: Optional[tuple] = None) -> dict:
        """先让在途同步无法恢复旧授权，再清理插件管理的服务器"""
        # 回调可能发生在 synchronize 等待 YiYi 响应时，不能在此获取同步锁。
        with self._state_lock:
            self._validate_callback(authorization)
            self._clear_binding("双方授权已解除")
            return {"success": True}

    def create_subscribe(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """通过 MoviePilot SubscribeChain 创建订阅；若同片同季已存在则直接幂等挂载"""
        with self._state_lock:
            self._validate_callback(authorization)
        data = payload if isinstance(payload, dict) else {}
        media_kind = str(data.get("mediaType") or "").strip().upper()
        if media_kind not in ("MOVIE", "TV"):
            raise HTTPException(status_code=400, detail="不支持的媒体类型")
        tmdb_id = self._positive_int(data.get("tmdbId"), "tmdbId")
        title = str(data.get("title") or "").strip()
        if not title or len(title) > 512:
            raise HTTPException(status_code=400, detail="媒体标题无效")
        season = self._positive_int(data.get("season"), "season") if media_kind == "TV" else None
        year_val = data.get("year")
        year = str(year_val).strip() if year_val not in (None, "") else ""
        if year and not re.fullmatch(r"[0-9]{4}", year):
            year = ""
        best_version = 1 if bool(data.get("bestVersion")) else 0
        request_id = data.get("requestId")
        tracked_request_id = int(request_id) if isinstance(request_id, int) and request_id > 0 else None
        username = str(data.get("username") or "").strip()[:64] or "YiYi Media"

        oper = SubscribeOper()
        existing = self._find_existing_subscribe(oper, media_kind, tmdb_id, season)
        if existing is not None:
            sub_id = int(getattr(existing, "id"))
            self._remember_subscription(sub_id, tracked_request_id, media_kind, tmdb_id, season)
            return {
                "success": True,
                "action": "ATTACHED",
                "mpSubscribeId": sub_id,
                "state": str(getattr(existing, "state", "") or "R"),
                "totalEpisode": self._optional_int(getattr(existing, "total_episode", None)),
                "lackEpisode": self._optional_int(getattr(existing, "lack_episode", None)),
                "message": "已关联 MoviePilot 中已存在的同款订阅",
            }

        mtype = MediaType.TV if media_kind == "TV" else MediaType.MOVIE
        try:
            sid, err_msg = SubscribeChain().add(
                mtype=mtype,
                title=title,
                year=year,
                tmdbid=tmdb_id,
                season=season,
                best_version=best_version,
                exist_ok=True,
                username=username,
            )
        except Exception:
            return {"success": False, "action": "FAILED", "message": "MoviePilot 执行添加订阅时发生内部异常"}

        resolved = oper.get(int(sid)) if sid else self._find_existing_subscribe(oper, media_kind, tmdb_id, season)
        resolved_id = int(sid) if sid else (int(getattr(resolved, "id")) if resolved is not None else None)
        if resolved_id is None:
            return {
                "success": False,
                "action": "FAILED",
                "message": self._sanitize_message(err_msg) or "MoviePilot 未能识别或创建该订阅",
            }
        self._remember_subscription(resolved_id, tracked_request_id, media_kind, tmdb_id, season)
        return {
            "success": True,
            "action": "CREATED" if sid else "ATTACHED",
            "mpSubscribeId": resolved_id,
            "state": str(getattr(resolved, "state", "") or "R") if resolved is not None else "R",
            "totalEpisode": self._optional_int(getattr(resolved, "total_episode", None)) if resolved is not None else None,
            "lackEpisode": self._optional_int(getattr(resolved, "lack_episode", None)) if resolved is not None else None,
            "message": self._sanitize_message(err_msg) or "已创建 MoviePilot 订阅",
        }

    def query_subscribes(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """按 MoviePilot 订阅 ID 列表批量返回状态"""
        with self._state_lock:
            self._validate_callback(authorization)
        data = payload if isinstance(payload, dict) else {}
        raw_ids = data.get("mpSubscribeIds")
        if not isinstance(raw_ids, list) or len(raw_ids) > 200:
            raise HTTPException(status_code=400, detail="mpSubscribeIds 必须是长度不超过 200 的数组")
        oper = SubscribeOper()
        items = []
        for raw_id in raw_ids:
            sub_id = self._positive_int(raw_id, "mpSubscribeId")
            sub = oper.get(sub_id)
            if sub is None:
                items.append({
                    "mpSubscribeId": sub_id,
                    "exists": False,
                    "state": "NOT_FOUND",
                    "totalEpisode": None,
                    "lackEpisode": None,
                })
            else:
                items.append({
                    "mpSubscribeId": sub_id,
                    "exists": True,
                    "state": str(getattr(sub, "state", "") or "R"),
                    "totalEpisode": self._optional_int(getattr(sub, "total_episode", None)),
                    "lackEpisode": self._optional_int(getattr(sub, "lack_episode", None)),
                })
        return {"items": items}

    def cancel_subscribe(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """按 MoviePilot 订阅 ID 取消订阅（若已不存在则幂等成功）"""
        with self._state_lock:
            self._validate_callback(authorization)
        data = payload if isinstance(payload, dict) else {}
        sub_id = self._positive_int(data.get("mpSubscribeId"), "mpSubscribeId")
        oper = SubscribeOper()
        existing = oper.get(sub_id)
        if existing is not None:
            oper.delete(sub_id)
        self._forget_subscription(sub_id)
        return {"success": True, "removed": existing is not None}

    def search_downloads(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """检索匹配 TMDB ID 与季/集的候选种子，仅返回脱敏摘要并缓存完整上下文于本机内存"""
        with self._state_lock:
            self._validate_callback(authorization)
            state = self.get_data(STATE_KEY) or {}
            scope_key = (self._generation, state.get("connection_id"))
        if SearchChain is None:
            return {"success": False, "action": "UNSUPPORTED", "searchToken": None, "totalCount": 0, "items": [],
                    "message": "当前 MoviePilot 运行环境不支持资源检索链"}

        data = payload if isinstance(payload, dict) else {}
        media_kind = str(data.get("mediaType") or "TV").strip().upper()
        if media_kind not in ("MOVIE", "TV"):
            raise HTTPException(status_code=400, detail="不支持的媒体类型")
        tmdb_id = self._positive_int(data.get("tmdbId"), "tmdbId")
        season = self._positive_int(data.get("season"), "season") if media_kind == "TV" else None
        missing_episodes = self._parse_episodes(data.get("episodes"))
        keyword_filter = str(data.get("keyword") or "").strip().lower()
        mtype = MediaType.TV if media_kind == "TV" else MediaType.MOVIE

        try:
            contexts = self._search_contexts(mtype=mtype, tmdb_id=tmdb_id, season=season, missing_episodes=None,
                                             total_episode=None)
        except Exception:
            return {"success": False, "action": "SEARCH_FAILED", "searchToken": None, "totalCount": 0, "items": [],
                    "message": "MoviePilot 执行资源检索时发生异常，请查看 MoviePilot 日志"}

        entries: List[Tuple[dict, Any]] = []
        missing_set = set(missing_episodes)
        for ctx in contexts or []:
            torrent = getattr(ctx, "torrent_info", None)
            if torrent is None:
                continue
            title = self._sanitize_text(getattr(torrent, "title", "") or "", 300)
            description = self._sanitize_text(getattr(torrent, "description", "") or "", 300)
            site_name = self._sanitize_text(getattr(torrent, "site_name", "") or "", 64)
            if keyword_filter:
                haystack = f"{title} {description} {site_name}".lower()
                if keyword_filter not in haystack:
                    continue
            meta = getattr(ctx, "meta_info", None)
            ep_list = self._parse_episodes(getattr(meta, "episode_list", None))
            season_list = self._parse_episodes(getattr(meta, "season_list", None))
            if media_kind == "TV" and season is not None and season_list and season not in season_list:
                continue
            whole_season = media_kind == "TV" and not ep_list
            if missing_set:
                if whole_season:
                    matched_eps = list(missing_episodes)
                    matches_missing = True
                else:
                    matched_eps = sorted(missing_set.intersection(ep_list))
                    matches_missing = bool(matched_eps)
            else:
                matched_eps = list(ep_list)
                matches_missing = True

            raw_labels = getattr(torrent, "labels", None) or []
            labels = [self._sanitize_text(str(lbl), 32) for lbl in raw_labels if str(lbl).strip()][:10]
            item_summary = {
                "siteName": site_name,
                "title": title,
                "description": description,
                "size": int(float(getattr(torrent, "size", 0) or 0)),
                "seeders": self._optional_int(getattr(torrent, "seeders", 0)) or 0,
                "peers": self._optional_int(getattr(torrent, "peers", 0)) or 0,
                "grabs": self._optional_int(getattr(torrent, "grabs", 0)) or 0,
                "pubdate": self._sanitize_text(str(getattr(torrent, "pubdate", "") or ""), 40),
                "volumeFactor": self._sanitize_text(str(getattr(torrent, "volume_factor", "") or ""), 32),
                "hitAndRun": bool(getattr(torrent, "hit_and_run", False)),
                "labels": labels,
                "seasonEpisode": self._sanitize_text(str(getattr(meta, "season_episode", "") or ""), 48),
                "episodes": ep_list[:200],
                "wholeSeason": whole_season,
                "resolution": self._sanitize_text(str(getattr(meta, "resource_pix", "") or ""), 32),
                "edition": self._sanitize_text(str(getattr(meta, "edition", "") or ""), 48),
                "videoEncode": self._sanitize_text(str(getattr(meta, "video_encode", "") or ""), 32),
                "audioEncode": self._sanitize_text(str(getattr(meta, "audio_encode", "") or ""), 32),
                "resourceTeam": self._sanitize_text(str(getattr(meta, "resource_team", "") or ""), 48),
                "priOrder": self._optional_int(getattr(torrent, "pri_order", 0)) or 0,
                "matchesMissing": matches_missing,
                "matchedEpisodes": matched_eps[:200],
            }
            entries.append((item_summary, ctx))

        entries.sort(
            key=lambda pair: (
                1 if pair[0]["matchesMissing"] else 0,
                len(pair[0]["matchedEpisodes"]),
                pair[0]["priOrder"],
                pair[0]["seeders"],
            ),
            reverse=True,
        )
        entries = entries[:MAX_SEARCH_RESULTS]

        search_token = secrets.token_urlsafe(24)
        candidate_map: Dict[str, Any] = {}
        items: List[dict] = []
        for idx, (summary, ctx) in enumerate(entries):
            cid = str(idx)
            summary["candidateId"] = cid
            candidate_map[cid] = ctx
            items.append(summary)

        self._store_search_session(search_token, scope_key, media_kind, tmdb_id, season, candidate_map)
        msg = f"检索到 {len(items)} 条候选资源" if items else "未在已启用的站点中检索到可用资源"
        return {
            "success": True,
            "action": "SEARCHED",
            "searchToken": search_token,
            "totalCount": len(items),
            "items": items,
            "message": msg,
        }

    def trigger_download(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """直接调用 MoviePilot DownloadChain 推送下载：支持 SELECTED（选种）与 AUTO（自动搜源补全）"""
        with self._state_lock:
            self._validate_callback(authorization)
            state = self.get_data(STATE_KEY) or {}
            scope_key = (self._generation, state.get("connection_id"))
        if DownloadChain is None:
            return {"success": False, "action": "UNSUPPORTED", "mode": "AUTO", "downloadedCount": 0,
                    "downloadedTitles": [], "remainingEpisodes": [],
                    "message": "当前 MoviePilot 运行环境不支持直接下载链"}

        data = payload if isinstance(payload, dict) else {}
        mode = str(data.get("mode") or "AUTO").strip().upper()
        if mode not in ("AUTO", "SELECTED"):
            raise HTTPException(status_code=400, detail="mode 必须为 AUTO 或 SELECTED")
        username = str(data.get("username") or "").strip()[:64] or "YiYi Media"

        if mode == "SELECTED":
            search_token = str(data.get("searchToken") or "").strip()
            candidate_id = str(data.get("candidateId") or "").strip()
            if not search_token or not candidate_id:
                raise HTTPException(status_code=400, detail="SELECTED 模式必须提供 searchToken 与 candidateId")
            session = self._get_search_session(search_token, scope_key)
            if session is None:
                return {"success": False, "action": "SESSION_EXPIRED", "mode": "SELECTED",
                        "downloadedCount": 0, "downloadedTitles": [], "remainingEpisodes": [],
                        "message": "候选资源列表已过期，请重新搜索后再点击下载"}
            ctx = session["candidates"].get(candidate_id)
            if ctx is None:
                raise HTTPException(status_code=404, detail="指定的候选资源不存在")
            requested_eps = self._parse_episodes(data.get("episodes"))
            ep_set = set(requested_eps) if (session["media_kind"] == "TV" and requested_eps) else None
            try:
                raw_did = DownloadChain().download_single(
                    context=ctx,
                    episodes=ep_set,
                    username=username,
                    source="YiYi Media",
                )
            except Exception:
                return {"success": False, "action": "FAILED", "mode": "SELECTED",
                        "downloadedCount": 0, "downloadedTitles": [], "remainingEpisodes": requested_eps,
                        "message": "MoviePilot 提交下载任务失败，请检查下载器配置与日志"}
            did = raw_did[0] if isinstance(raw_did, tuple) else raw_did
            err_detail = raw_did[1] if (isinstance(raw_did, tuple) and len(raw_did) > 1) else ""
            torrent = getattr(ctx, "torrent_info", None)
            title = self._sanitize_text(getattr(torrent, "title", "") or "", 200) or "候选资源"
            if did:
                return {
                    "success": True,
                    "action": "DOWNLOADED",
                    "mode": "SELECTED",
                    "downloadedCount": 1,
                    "downloadedTitles": [title],
                    "remainingEpisodes": [],
                    "message": f"已推送至 MoviePilot 下载：{title}",
                }
            return {
                "success": False,
                "action": "FAILED",
                "mode": "SELECTED",
                "downloadedCount": 0,
                "downloadedTitles": [],
                "remainingEpisodes": requested_eps,
                "message": self._sanitize_message(err_detail) or "MoviePilot 未能添加该下载任务（可能已存在或下载器拒绝）",
            }

        # AUTO 模式：自动搜源 + 按缺失集择优推送下载
        if SearchChain is None:
            return {"success": False, "action": "UNSUPPORTED", "mode": "AUTO", "downloadedCount": 0,
                    "downloadedTitles": [], "remainingEpisodes": [],
                    "message": "当前 MoviePilot 运行环境不支持自动检索链"}
        media_kind = str(data.get("mediaType") or "TV").strip().upper()
        if media_kind not in ("MOVIE", "TV"):
            raise HTTPException(status_code=400, detail="不支持的媒体类型")
        tmdb_id = self._positive_int(data.get("tmdbId"), "tmdbId")
        season = self._positive_int(data.get("season"), "season") if media_kind == "TV" else None
        missing_episodes = self._parse_episodes(data.get("episodes"))
        total_episode = self._optional_int(data.get("totalEpisode"))
        mtype = MediaType.TV if media_kind == "TV" else MediaType.MOVIE

        try:
            search_chain = SearchChain()
            mediainfo = search_chain.recognize_media(mtype=mtype, tmdbid=tmdb_id) if hasattr(search_chain, "recognize_media") else None
            no_exists = self._build_no_exists(media_kind, tmdb_id, season, missing_episodes, total_episode, mediainfo=mediainfo)
            if mediainfo is not None and hasattr(search_chain, "process"):
                contexts = search_chain.process(mediainfo=mediainfo, no_exists=no_exists) or []
            elif hasattr(search_chain, "search_by_id"):
                contexts = search_chain.search_by_id(tmdbid=tmdb_id, mtype=mtype, season=season) or []
            else:
                contexts = []
            if not contexts:
                return {
                    "success": False,
                    "action": "NO_RESULTS",
                    "mode": "AUTO",
                    "downloadedCount": 0,
                    "downloadedTitles": [],
                    "remainingEpisodes": missing_episodes,
                    "message": "未检索到符合过滤规则的可用资源，建议改为订阅持续追更",
                }
            downloaded_list, lefts = DownloadChain().batch_download(
                contexts=contexts,
                no_exists=no_exists,
                username=username,
                source="YiYi Media",
            )
        except Exception:
            return {
                "success": False,
                "action": "FAILED",
                "mode": "AUTO",
                "downloadedCount": 0,
                "downloadedTitles": [],
                "remainingEpisodes": missing_episodes,
                "message": "MoviePilot 自动搜源下载执行异常，请查看 MoviePilot 日志",
            }

        titles = []
        for ctx in downloaded_list or []:
            torrent = getattr(ctx, "torrent_info", None)
            t_title = self._sanitize_text(getattr(torrent, "title", "") or "", 200)
            if t_title:
                titles.append(t_title)
        remaining = self._extract_remaining_episodes(lefts, tmdb_id, season)
        if titles:
            if remaining:
                msg = f"已推送 {len(titles)} 个下载任务，仍有 {len(remaining)} 集未找到资源，可改用订阅继续追更"
            else:
                msg = f"已自动推送 {len(titles)} 个下载任务至 MoviePilot"
            return {
                "success": True,
                "action": "DOWNLOADED",
                "mode": "AUTO",
                "downloadedCount": len(titles),
                "downloadedTitles": titles[:20],
                "remainingEpisodes": remaining,
                "message": msg,
            }
        return {
            "success": False,
            "action": "NO_MATCH",
            "mode": "AUTO",
            "downloadedCount": 0,
            "downloadedTitles": [],
            "remainingEpisodes": remaining if remaining else missing_episodes,
            "message": "已检索到候选资源但无符合缺失集或过滤优先级的项目，可改用手动选种或创建订阅",
        }

    def upload_torrent_download(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """接收 YiYi 审核通过的原始种子内容，绑定精准 TMDB ID 与季/集信息后交由 DownloadChain 下发下载器"""
        with self._state_lock:
            self._validate_callback(authorization)
        if DownloadChain is None:
            return {
                "success": False,
                "action": "UNSUPPORTED",
                "downloadHash": None,
                "downloader": None,
                "message": "当前 MoviePilot 运行环境不支持直接下载链",
            }

        data = payload if isinstance(payload, dict) else {}
        media_kind = str(data.get("mediaType") or "").strip().upper()
        if media_kind not in ("MOVIE", "TV"):
            raise HTTPException(status_code=400, detail="不支持的媒体类型")
        tmdb_id = self._positive_int(data.get("tmdbId"), "tmdbId")
        title = str(data.get("title") or "").strip()
        if not title or len(title) > 512:
            raise HTTPException(status_code=400, detail="媒体标题无效")
        season = self._positive_int(data.get("season"), "season") if media_kind == "TV" else None
        episodes = self._parse_episodes(data.get("episodes")) if media_kind == "TV" else []
        year_val = data.get("year")
        year = str(year_val).strip() if year_val not in (None, "") else ""
        if year and not re.fullmatch(r"[0-9]{4}", year):
            year = ""
        imdb_id = str(data.get("imdbId") or "").strip()[:32] or None
        torrent_name = self._sanitize_text(data.get("torrentName") or data.get("torrentFileName") or title, 300) or title
        username = str(data.get("username") or "").strip()[:64] or "YiYi Media"
        downloader = str(data.get("downloader") or "").strip()[:64] or None
        save_path = str(data.get("savePath") or "").strip()[:512] or None
        label = self._merge_torrent_labels(str(data.get("label") or "").strip()[:64] or None)
        total_size = int(float(data.get("totalSize") or 0)) if data.get("totalSize") not in (None, "") else 0

        raw_b64 = data.get("torrentBase64")
        if not isinstance(raw_b64, str) or not raw_b64.strip():
            raise HTTPException(status_code=400, detail="缺少种子文件内容")
        if len(raw_b64) > int(MAX_TORRENT_UPLOAD_BYTES * 1.4) + 1024:
            raise HTTPException(status_code=400, detail="种子文件体积超出限制（最大 10 MB）")
        try:
            torrent_bytes = base64.b64decode(raw_b64.strip(), validate=True)
        except Exception as exc:
            raise HTTPException(status_code=400, detail="种子文件 Base64 编码无效") from exc
        if not torrent_bytes or len(torrent_bytes) > MAX_TORRENT_UPLOAD_BYTES or not torrent_bytes.startswith(b"d"):
            raise HTTPException(status_code=400, detail="种子文件格式无效，仅支持标准 BitTorrent (.torrent) 文件")

        mtype = MediaType.TV if media_kind == "TV" else MediaType.MOVIE
        mediainfo = self._resolve_media_info_for_torrent(mtype, tmdb_id, title, year, imdb_id)
        meta = self._build_meta_for_torrent(torrent_name, mtype, tmdb_id, season, episodes, year)
        torrent_info = self._build_torrent_info(torrent_name, tmdb_id, total_size)
        ctx = self._build_context(meta, mediainfo, torrent_info)
        ep_set = set(episodes) if (media_kind == "TV" and episodes) else None

        try:
            download_chain = DownloadChain()
            call_kwargs = self._build_download_single_kwargs(
                download_chain.download_single,
                context=ctx,
                torrent_content=torrent_bytes,
                episodes=ep_set,
                username=username,
                source="YiYi Media",
                downloader=downloader,
                save_path=save_path,
                label=label,
            )
            raw_did = download_chain.download_single(**call_kwargs)
        except Exception:
            return {
                "success": False,
                "action": "FAILED",
                "downloadHash": None,
                "downloader": downloader,
                "message": "MoviePilot 提交种子下载任务失败，请检查下载器、下载目录配置与 MoviePilot 日志",
            }

        did = raw_did[0] if isinstance(raw_did, tuple) else raw_did
        err_detail = raw_did[1] if (isinstance(raw_did, tuple) and len(raw_did) > 1) else ""
        if did:
            resolved_downloader = downloader
            if not resolved_downloader and DownloadHistoryOper is not None:
                try:
                    hist = DownloadHistoryOper().get_by_hash(str(did))
                    if hist is not None and getattr(hist, "downloader", None):
                        resolved_downloader = str(getattr(hist, "downloader"))[:64]
                except Exception:
                    pass
            return {
                "success": True,
                "action": "DOWNLOADED",
                "downloadHash": str(did).strip()[:128],
                "downloader": resolved_downloader,
                "message": f"已推送种子至 MoviePilot 下载：{torrent_name}",
            }
        return {
            "success": False,
            "action": "FAILED",
            "downloadHash": None,
            "downloader": downloader,
            "message": self._sanitize_message(err_detail) or "MoviePilot 未能添加该种子下载任务（可能已存在或下载器拒绝）",
        }

    def query_downloads(self, payload: Optional[Dict[str, Any]], authorization: Optional[tuple] = None) -> dict:
        """批量查询种子下载任务的实时进度与下载器状态"""
        with self._state_lock:
            self._validate_callback(authorization)
        data = payload if isinstance(payload, dict) else {}
        raw_hashes = data.get("hashes")
        if not isinstance(raw_hashes, list) or len(raw_hashes) > 100:
            raise HTTPException(status_code=400, detail="hashes 必须是长度不超过 100 的数组")

        normalized_hashes: List[str] = []
        seen_hashes = set()
        for raw_h in raw_hashes:
            h_str = str(raw_h or "").strip()
            if not h_str or len(h_str) > 128:
                continue
            low = h_str.lower()
            if low not in seen_hashes:
                seen_hashes.add(low)
                normalized_hashes.append(h_str)

        active_map = self._query_live_torrents_by_hash(normalized_hashes)

        hist_oper = None
        if DownloadHistoryOper is not None:
            try:
                hist_oper = DownloadHistoryOper()
            except Exception:
                hist_oper = None

        done_states = {
            "completed", "seeding", "uploading", "finished",
            "stalledup", "pausedup", "queuedup", "forcedup",
        }
        items = []
        for h_str in normalized_hashes:
            key_lower = h_str.lower()
            active = active_map.get(key_lower)
            if active is not None:
                raw_prog = getattr(active, "progress", 0.0)
                try:
                    progress = max(0.0, min(100.0, float(raw_prog or 0.0)))
                except (TypeError, ValueError):
                    progress = 0.0
                state = self._sanitize_text(str(getattr(active, "state", "") or "downloading"), 32) or "downloading"
                dl_name = self._sanitize_text(str(getattr(active, "downloader", "") or ""), 64) or None
                is_active = state.lower() not in done_states and progress < 100.0
                items.append({
                    "downloadHash": h_str,
                    "exists": True,
                    "active": is_active,
                    "state": state,
                    "progress": round(progress, 1),
                    "downloader": dl_name,
                })
                continue

            hist = None
            if hist_oper is not None and hasattr(hist_oper, "get_by_hash"):
                try:
                    hist = hist_oper.get_by_hash(h_str)
                except Exception:
                    hist = None
            if hist is not None:
                items.append({
                    "downloadHash": h_str,
                    "exists": True,
                    "active": False,
                    "state": "FINISHED_OR_REMOVED",
                    "progress": 100.0,
                    "downloader": self._sanitize_text(str(getattr(hist, "downloader", "") or ""), 64) or None,
                })
            else:
                items.append({
                    "downloadHash": h_str,
                    "exists": False,
                    "active": False,
                    "state": "NOT_FOUND",
                    "progress": None,
                    "downloader": None,
                })
        return {"items": items}

    @staticmethod
    def _merge_torrent_labels(label: Optional[str]) -> Optional[str]:
        """自定义标签时一并保留 MoviePilot 内置 TORRENT_TAG，确保下载监控与自动整理正常命中"""
        custom_tags = [part.strip() for part in str(label or "").split(",") if part.strip()]
        if not custom_tags:
            return None
        default_tags = [
            part.strip()
            for part in str(getattr(settings, "TORRENT_TAG", "") or "").split(",")
            if part.strip()
        ]
        merged: List[str] = []
        for tag in default_tags + custom_tags:
            if tag not in merged:
                merged.append(tag)
        return ",".join(merged) if merged else None

    @staticmethod
    def _query_live_torrents_by_hash(hashes: List[str]) -> Dict[str, Any]:
        """优先按 Hash 跨所有下载器与标签实时查询种子进度，旧宿主自动回退 downloading()"""
        result: Dict[str, Any] = {}
        if not hashes or DownloadChain is None:
            return result
        try:
            dc = DownloadChain()
        except Exception:
            return result

        if hasattr(dc, "list_torrents"):
            try:
                list_fn = dc.list_torrents
                kwargs: Dict[str, Any] = {"hashs": hashes}
                try:
                    params = inspect.signature(list_fn).parameters
                    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
                    if accepts_var_kw or "include_all_tags" in params:
                        kwargs["include_all_tags"] = True
                except (TypeError, ValueError):
                    pass
                for item in list_fn(**kwargs) or []:
                    h = str(getattr(item, "hash", "") or "").strip().lower()
                    if h:
                        result[h] = item
            except Exception:
                pass

        if len(result) < len(hashes) and hasattr(dc, "downloading"):
            try:
                for item in dc.downloading() or []:
                    h = str(getattr(item, "hash", "") or "").strip().lower()
                    if h and h not in result:
                        result[h] = item
            except Exception:
                pass
        return result

    @staticmethod
    def _resolve_media_info_for_torrent(mtype: Any, tmdb_id: int, title: str,
                                        year: str, imdb_id: Optional[str]) -> Any:
        mediainfo = None
        for chain_cls in (MediaChain, SearchChain):
            if chain_cls is None:
                continue
            try:
                chain = chain_cls()
                if hasattr(chain, "recognize_media"):
                    mediainfo = chain.recognize_media(mtype=mtype, tmdbid=tmdb_id)
                    if mediainfo is not None:
                        break
            except Exception:
                continue
        if mediainfo is not None:
            try:
                mediainfo.tmdb_id = tmdb_id
                mediainfo.type = mtype
            except Exception:
                pass
            return mediainfo

        if MediaInfo is not None:
            try:
                fallback = MediaInfo()
                fallback.type = mtype
                fallback.tmdb_id = tmdb_id
                fallback.title = title
                fallback.year = year or None
                fallback.imdb_id = imdb_id
                return fallback
            except Exception:
                pass
        return SimpleNamespace(
            type=mtype,
            tmdb_id=tmdb_id,
            title=title,
            year=year or None,
            imdb_id=imdb_id,
            category=None,
            title_year=f"{title} ({year})" if year else title,
        )

    @staticmethod
    def _build_meta_for_torrent(torrent_name: str, mtype: Any, tmdb_id: int,
                                season: Optional[int], episodes: List[int], year: str) -> Any:
        if MetaInfo is not None:
            try:
                meta = MetaInfo(title=torrent_name)
            except Exception:
                meta = None
        else:
            meta = None

        if meta is None:
            season_str = f"S{season:02d}" if season else ""
            ep_str = ""
            if episodes:
                ep_str = f"E{min(episodes):02d}" if len(episodes) == 1 else f"E{min(episodes):02d}-E{max(episodes):02d}"
            meta = SimpleNamespace(
                org_string=torrent_name,
                title=torrent_name,
                type=mtype,
                year=year or None,
                tmdbid=tmdb_id,
                begin_season=season,
                end_season=None,
                begin_episode=min(episodes) if episodes else None,
                end_episode=max(episodes) if len(episodes) > 1 else None,
                episode_list=list(episodes),
                season=season_str,
                episode=ep_str,
                season_episode=f"{season_str}{ep_str}",
            )
            return meta

        for attr, val in (
            ("type", mtype),
            ("tmdbid", tmdb_id),
            ("year", year or getattr(meta, "year", None)),
        ):
            try:
                setattr(meta, attr, val)
            except Exception:
                pass
        if mtype == MediaType.TV:
            try:
                meta.begin_season = season
                meta.end_season = None
                if episodes:
                    meta.begin_episode = min(episodes)
                    meta.end_episode = max(episodes) if len(episodes) > 1 else None
                else:
                    meta.begin_episode = None
                    meta.end_episode = None
            except Exception:
                pass
        else:
            try:
                meta.begin_season = None
                meta.end_season = None
                meta.begin_episode = None
                meta.end_episode = None
            except Exception:
                pass
        return meta

    @staticmethod
    def _build_torrent_info(torrent_name: str, tmdb_id: int, total_size: int) -> Any:
        if TorrentInfo is not None:
            try:
                return TorrentInfo(
                    site_name="YiYi用户投递",
                    title=torrent_name,
                    description=f"YiYi 用户上传种子 (TMDB:{tmdb_id})",
                    size=float(max(0, total_size)),
                    enclosure="",
                )
            except Exception:
                pass
        return SimpleNamespace(
            site_name="YiYi用户投递",
            site_cookie=None,
            site_downloader=None,
            title=torrent_name,
            description=f"YiYi 用户上传种子 (TMDB:{tmdb_id})",
            size=float(max(0, total_size)),
            enclosure="",
        )

    @staticmethod
    def _build_context(meta: Any, mediainfo: Any, torrent_info: Any) -> Any:
        if Context is not None:
            try:
                return Context(meta_info=meta, media_info=mediainfo, torrent_info=torrent_info)
            except Exception:
                pass
        return SimpleNamespace(meta_info=meta, media_info=mediainfo, torrent_info=torrent_info)

    @staticmethod
    def _build_download_single_kwargs(func: Any, **candidates: Any) -> Dict[str, Any]:
        """按宿主 MoviePilot 版本动态过滤可选参数，兼容新旧版 DownloadChain.download_single"""
        desired = {**candidates, "return_detail": True}
        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError):
            return {k: v for k, v in candidates.items() if v is not None or k in ("context", "torrent_content", "episodes")}
        params = sig.parameters
        accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        if accepts_var_kw:
            return {k: v for k, v in desired.items() if v is not None or k in ("context", "torrent_content", "episodes")}
        out: Dict[str, Any] = {}
        for k, v in desired.items():
            if k in params and (v is not None or k in ("context", "torrent_content", "episodes")):
                out[k] = v
        return out

    def _search_contexts(self, mtype: Any, tmdb_id: int, season: Optional[int],
                         missing_episodes: Optional[List[int]], total_episode: Optional[int],
                         no_exists: Optional[dict] = None) -> List[Any]:
        chain = SearchChain()
        if no_exists is not None and hasattr(chain, "recognize_media") and hasattr(chain, "process"):
            mediainfo = chain.recognize_media(mtype=mtype, tmdbid=tmdb_id)
            if mediainfo is not None:
                return chain.process(mediainfo=mediainfo, no_exists=no_exists) or []
        if hasattr(chain, "search_by_id"):
            return chain.search_by_id(tmdbid=tmdb_id, mtype=mtype, season=season) or []
        if hasattr(chain, "recognize_media") and hasattr(chain, "process"):
            mediainfo = chain.recognize_media(mtype=mtype, tmdbid=tmdb_id)
            if mediainfo is not None:
                return chain.process(mediainfo=mediainfo, no_exists=no_exists) or []
        return []

    @staticmethod
    def _build_no_exists(media_kind: str, tmdb_id: int, season: Optional[int],
                         missing_episodes: List[int], total_episode: Optional[int],
                         mediainfo: Any = None) -> Optional[dict]:
        if media_kind != "TV" or season is None:
            return None
        effective_total = total_episode if (total_episode and total_episode > 0) else (
            max(missing_episodes) if missing_episodes else 0
        )
        start_ep = min(missing_episodes) if missing_episodes else 1
        if NotExistMediaInfo is not None:
            info = NotExistMediaInfo(
                season=season,
                episodes=list(missing_episodes),
                total_episode=effective_total,
                start_episode=start_ep,
            )
        else:  # pragma: no cover
            from types import SimpleNamespace
            info = SimpleNamespace(
                season=season,
                episodes=list(missing_episodes),
                total_episode=effective_total,
                start_episode=start_ep,
            )
        media_key: Any = tmdb_id
        if mediainfo is not None and build_media_key is not None and resolve_media_identity is not None:
            try:
                src, mid = resolve_media_identity(media=mediainfo)
                media_key = build_media_key(src, mid)
            except Exception:
                media_key = tmdb_id
        return {media_key: {season: info}}

    @classmethod
    def _extract_remaining_episodes(cls, lefts: Any, tmdb_id: int, season: Optional[int]) -> List[int]:
        if not isinstance(lefts, dict) or season is None:
            return []
        season_map = None
        for candidate_key in (tmdb_id, str(tmdb_id), f"tmdb:{tmdb_id}"):
            if candidate_key in lefts:
                season_map = lefts[candidate_key]
                break
        if season_map is None and len(lefts) == 1:
            season_map = next(iter(lefts.values()))
        if not isinstance(season_map, dict):
            return []
        ne = season_map.get(season)
        if ne is None:
            return []
        return cls._parse_episodes(getattr(ne, "episodes", None))

    def _store_search_session(self, token: str, scope_key: tuple, media_kind: str,
                              tmdb_id: int, season: Optional[int], candidates: Dict[str, Any]):
        now = datetime.now(timezone.utc)
        with self._state_lock:
            self._prune_search_cache_locked(now)
            while len(self._search_cache) >= MAX_SEARCH_SESSIONS:
                oldest = next(iter(self._search_cache))
                self._search_cache.pop(oldest, None)
            self._search_cache[token] = {
                "created_at": now,
                "scope": scope_key,
                "media_kind": media_kind,
                "tmdb_id": tmdb_id,
                "season": season,
                "candidates": candidates,
            }

    def _get_search_session(self, token: str, scope_key: tuple) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        with self._state_lock:
            self._prune_search_cache_locked(now)
            entry = self._search_cache.get(token)
            if entry is None or entry.get("scope") != scope_key:
                return None
            return entry

    def _prune_search_cache_locked(self, now: datetime):
        cutoff = now - timedelta(seconds=SEARCH_SESSION_TTL_SECONDS)
        expired = [tok for tok, item in self._search_cache.items() if item.get("created_at", now) < cutoff]
        for tok in expired:
            self._search_cache.pop(tok, None)

    @classmethod
    def _parse_episodes(cls, raw: Any) -> List[int]:
        if not raw or not isinstance(raw, (list, tuple, set)):
            return []
        result = set()
        for item in raw:
            val = cls._optional_int(item)
            if val is not None and val > 0:
                result.add(val)
            if len(result) >= 500:
                break
        return sorted(result)

    @staticmethod
    def _sanitize_text(raw: Any, max_len: int) -> str:
        if not isinstance(raw, str):
            return ""
        cleaned = re.sub(r"https?://\S+", "[url]", raw).strip()
        return cleaned[:max_len]

    @eventmanager.register([EventType.SubscribeComplete, EventType.SubscribeModified, EventType.SubscribeDeleted])
    def on_subscribe_event(self, event: Event):
        """监听 MoviePilot 订阅变更与完成事件并异步通知 YiYi"""
        if not self._enabled or event is None:
            return
        with self._state_lock:
            state = self.get_data(STATE_KEY) or {}
            connection_id = state.get("connection_id")
            yiyi_url = state.get("yiyi_url")
            access_key = state.get("access_key")
            tracked_map = self.get_data(SUBSCRIPTIONS_KEY) or {}
        if not connection_id or not yiyi_url or not access_key:
            return

        event_data = getattr(event, "event_data", None) or {}
        if not isinstance(event_data, dict):
            return
        sub_info = event_data.get("subscribe_info")
        info = sub_info if isinstance(sub_info, dict) else {}
        raw_sub_id = event_data.get("subscribe_id") or info.get("id")
        sub_id = self._optional_int(raw_sub_id)
        tracked = tracked_map.get(str(sub_id)) if sub_id is not None else None

        tmdb_id = self._optional_int(info.get("tmdbid") or (tracked or {}).get("tmdbId"))
        if sub_id is None and tmdb_id is None:
            return

        raw_type = str(info.get("type") or (tracked or {}).get("mediaType") or "").strip()
        media_kind = "TV" if raw_type in ("电视剧", "TV", "Series", "tv", str(getattr(MediaType, "TV", "TV"))) else "MOVIE"
        season = self._optional_int(info.get("season") if "season" in info else (tracked or {}).get("season"))

        etype = getattr(event, "event_type", None)
        if etype == EventType.SubscribeComplete:
            normalized_event = "COMPLETED"
            sub_state = "COMPLETED"
            lack_episode = 0
            if sub_id is not None:
                self._forget_subscription(sub_id)
        elif etype == EventType.SubscribeDeleted:
            normalized_event = "DELETED"
            sub_state = "DELETED"
            lack_episode = self._optional_int(info.get("lack_episode"))
            if sub_id is not None:
                self._forget_subscription(sub_id)
        else:
            normalized_event = "UPDATED"
            sub_state = str(info.get("state") or "R")
            lack_episode = self._optional_int(info.get("lack_episode"))

        body = {
            "eventType": normalized_event,
            "mpSubscribeId": sub_id,
            "requestId": (tracked or {}).get("requestId"),
            "mediaType": media_kind,
            "tmdbId": tmdb_id,
            "season": season if media_kind == "TV" else 0,
            "state": sub_state,
            "totalEpisode": self._optional_int(info.get("total_episode")),
            "lackEpisode": lack_episode,
        }
        try:
            self._request(
                "POST",
                f"{yiyi_url}{API_BASE}/client/{connection_id}/subscribes/events",
                key=access_key,
                payload=body,
            )
        except Exception:
            # 事件回推失败不阻塞宿主事件总线；YiYi 对账接口可兜底同步状态。
            pass

    def _remember_subscription(self, sub_id: int, request_id: Optional[int], media_kind: str,
                               tmdb_id: int, season: Optional[int]):
        with self._state_lock:
            tracked = self.get_data(SUBSCRIPTIONS_KEY) or {}
            if len(tracked) >= MAX_TRACKED_SUBSCRIPTIONS and str(sub_id) not in tracked:
                oldest_key = next(iter(tracked))
                tracked.pop(oldest_key, None)
            tracked[str(sub_id)] = {
                "requestId": request_id,
                "mediaType": media_kind,
                "tmdbId": tmdb_id,
                "season": season if media_kind == "TV" else 0,
            }
            self.save_data(SUBSCRIPTIONS_KEY, tracked)

    def _forget_subscription(self, sub_id: int):
        with self._state_lock:
            tracked = self.get_data(SUBSCRIPTIONS_KEY) or {}
            if str(sub_id) in tracked:
                tracked.pop(str(sub_id), None)
                self.save_data(SUBSCRIPTIONS_KEY, tracked)

    @classmethod
    def _find_existing_subscribe(cls, oper: Any, media_kind: str, tmdb_id: int, season: Optional[int]) -> Any:
        rows = oper.list() if hasattr(oper, "list") else []
        for item in rows or []:
            item_tmdb = cls._optional_int(getattr(item, "tmdbid", None))
            if item_tmdb != tmdb_id:
                continue
            raw_type = getattr(item, "type", None)
            is_tv = raw_type in (
                getattr(MediaType, "TV", None),
                getattr(getattr(MediaType, "TV", None), "value", None),
                "电视剧", "TV", "tv",
            )
            if (media_kind == "TV") != is_tv:
                continue
            if media_kind == "TV":
                item_season = cls._optional_int(getattr(item, "season", None))
                if item_season != season:
                    continue
            return item
        return None

    @staticmethod
    def _positive_int(value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise HTTPException(status_code=400, detail=f"{field_name} 必须是正整数")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} 必须是正整数") from exc
        if parsed <= 0:
            raise HTTPException(status_code=400, detail=f"{field_name} 必须是正整数")
        return parsed

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool) or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sanitize_message(raw: Any) -> str:
        if not isinstance(raw, str):
            return ""
        cleaned = re.sub(r"https?://\S+", "[url]", raw).strip()
        if any(secret_word in cleaned.lower() for secret_word in ("token", "apikey", "api_key", "secret", "password")):
            return "MoviePilot 处理订阅未成功，请在 MoviePilot 日志查看详情"
        return cleaned[:240]

    def _validate_callback(self, authorization: Optional[tuple]):
        state = self.get_data(STATE_KEY) or {}
        if authorization is not None and authorization != (self._generation, state.get("connection_id")):
            raise HTTPException(status_code=401, detail="连接已变更，请重新验证")

    def synchronize(self):
        """配对、恢复媒体服务器配置并验证双向网络，失败时保留可重试状态"""
        if not self._lock.acquire(blocking=False):
            return
        try:
            generation = self._generation
            if self._config.get("disconnect"):
                self._disconnect_remote()
                return
            if not self._enabled:
                return
            state = self.get_data(STATE_KEY) or {}
            base = self._normalize_url(self._config.get("yiyi_url", ""))
            if state.get("yiyi_url") and state["yiyi_url"] != base:
                raise IntegrationError("请先解除旧绑定，再修改 YiYi 地址")
            if not state.get("connection_id"):
                state = self._pair(base, state, generation)
            if not self._current(generation):
                return
            status = self._request("GET", base + API_BASE + "/client/" + state["connection_id"] + "/status",
                                   key=state["access_key"])
            if status is None:
                raise IntegrationError("YiYi 暂时无法连接，稍后自动重试")
            if status.status_code in (401, 403):
                if self._current(generation):
                    self._clear_binding("YiYi 已撤销授权或授权管理员不可用，请重新配对")
                return
            self._require_ok(status)
            callback = self._normalize_url(self._config.get("callback_url") or settings.APP_DOMAIN or state["callback_url"])
            if callback != state["callback_url"]:
                updated = self._request("POST", base + API_BASE + "/client/" + state["connection_id"] + "/callback",
                                        key=state["access_key"], payload={"callbackUrl": callback})
                self._require_ok(updated)
                with self._state_lock:
                    if not self._current(generation):
                        return
                    state["callback_url"] = callback
                    self.save_data(STATE_KEY, state)
            # 先用查询入口确认协议可用，避免创建一个实际无法使用的服务器。
            server_url = base + API_BASE + "/server/" + state["connection_id"]
            info = self._request("GET", server_url + "/emby/System/Info", key=state["access_key"])
            self._require_ok(info)
            if not info.json().get("Id"):
                raise IntegrationError("YiYi Emby 查询入口返回无效服务器信息")
            # 直接读取系统名称；Emby 协议名称可能带后缀或独立覆盖配置。
            branding = self._request("GET", base + "/api/auth/branding")
            self._require_ok(branding)
            name = branding.json().get("systemName")
            if not isinstance(name, str) or not name.strip():
                raise IntegrationError("未能获取 YiYi 系统名称，稍后自动重试")
            if not self._current(generation):
                return
            with self._state_lock:
                if not self._current(generation):
                    return
                self._upsert_server(state, server_url, name.strip())
            result = self._request("POST", base + API_BASE + "/client/" + state["connection_id"] + "/activate",
                                   key=state["access_key"])
            self._require_ok(result)
            if not self._current(generation):
                return
            self._set_message("双向连接成功" if result.json().get("success") else
                              "媒体服务器已添加，YiYi 尚不能访问 MoviePilot，请检查回调地址和网络")
        except IntegrationError as error:
            if self._current(generation):
                self._set_message(str(error))
        except (ValueError, TypeError, KeyError):
            if self._current(generation):
                self._set_message("远端响应格式无效，请检查地址及 YiYi 版本")
        except Exception:  # 第三方服务与运行环境异常不能把密钥或请求体写入日志。
            if self._current(generation):
                self._set_message("连接暂时不可用，稍后自动重试")
        finally:
            self._lock.release()

    def _current(self, generation: int) -> bool:
        return self._enabled and self._generation == generation

    def _pair(self, base: str, state: dict, generation: int) -> dict:
        code = str(self._config.get("pairing_code") or "").strip()
        if not code:
            raise IntegrationError("请填写 YiYi 管理员生成的配对码")
        callback = self._normalize_url(self._config.get("callback_url") or settings.APP_DOMAIN)
        code_hash = hashlib.sha256(code.encode()).hexdigest()
        if (not state.get("access_key") or state.get("pairing_hash") != code_hash
                or state.get("callback_url") != callback):
            state = {"instance_id": self.get_data("instance_id") or str(uuid.uuid4()),
                     "yiyi_url": base, "callback_url": callback, "pairing_hash": code_hash,
                     "access_key": secrets.token_urlsafe(32), "callback_key": secrets.token_urlsafe(32)}
            with self._state_lock:
                if not self._current(generation):
                    raise IntegrationError("配置已变更")
                self.save_data("instance_id", state["instance_id"])
                # 先落盘，再发交换请求；丢失响应时仍能携带同一组凭据幂等重试。
                self.save_data(STATE_KEY, state)
        response = self._request("POST", base + API_BASE + "/exchange", payload={
            "pairingCode": code, "instanceId": state["instance_id"], "callbackUrl": state["callback_url"],
            "callbackKey": state["callback_key"], "accessKey": state["access_key"],
        })
        self._require_ok(response)
        data = response.json()
        connection_id = data.get("connectionId", "")
        if not re.fullmatch(r"[a-f0-9-]{36}", connection_id) or data.get("protocolVersion") not in (1, 2, 3, 4):
            raise IntegrationError("不支持的配对协议")
        with self._state_lock:
            if not self._current(generation):
                raise IntegrationError("配置已变更")
            state["connection_id"] = connection_id
            self.save_data(STATE_KEY, state)
            self._config["pairing_code"] = ""
            self.update_config(self._config)
        return state

    def _upsert_server(self, state: dict, server_url: str, name: str):
        servers = copy.deepcopy(self.systemconfig.get(SystemConfigKey.MediaServers) or [])
        marker = state["instance_id"]
        managed = [s for s in servers if s.get("config", {}).get("yiyi_instance_id") == marker]
        other_names = {s.get("name") for s in servers if s.get("config", {}).get("yiyi_instance_id") != marker}
        if name in other_names:
            name += " (" + marker[:8] + ")"
        if name in other_names:
            raise IntegrationError("自动生成的媒体服务器名称已被占用，请修改同名的其它媒体服务器名称")
        server = copy.deepcopy(managed[0]) if managed else {}
        server.update({"name": name, "type": "emby", "enabled": True})
        server["config"] = {**server.get("config", {}), "host": server_url,
                            "apikey": state["access_key"], "yiyi_instance_id": marker}
        updated = [s for s in servers if s.get("config", {}).get("yiyi_instance_id") != marker] + [server]
        if updated != servers:
            self._save_servers(updated)

    def _disable_server(self):
        marker = self.get_data("instance_id")
        if not marker:
            return
        servers = copy.deepcopy(self.systemconfig.get(SystemConfigKey.MediaServers) or [])
        changed = False
        for server in servers:
            if server.get("config", {}).get("yiyi_instance_id") == marker:
                if server.get("enabled") or server.get("config", {}).get("apikey"):
                    server["enabled"] = False
                    server["config"].pop("apikey", None)
                    changed = True
        if changed:
            self._save_servers(servers)

    def _save_servers(self, servers: list):
        result = self.systemconfig.set(SystemConfigKey.MediaServers, servers)
        if result is False:
            raise IntegrationError("媒体服务器配置保存失败")
        self.eventmanager.send_event(EventType.ConfigChanged, ConfigChangeEventData(
            key={SystemConfigKey.MediaServers.value}, value=servers, change_type="update"))

    def _disconnect_remote(self):
        state = self.get_data(STATE_KEY) or {}
        remote_ok = True
        if state.get("connection_id"):
            response = self._request("POST", state["yiyi_url"] + API_BASE + "/client/" + state["connection_id"]
                                     + "/disconnect", key=state["access_key"])
            remote_ok = response is not None and response.status_code in (200, 401)
        self._enabled = False
        self._clear_binding("双方授权已解除" if remote_ok else "本地已解除绑定；YiYi 不可达，请在 YiYi 管理页撤销旧连接")

    def _clear_binding(self, message: str):
        with self._state_lock:
            self._enabled = False
            self._generation += 1
            self._search_cache.clear()
            self.save_data(STATE_KEY, {})
            self.save_data(SUBSCRIPTIONS_KEY, {})
            self._config.update({"enabled": False, "disconnect": False, "pairing_code": ""})
            self.update_config(self._config)
            self._disable_server()
            self._set_message(message)

    def _set_message(self, message: str):
        if self.get_data("status") != message:
            self.save_data("status", message)
            logger.info(f"YiYi Media：{message}")

    @staticmethod
    def _normalize_url(raw: str) -> str:
        parsed = urlsplit(str(raw or "").strip())
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or "%" in parsed.netloc
                or any(c.isspace() for c in str(raw))):
            raise IntegrationError("请填写完整的 HTTP(S) 站点地址（回调地址留空时须配置 APP_DOMAIN）")
        # 启用端口校验，但保留反向代理下的路径前缀。
        _ = parsed.port
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))

    @staticmethod
    def _request(method: str, url: str, key: Optional[str] = None, payload: Optional[dict] = None):
        headers = {"X-YiYi-Integration-Key": key} if key else {}
        client = RequestUtils(timeout=8)
        if method == "GET":
            return client.get_res(url=url, headers=headers, allow_redirects=False, verify=True)
        return client.post_res(url=url, json=payload, headers=headers, allow_redirects=False, verify=True)

    @staticmethod
    def _require_ok(response):
        if response is None or response.status_code != 200:
            status = response.status_code if response is not None else "不可达"
            raise IntegrationError(f"远端请求未成功（{status}），请检查地址、配对码及网络")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """分组展示连接配置，仅返回脱敏状态和用户输入字段"""
        status = self.get_data("status") or "尚未配对"
        paired = bool((self.get_data(STATE_KEY) or {}).get("connection_id"))
        connected = status == "双向连接成功"
        field_props = {"variant": "outlined", "density": "comfortable", "persistent-hint": True}
        return [
            {"component": "VForm", "content": [
                {"component": "VAlert", "props": {
                    "type": "success" if connected else "info", "variant": "tonal", "density": "compact",
                    "class": "mb-5 rounded-lg", "title": status,
                    "text": "媒体服务器与求片订阅通道均已就绪，每分钟检查连接。" if connected else
                            "首次连接：在 YiYi 生成配对码，填写下方地址和配对码，启用后保存。",
                }},
                {"component": "VSwitch", "props": {
                    "model": "enabled", "label": "启用插件", "color": "primary", "inset": True,
                    "hide-details": True, "class": "mb-4",
                }},
                {"component": "VCard", "props": {
                    "title": "连接设置", "variant": "flat", "class": "mb-5 rounded-lg border",
                }, "content": [{"component": "VCardText", "props": {"class": "pt-4"}, "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                **field_props, "model": "yiyi_url", "label": "YiYi 站点地址",
                                "placeholder": "https://media.example.com",
                                "hint": "填写 YiYi 管理站点入口，无需添加 API 路径。",
                            }},
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                **field_props, "model": "pairing_code", "label": "一次性配对码",
                                "type": "password", "autocomplete": "new-password",
                                "placeholder": "已配对，无需再次填写" if paired else "填写 YiYi 生成的配对码",
                                "hint": "已配对，保持留空即可；重新配对时才需填写。" if paired else
                                        "在 YiYi 的 MoviePilot 集成页生成，5 分钟内有效。",
                            }},
                        ]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextField", "props": {
                                **field_props, "model": "callback_url", "label": "MoviePilot 回调地址（选填）",
                                "placeholder": "https://moviepilot.example.com",
                                "hint": "填写 YiYi 能访问的 MoviePilot 入口，留空使用已配置的站点域名。",
                            }},
                        ]},
                    ]},
                    {"component": "VAlert", "props": {
                        "type": "info", "variant": "tonal", "density": "compact", "class": "mt-3",
                        "text": "媒体服务器名称自动读取 YiYi 的「系统名称」；管理员确认的求片订阅也将通过本连接自动创建。",
                    }},
                ]}]},
                {"component": "VExpansionPanels", "props": {"variant": "accordion"}, "content": [
                    {"component": "VExpansionPanel", "props": {
                        "title": "连接管理 · 解除绑定", "elevation": 0, "class": "border rounded-lg",
                    }, "content": [{"component": "VExpansionPanelText", "content": [
                        {"component": "VAlert", "props": {
                            "type": "warning", "variant": "tonal", "density": "compact", "class": "mb-3",
                            "text": "解除后将撤销双方授权，并停用插件创建的媒体服务器。重新连接需要新的配对码。",
                        }},
                        {"component": "VSwitch", "props": {
                            "model": "disconnect", "label": "解除双方绑定（保存后执行）", "color": "error",
                            "inset": True, "hide-details": True,
                        }},
                    ]}]},
                ]},
            ]},
        ], {"enabled": False, "yiyi_url": "", "pairing_code": "", "callback_url": "", "disconnect": False}

    def get_page(self) -> List[dict]:
        """展示当前连接状态"""
        return [{"component": "VAlert", "props": {"type": "info", "text": self.get_data("status") or "尚未配对"}}]

    def stop_service(self):
        """停止后台检查，保留授权供下次启用或重启恢复"""
        with self._state_lock:
            self._enabled = False
            self._generation += 1
            self._search_cache.clear()
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
