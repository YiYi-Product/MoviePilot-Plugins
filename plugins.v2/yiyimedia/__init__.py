"""YiYi Media 与 MoviePilot 的双向配对插件"""

import copy
import hashlib
import hmac
import re
import secrets
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Depends, Header, HTTPException

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import ConfigChangeEventData
from app.schemas.types import EventType, SystemConfigKey
from app.utils.http import RequestUtils

API_BASE = "/api/media/integrations/moviepilot"
STATE_KEY = "connection"


class IntegrationError(ValueError):
    """可直接展示给管理员的固定错误信息"""


class YiYiMedia(_PluginBase):
    """管理独立集成凭据和插件创建的媒体服务器，不改动其它服务器"""

    plugin_name = "YiYi Media"
    plugin_desc = "双向配对授权并自动添加 YiYi Media 媒体服务器。"
    plugin_icon = "Emby_A.png"
    plugin_version = "1.0.0"
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

        return [
            {"path": "/ping", "endpoint": ping_callback, "methods": ["GET"],
             "summary": "YiYi 双向连通验证", "allow_anonymous": True},
            {"path": "/disconnect", "endpoint": disconnect_callback, "methods": ["POST"],
             "summary": "YiYi 解除配对", "allow_anonymous": True},
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
        """只返回连接标识，不返回配置或凭据"""
        with self._state_lock:
            self._validate_callback(authorization)
            state = self.get_data(STATE_KEY) or {}
            return {"instanceId": state.get("instance_id"), "connectionId": state.get("connection_id"),
                    "protocolVersion": 1}

    def disconnect(self, authorization: Optional[tuple] = None) -> dict:
        """先让在途同步无法恢复旧授权，再清理插件管理的服务器"""
        # 回调可能发生在 synchronize 等待 YiYi 响应时，不能在此获取同步锁。
        with self._state_lock:
            self._validate_callback(authorization)
            self._clear_binding("双方授权已解除")
            return {"success": True}

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
            if not self._current(generation):
                return
            with self._state_lock:
                if not self._current(generation):
                    return
                self._upsert_server(state, server_url)
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
        if not re.fullmatch(r"[a-f0-9-]{36}", connection_id) or data.get("protocolVersion") != 1:
            raise IntegrationError("不支持的配对协议")
        with self._state_lock:
            if not self._current(generation):
                raise IntegrationError("配置已变更")
            state["connection_id"] = connection_id
            self.save_data(STATE_KEY, state)
            self._config["pairing_code"] = ""
            self.update_config(self._config)
        return state

    def _upsert_server(self, state: dict, server_url: str):
        servers = copy.deepcopy(self.systemconfig.get(SystemConfigKey.MediaServers) or [])
        marker = state["instance_id"]
        managed = [s for s in servers if s.get("config", {}).get("yiyi_instance_id") == marker]
        name = managed[0]["name"] if managed else str(self._config.get("server_name") or "YiYi Media").strip()
        if not name:
            name = "YiYi Media"
        if not managed and any(s.get("name") == name for s in servers):
            name += " (" + marker[:8] + ")"
        if not managed and any(s.get("name") == name for s in servers):
            raise IntegrationError("媒体服务器名称冲突，请修改插件中的名称")
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
            self.save_data(STATE_KEY, {})
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
        """仅展示用户输入和脱敏状态，不回填持久化的集成凭据"""
        return [
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text":
                "在 YiYi 系统设置生成配对码，再填写地址并保存。MoviePilot 回调地址须能被 YiYi 访问；"
                "留空使用 APP_DOMAIN。配对后自动添加 Emby 媒体服务器，每分钟检查连接。"}},
            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}},
            {"component": "VTextField", "props": {"model": "yiyi_url", "label": "YiYi 站点地址"}},
            {"component": "VTextField", "props": {"model": "pairing_code", "label": "一次性配对码（5分钟有效）", "type": "password"}},
            {"component": "VTextField", "props": {"model": "callback_url", "label": "MoviePilot 回调地址（选填）"}},
            {"component": "VTextField", "props": {"model": "server_name", "label": "媒体服务器名称"}},
            {"component": "VSwitch", "props": {"model": "disconnect", "label": "解除双方绑定（保存后执行）"}},
            {"component": "VAlert", "props": {"type": "info", "text": self.get_data("status") or "尚未配对"}},
        ], {"enabled": False, "yiyi_url": "", "pairing_code": "", "callback_url": "", "server_name": "YiYi Media", "disconnect": False}

    def get_page(self) -> List[dict]:
        """展示当前连接状态"""
        return [{"component": "VAlert", "props": {"type": "info", "text": self.get_data("status") or "尚未配对"}}]

    def stop_service(self):
        """停止后台检查，保留授权供下次启用或重启恢复"""
        with self._state_lock:
            self._enabled = False
            self._generation += 1
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
