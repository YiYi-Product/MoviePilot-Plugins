"""验证配对恢复、配置隔离、专属鉴权及撤销竞争"""

import copy
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

CONNECTION_ID = "12345678-1234-4234-8234-123456789012"


def response(body=None, status=200):
    """构造独立 HTTP 响应替身"""
    return SimpleNamespace(status_code=status, json=lambda: body)


def successful_request(method, url, key=None, payload=None):
    """重放正常的配对和检查响应"""
    if url.endswith("/exchange"):
        return response({"connectionId": CONNECTION_ID, "protocolVersion": 1})
    if url.endswith("/Info"):
        return response({"Id": "yiyi-server-1"})
    return response({"success": True})


def install_request(monkeypatch, plugin):
    """记录网络边界并使用本地固定响应"""
    calls = []

    def request(method, url, key=None, payload=None):
        calls.append((method, url, key, copy.deepcopy(payload)))
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    return calls


def test_pair_creates_one_server_and_survives_restart(plugin, plugin_module, monkeypatch):
    """重启不重新配对，保留已有服务器与用户设置"""
    original = {"name": "Other Emby", "type": "emby", "enabled": True, "config": {"apikey": "other-test-key"}}
    plugin.servers = [copy.deepcopy(original)]
    calls = install_request(monkeypatch, plugin)
    plugin.synchronize()
    state = plugin.get_data("connection")
    assert state["connection_id"] == CONNECTION_ID
    assert plugin.config["pairing_code"] == ""
    assert plugin.servers[0] == original
    assert plugin.servers[1]["config"]["host"].endswith("/server/" + CONNECTION_ID)
    assert plugin.servers[1]["config"]["apikey"] == state["access_key"]
    assert plugin.get_data("status") == "双向连接成功"
    assert len(plugin.events) == 1
    plugin.servers[1]["sync_libraries"] = ["library-7"]
    restarted = plugin_module.YiYiMedia()
    restarted.data = copy.deepcopy(plugin.data)
    restarted.servers = copy.deepcopy(plugin.servers)
    restarted._config = copy.deepcopy(plugin.config)
    restarted._enabled = True
    restart_calls = install_request(monkeypatch, restarted)
    restarted.synchronize()
    assert all(not call[1].endswith("/exchange") for call in restart_calls)
    assert len(restarted.servers) == 2
    assert restarted.servers[1]["sync_libraries"] == ["library-7"]
    assert restarted.events == []
    assert len([call for call in calls if call[1].endswith("/exchange")]) == 1


def test_lost_exchange_response_reuses_persisted_keys(plugin, monkeypatch):
    """交换响应丢失后重用同一组随机凭据"""
    attempts = []

    def request(method, url, key=None, payload=None):
        if url.endswith("/exchange"):
            attempts.append(copy.deepcopy(payload))
            if len(attempts) == 1:
                return None
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    plugin.synchronize()
    assert not plugin.servers
    plugin.synchronize()
    assert attempts[0] == attempts[1]
    assert plugin.get_data("status") == "双向连接成功"


def test_name_collision_does_not_replace_user_server(plugin, monkeypatch):
    """默认名称被占用时新增独立名称"""
    existing = {"name": "YiYi Media", "type": "emby", "enabled": True, "config": {"host": "https://other.example.com"}}
    plugin.servers = [copy.deepcopy(existing)]
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    assert plugin.servers[0] == existing
    assert plugin.servers[1]["name"].startswith("YiYi Media (")


def test_revoked_authorization_disables_only_managed_server(plugin, monkeypatch):
    """撤销后清除双方凭据，保留其他服务器"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    plugin.servers.insert(0, {"name": "Other", "enabled": True, "config": {"apikey": "other-test-key"}})
    monkeypatch.setattr(plugin, "_request", lambda *args, **kwargs: response(status=401))
    plugin.synchronize()
    assert plugin.get_data("connection") == {}
    assert plugin.servers[0]["enabled"]
    assert not plugin.servers[1]["enabled"]
    assert "apikey" not in plugin.servers[1]["config"]
    assert not plugin.config["enabled"]


@pytest.mark.parametrize("during", ["/status", "/Info", "/activate"])
def test_disconnect_during_sync_cannot_restore_old_keys(plugin, monkeypatch, during):
    """检查期间收到解除回调时，在途任务不能重新启用服务器"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()

    def request(method, url, key=None, payload=None):
        if url.endswith(during):
            plugin.disconnect()
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    plugin.synchronize()
    assert plugin.get_data("connection") == {}
    assert all(not server["enabled"] and "apikey" not in server["config"] for server in plugin.servers)
    assert plugin.get_data("status") == "双方授权已解除"


def test_disconnect_during_exchange_does_not_persist_response(plugin, monkeypatch):
    """配对响应到达之前解除绑定，不能复活连接"""
    def request(method, url, key=None, payload=None):
        plugin.disconnect()
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    plugin.synchronize()
    assert plugin.get_data("connection") == {}
    assert not plugin.servers


def test_plugin_callbacks_require_separate_key(plugin, monkeypatch):
    """路由依赖拒绝匿名和全局密钥，只接受专属回调凭据"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    key = plugin.get_data("connection")["callback_key"]
    app = FastAPI()
    for api in plugin.get_api():
        api = dict(api)
        api.pop("allow_anonymous")
        app.add_api_route(**api)
    with TestClient(app) as client:
        assert client.get("/ping").status_code == 401
        assert client.get("/ping", headers={"X-API-KEY": "global-test-key"}).status_code == 401
        assert client.post("/disconnect", headers={"X-YiYi-Integration-Key": "wrong-test-key"}).status_code == 401
        result = client.get("/ping", headers={"X-YiYi-Integration-Key": key})
        assert result.status_code == 200
        assert result.json()["connectionId"] == CONNECTION_ID
        assert key not in result.text
        assert client.post("/disconnect", headers={"X-YiYi-Integration-Key": key}).status_code == 200
        assert client.get("/ping", headers={"X-YiYi-Integration-Key": key}).status_code == 401


@pytest.mark.parametrize("url", ["ftp://example.com", "https://u:p@example.com", "https://example.com?key=x", "http://localhost:invalid"])
def test_invalid_url_does_not_send_request(plugin, monkeypatch, url):
    """非法输入在发请求之前被拒绝"""
    plugin._config["yiyi_url"] = url
    calls = install_request(monkeypatch, plugin)
    plugin.synchronize()
    assert not calls
    assert not plugin.servers


def test_callback_network_failure_keeps_server_and_retryable_connection(plugin, monkeypatch):
    """单向成功的状态不能冒充双向成功"""
    def request(method, url, key=None, payload=None):
        if url.endswith("/activate"):
            return response({"success": False})
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    plugin.synchronize()
    assert plugin.servers[0]["enabled"]
    assert "尚不能访问" in plugin.get_data("status")
    assert plugin.get_data("connection")["connection_id"]


def test_http_client_enforces_tls_and_disables_redirects(plugin_module, monkeypatch):
    """发送长期凭据时显式覆盖 RequestUtils 的默认 TLS 设置"""
    calls = []
    client = SimpleNamespace(get_res=lambda **kwargs: calls.append(kwargs), post_res=lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(plugin_module, "RequestUtils", lambda **kwargs: client)
    plugin_module.YiYiMedia._request("GET", "https://yiyi.example.com", key="unit-test-key")
    plugin_module.YiYiMedia._request("POST", "https://yiyi.example.com", payload={})
    assert all(call["verify"] is True and call["allow_redirects"] is False for call in calls)
    assert calls[0]["headers"] == {"X-YiYi-Integration-Key": "unit-test-key"}


def test_config_change_event_contains_current_servers(plugin, monkeypatch):
    """落库后发送配置事件，让现有 Emby 模块重建实例"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    event, payload = plugin.events[0]
    assert event == "ConfigChanged"
    assert payload.key == {"MediaServers"}
    assert payload.value == plugin.servers


def test_paired_callback_address_can_be_corrected_without_new_pairing(plugin, monkeypatch):
    """回调地址填错后保存修正值，连接身份和长期凭据保持不变"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    before = plugin.get_data("connection")
    plugin._config["callback_url"] = "https://new-mp.example.com"
    calls = install_request(monkeypatch, plugin)
    plugin.synchronize()
    after = plugin.get_data("connection")
    assert after["connection_id"] == before["connection_id"]
    assert after["access_key"] == before["access_key"]
    assert after["callback_url"] == "https://new-mp.example.com"
    assert any(call[1].endswith("/callback") for call in calls)
    assert all(not call[1].endswith("/exchange") for call in calls)


def test_old_authorized_callback_cannot_disconnect_a_new_binding(plugin, monkeypatch):
    """鉴权后的旧回调不能清除后来生成的新连接"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    old = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])
    plugin._generation += 1
    with pytest.raises(Exception) as error:
        plugin.disconnect(old)
    assert error.value.status_code == 401
    assert plugin.get_data("connection")["connection_id"] == CONNECTION_ID
