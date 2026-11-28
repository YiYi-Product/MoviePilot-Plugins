"""验证配对恢复、配置隔离、专属鉴权及撤销竞争"""

import base64
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
        return response({"Id": "yiyi-server-1", "ServerName": "YiYi Media"})
    if url.endswith("/branding"):
        return response({"systemName": "YiYi"})
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
    assert plugin.servers[1]["name"] == "YiYi"
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
    existing = {"name": "YiYi", "type": "emby", "enabled": True, "config": {"host": "https://other.example.com"}}
    plugin.servers = [copy.deepcopy(existing)]
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    assert plugin.servers[0] == existing
    assert plugin.servers[1]["name"].startswith("YiYi (")


def test_system_name_updates_existing_server_and_preserves_settings(plugin, monkeypatch):
    """升级旧配置或系统更名时更新原记录，保留同步设置和其它服务器"""
    plugin._config["server_name"] = "旧的手工名称"
    calls = install_request(monkeypatch, plugin)
    plugin.synchronize()
    assert plugin.servers[0]["name"] == "YiYi"
    assert all(key is None for _, url, key, _ in calls if url.endswith("/branding"))
    plugin.servers[0].update({"name": "旧的手工名称", "sync_libraries": ["library-7"], "sync_interval": 90})
    existing = {"name": "家庭影院", "type": "emby", "enabled": True, "config": {"host": "https://other.example.com"}}
    plugin.servers.insert(0, copy.deepcopy(existing))
    before = copy.deepcopy(plugin.servers[1])
    state = plugin.get_data("connection")

    def request(method, url, key=None, payload=None):
        if url.endswith("/branding"):
            return response({"systemName": " 家庭影院 "})
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    plugin.synchronize()
    expected = {**before, "name": "家庭影院 (" + state["instance_id"][:8] + ")"}
    assert plugin.servers == [existing, expected]
    assert plugin.get_data("connection") == state
    plugin.events.clear()
    plugin.synchronize()
    assert plugin.events == []


@pytest.mark.parametrize("branding_response", [
    None, response(status=503), response({}), response({"systemName": "  "}), response({"systemName": 42}),
])
def test_missing_system_name_preserves_server_and_retries(plugin, monkeypatch, branding_response):
    """名称读取失败不创建默认服务器，也不覆盖已保存的服务器或丢弃授权"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    state = plugin.get_data("connection")
    servers = copy.deepcopy(plugin.servers)

    def request(method, url, key=None, payload=None):
        if url.endswith("/branding"):
            return branding_response
        return successful_request(method, url, key, payload)

    monkeypatch.setattr(plugin, "_request", request)
    plugin.synchronize()
    assert plugin.servers == servers
    assert plugin.get_data("connection") == state
    assert plugin.get_data("status") != "双向连接成功"
    plugin.servers = []
    plugin.synchronize()
    assert plugin.servers == []
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    assert plugin.servers[0]["name"] == "YiYi"


def test_generated_name_collision_preserves_all_servers(plugin, monkeypatch):
    """自动后缀名称仍被占用时停止更新，不覆盖用户服务器"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    marker = plugin.get_data("instance_id")
    plugin.servers[0]["name"] = "旧名称"
    plugin.servers.extend([
        {"name": "YiYi", "config": {}},
        {"name": "YiYi (" + marker[:8] + ")", "config": {}},
    ])
    before = copy.deepcopy(plugin.servers)
    plugin.synchronize()
    assert plugin.servers == before
    assert "名称已被占用" in plugin.get_data("status")


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


@pytest.mark.parametrize("during", ["/status", "/Info", "/branding", "/activate"])
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


def test_subscribe_endpoints_create_attach_query_and_cancel(plugin, plugin_module, monkeypatch):
    """协议 v2 回调支持创建订阅、幂等关联已有订阅、批量查进度与取消订阅，且拒绝未授权请求"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    key = plugin.get_data("connection")["callback_key"]
    app = FastAPI()
    for api in plugin.get_api():
        api = dict(api)
        api.pop("allow_anonymous")
        app.add_api_route(**api)

    with TestClient(app) as client:
        ping = client.get("/ping", headers={"X-YiYi-Integration-Key": key})
        assert ping.status_code == 200
        assert ping.json()["protocolVersion"] == 4
        assert "subscribe" in ping.json()["capabilities"]
        assert "download" in ping.json()["capabilities"]
        assert "torrent_upload" in ping.json()["capabilities"]

        assert client.post("/subscribes", json={"mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "season": 2}).status_code == 401

        created = client.post(
            "/subscribes",
            headers={"X-YiYi-Integration-Key": key},
            json={"requestId": 77, "mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "year": 2012, "season": 2, "bestVersion": True, "username": "alice"},
        )
        assert created.status_code == 200
        body = created.json()
        assert body["success"] is True
        assert body["action"] == "CREATED"
        sub_id = body["mpSubscribeId"]
        assert body["totalEpisode"] == 12
        assert body["lackEpisode"] == 4
        assert plugin_module._test_doubles.subscribe_calls[0]["best_version"] == 1

        # 重复推送同一部剧同一季，直接幂等挂载，不再调 SubscribeChain.add
        attached = client.post(
            "/subscribes",
            headers={"X-YiYi-Integration-Key": key},
            json={"requestId": 78, "mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "season": 2},
        )
        assert attached.status_code == 200
        assert attached.json()["action"] == "ATTACHED"
        assert attached.json()["mpSubscribeId"] == sub_id
        assert len(plugin_module._test_doubles.subscribe_calls) == 1

        queried = client.post(
            "/subscribes/query",
            headers={"X-YiYi-Integration-Key": key},
            json={"mpSubscribeIds": [sub_id, 99999]},
        )
        assert queried.status_code == 200
        items = queried.json()["items"]
        assert items[0] == {"mpSubscribeId": sub_id, "exists": True, "state": "R", "totalEpisode": 12, "lackEpisode": 4}
        assert items[1]["exists"] is False
        assert items[1]["state"] == "NOT_FOUND"

        cancelled = client.post(
            "/subscribes/cancel",
            headers={"X-YiYi-Integration-Key": key},
            json={"mpSubscribeId": sub_id},
        )
        assert cancelled.status_code == 200
        assert cancelled.json() == {"success": True, "removed": True}
        assert plugin_module._test_doubles.subscribes_store == {}


def test_subscribe_complete_event_pushes_callback_to_yiyi(plugin, plugin_module, monkeypatch):
    """MoviePilot 订阅完成事件携带访问凭据回推 YiYi，并脱敏清理本地跟踪"""
    calls = install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])
    created = plugin.create_subscribe(
        {"requestId": 88, "mediaType": "TV", "tmdbId": 94997, "title": "龙之家族", "season": 1},
        auth,
    )
    sub_id = created["mpSubscribeId"]
    calls.clear()

    event = plugin_module._test_doubles.Event(
        plugin_module._test_doubles.EventType.SubscribeComplete,
        {"subscribe_id": sub_id, "subscribe_info": {"id": sub_id, "tmdbid": 94997, "type": "电视剧", "season": 1, "total_episode": 10}},
    )
    plugin.on_subscribe_event(event)

    assert len(calls) == 1
    method, url, key, payload = calls[0]
    assert method == "POST"
    assert url.endswith(f"/client/{CONNECTION_ID}/subscribes/events")
    assert key == plugin.get_data("connection")["access_key"]
    assert payload == {
        "eventType": "COMPLETED",
        "mpSubscribeId": sub_id,
        "requestId": 88,
        "mediaType": "TV",
        "tmdbId": 94997,
        "season": 1,
        "state": "COMPLETED",
        "totalEpisode": 10,
        "lackEpisode": 0,
    }


def test_subscribe_error_message_redacts_sensitive_tokens(plugin, plugin_module, monkeypatch):
    """上游返回含密钥或 URL 的错误文本时自动脱敏"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])
    plugin_module._test_doubles.SubscribeChain.fail_message = "TMDB api_key=secret123 failed at https://internal.local"
    try:
        result = plugin.create_subscribe({"mediaType": "MOVIE", "tmdbId": 550, "title": "搏击俱乐部"}, auth)
        assert result["success"] is False
        assert "secret123" not in result["message"]
        assert "https://" not in result["message"]
    finally:
        plugin_module._test_doubles.SubscribeChain.fail_message = None


def test_download_search_and_selected_download_strip_credentials_and_match_missing(plugin, plugin_module, monkeypatch):
    """协议 v3 检索脱敏返回候选种子（不泄露站点 Cookie 与种子 Passkey），支持手动选种与整季按缺失集下载"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    key = plugin.get_data("connection")["callback_key"]

    ctx_other = SimpleNamespace(
        torrent_info=SimpleNamespace(
            site_name="SiteB", title="Game.of.Thrones.S02E01.1080p", description="ep1",
            site_cookie="SECRET_COOKIE_B", site_ua="SECRET_UA",
            enclosure="https://pt.example.com/download?passkey=SECRET_PASSKEY_1",
            page_url="https://pt.example.com/details.php?id=1",
            size=2_000_000_000, seeders=100, peers=5, grabs=50,
            pubdate="2026-10-01", volume_factor="普通", hit_and_run=False, labels=["中字"], pri_order=10,
        ),
        meta_info=SimpleNamespace(
            season_list=[2], episode_list=[1], season_episode="S02E01",
            resource_pix="1080p", edition="WEB-DL", video_encode="H264", audio_encode="AAC", resource_team="TEAMB",
        ),
    )
    ctx_match = SimpleNamespace(
        torrent_info=SimpleNamespace(
            site_name="MTeam", title="Game.of.Thrones.S02E03.2160p.HDR", description="ep3 免费",
            site_cookie="SECRET_COOKIE_A", site_ua="SECRET_UA",
            enclosure="https://pt.example.com/download?passkey=SECRET_PASSKEY_2",
            page_url="https://pt.example.com/details.php?id=2",
            size=4_500_000_000, seeders=42, peers=3, grabs=120,
            pubdate="2026-10-02", volume_factor="免费", hit_and_run=False, labels=["HDR", "中字"], pri_order=50,
        ),
        meta_info=SimpleNamespace(
            season_list=[2], episode_list=[3], season_episode="S02E03",
            resource_pix="2160p", edition="WEB-DL", video_encode="H265", audio_encode="DDP5.1", resource_team="HHWEB",
        ),
    )
    ctx_pack = SimpleNamespace(
        torrent_info=SimpleNamespace(
            site_name="HDSky", title="Game.of.Thrones.S02.Complete.2160p", description="全季合集",
            site_cookie="SECRET_COOKIE_C", site_ua="SECRET_UA",
            enclosure="https://pt.example.com/download?passkey=SECRET_PASSKEY_3",
            size=45_000_000_000, seeders=88, peers=10, grabs=300,
            pubdate="2026-10-03", volume_factor="2X免费", hit_and_run=True, labels=["合集"], pri_order=30,
        ),
        meta_info=SimpleNamespace(
            season_list=[2], episode_list=[], season_episode="S02",
            resource_pix="2160p", edition="BluRay", video_encode="H265", audio_encode="Atmos", resource_team="HDS",
        ),
    )
    plugin_module._test_doubles.search_contexts[:] = [ctx_other, ctx_match, ctx_pack]

    app = FastAPI()
    for api in plugin.get_api():
        api = dict(api)
        api.pop("allow_anonymous")
        app.add_api_route(**api)

    with TestClient(app) as client:
        assert client.post("/downloads/search", json={"mediaType": "TV", "tmdbId": 1399, "season": 2}).status_code == 401

        res = client.post(
            "/downloads/search",
            headers={"X-YiYi-Integration-Key": key},
            json={"mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "season": 2, "episodes": [3, 7]},
        )
        assert res.status_code == 200
        assert "SECRET_COOKIE" not in res.text
        assert "SECRET_PASSKEY" not in res.text
        assert "SECRET_UA" not in res.text
        body = res.json()
        assert body["success"] is True
        assert body["totalCount"] == 3
        # 整季合集覆盖 [3, 7]（2 集）排第一，其次 S02E03（1 集），最后不命中的 S02E01
        assert body["items"][0]["wholeSeason"] is True
        assert body["items"][0]["matchesMissing"] is True
        assert body["items"][0]["matchedEpisodes"] == [3, 7]
        assert body["items"][1]["matchedEpisodes"] == [3]
        assert body["items"][2]["matchesMissing"] is False

        # 手动挑选整季包，并指定只下载缺失集 [3, 7]
        dl = client.post(
            "/downloads",
            headers={"X-YiYi-Integration-Key": key},
            json={
                "mode": "SELECTED",
                "searchToken": body["searchToken"],
                "candidateId": body["items"][0]["candidateId"],
                "episodes": [3, 7],
                "username": "admin",
            },
        )
        assert dl.status_code == 200
        dl_body = dl.json()
        assert dl_body["success"] is True
        assert dl_body["action"] == "DOWNLOADED"
        assert dl_body["downloadedCount"] == 1
        assert plugin_module._test_doubles.download_single_calls[-1]["episodes"] == {3, 7}
        assert plugin_module._test_doubles.download_single_calls[-1]["context"] is ctx_pack

        # 解除绑定后旧搜索会话立即失效
        assert client.post("/disconnect", headers={"X-YiYi-Integration-Key": key}).status_code == 200
        assert plugin._search_cache == {}


def test_download_auto_mode_uses_batch_download_and_reports_remaining_episodes(plugin, plugin_module, monkeypatch):
    """协议 v3 AUTO 模式按缺失集调用 SearchChain + DownloadChain.batch_download 并回传剩余缺失集"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])

    ctx = SimpleNamespace(
        torrent_info=SimpleNamespace(title="Show.S02E03.2160p.WEB-DL"),
        meta_info=SimpleNamespace(season_list=[2], episode_list=[3]),
    )
    plugin_module._test_doubles.search_contexts[:] = [ctx]
    plugin_module._test_doubles.DownloadChain.batch_downloaded_override = [ctx]
    plugin_module._test_doubles.DownloadChain.batch_lefts = {
        1399: {2: plugin_module._test_doubles.NotExistMediaInfo(season=2, episodes=[7], total_episode=10, start_episode=3)}
    }

    res = plugin.trigger_download(
        {"mode": "AUTO", "mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "season": 2, "episodes": [3, 7], "totalEpisode": 10, "username": "admin"},
        auth,
    )
    assert res["success"] is True
    assert res["action"] == "DOWNLOADED"
    assert res["downloadedCount"] == 1
    assert res["downloadedTitles"] == ["Show.S02E03.2160p.WEB-DL"]
    assert res["remainingEpisodes"] == [7]
    call = plugin_module._test_doubles.batch_download_calls[-1]
    assert call["no_exists"][1399][2].episodes == [3, 7]

    # 无结果时返回 NO_RESULTS 并保留缺失集列表
    plugin_module._test_doubles.search_contexts[:] = []
    empty_res = plugin.trigger_download(
        {"mode": "AUTO", "mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "season": 2, "episodes": [7]},
        auth,
    )
    assert empty_res["success"] is False
    assert empty_res["action"] == "NO_RESULTS"
    assert empty_res["remainingEpisodes"] == [7]


def test_torrent_upload_download_and_query_progress_protocol_v4(plugin, plugin_module, monkeypatch):
    """协议 v4 支持接收审核后的原始种子并精准传递 TMDB ID / 季 / 集，同时支持批量查询下载进度"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    key = plugin.get_data("connection")["callback_key"]
    sample_torrent = b"d8:announce26:https://pt.test/a?pk=sec4:infod4:name18:Show.S02E07.2160p.e"
    b64_payload = base64.b64encode(sample_torrent).decode("ascii")

    plugin_module._test_doubles.DownloadChain.single_return = ("hash-torrent-99", "")
    plugin_module._test_doubles.DownloadChain.downloading_items[:] = [
        SimpleNamespace(hash="hash-torrent-99", progress=62.5, state="downloading", downloader="qBittorrent")
    ]
    plugin_module._test_doubles.history_store["hash-done-1"] = SimpleNamespace(downloader="Transmission")

    app = FastAPI()
    for api in plugin.get_api():
        api = dict(api)
        api.pop("allow_anonymous")
        app.add_api_route(**api)

    with TestClient(app) as client:
        # 拒绝未授权
        assert client.post("/downloads/torrent", json={}).status_code == 401
        # 拒绝非法种子字节
        bad = client.post(
            "/downloads/torrent",
            headers={"X-YiYi-Integration-Key": key},
            json={
                "mediaType": "TV",
                "tmdbId": 1399,
                "title": "权力的游戏",
                "season": 2,
                "episodes": [7],
                "torrentName": "bad.torrent",
                "torrentBase64": base64.b64encode(b"not-a-torrent").decode("ascii"),
            },
        )
        assert bad.status_code == 400

        ok = client.post(
            "/downloads/torrent",
            headers={"X-YiYi-Integration-Key": key},
            json={
                "requestId": 12,
                "mediaType": "TV",
                "tmdbId": 1399,
                "title": "权力的游戏",
                "year": 2012,
                "season": 2,
                "episodes": [7, 8],
                "torrentName": "Game.of.Thrones.S02E07-E08.2160p",
                "torrentBase64": b64_payload,
                "totalSize": 8_000_000_000,
                "username": "alice",
                "downloader": "qBittorrent",
                "savePath": "/downloads/tv",
                "label": "YiYi用户投递",
            },
        )
        assert ok.status_code == 200
        body = ok.json()
        assert body["success"] is True
        assert body["action"] == "DOWNLOADED"
        assert body["downloadHash"] == "hash-torrent-99"
        assert body["downloader"] == "qBittorrent"

        last_call = plugin_module._test_doubles.download_single_calls[-1]
        assert last_call["torrent_content"] == sample_torrent
        assert last_call["episodes"] == {7, 8}
        assert last_call["downloader"] == "qBittorrent"
        assert last_call["save_path"] == "/downloads/tv"
        assert last_call["label"] == "YiYi用户投递"
        ctx = last_call["context"]
        assert ctx.media_info.tmdb_id == 1399
        assert ctx.meta_info.tmdbid == 1399
        assert ctx.meta_info.begin_season == 2
        assert ctx.meta_info.begin_episode == 7
        assert ctx.meta_info.end_episode == 8

        # 批量查询下载进度：活跃任务 + 历史已完成 + 不存在
        queried = client.post(
            "/downloads/query",
            headers={"X-YiYi-Integration-Key": key},
            json={"hashes": ["hash-torrent-99", "hash-done-1", "hash-missing"]},
        )
        assert queried.status_code == 200
        items = queried.json()["items"]
        assert items[0] == {
            "downloadHash": "hash-torrent-99",
            "exists": True,
            "active": True,
            "state": "downloading",
            "progress": 62.5,
            "downloader": "qBittorrent",
        }
        assert items[1]["exists"] is True
        assert items[1]["active"] is False
        assert items[1]["state"] == "FINISHED_OR_REMOVED"
        assert items[1]["downloader"] == "Transmission"
        assert items[2]["exists"] is False
        assert items[2]["state"] == "NOT_FOUND"


def test_torrent_upload_preserves_builtin_tag_and_queries_all_tags(plugin, plugin_module, monkeypatch):
    """自定义标签必须保留 settings.TORRENT_TAG，且进度查询优先用 list_torrents(include_all_tags=True)"""
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])
    monkeypatch.setattr(plugin_module.settings, "TORRENT_TAG", "MOVIEPILOT", raising=False)

    sample_torrent = b"d8:announce19:https://pt.test/ann4:infod4:name12:Movie.2026.e"
    plugin_module._test_doubles.DownloadChain.single_return = ("hash-pt-1", "")
    res = plugin.upload_torrent_download(
        {
            "mediaType": "MOVIE",
            "tmdbId": 1137844,
            "title": "求救信号",
            "year": 2026,
            "torrentName": "Mayday.2026.2160p",
            "torrentBase64": base64.b64encode(sample_torrent).decode("ascii"),
            "label": "YiYi用户投递",
        },
        auth,
    )
    assert res["success"] is True
    last_call = plugin_module._test_doubles.download_single_calls[-1]
    assert last_call["label"] == "MOVIEPILOT,YiYi用户投递"

    # 模拟 downloading() 为空（仅限内置标签过滤掉），但 list_torrents(include_all_tags=True) 命中真实进度
    plugin_module._test_doubles.DownloadChain.downloading_items[:] = []
    plugin_module._test_doubles.DownloadChain.list_torrents_items[:] = [
        SimpleNamespace(hash="hash-pt-1", progress=27.4, state="downloading", downloader="下载器2"),
        SimpleNamespace(hash="hash-pt-done", progress=100.0, state="completed", downloader="下载器2"),
    ]
    plugin_module._test_doubles.history_store["hash-pt-1"] = SimpleNamespace(downloader="下载器2")

    queried = plugin.query_downloads({"hashes": ["hash-pt-1", "hash-pt-done"]}, auth)
    assert plugin_module._test_doubles.DownloadChain.list_torrents_calls[-1]["include_all_tags"] is True
    assert queried["items"][0] == {
        "downloadHash": "hash-pt-1",
        "exists": True,
        "active": True,
        "state": "downloading",
        "progress": 27.4,
        "downloader": "下载器2",
    }
    assert queried["items"][1] == {
        "downloadHash": "hash-pt-done",
        "exists": True,
        "active": False,
        "state": "completed",
        "progress": 100.0,
        "downloader": "下载器2",
    }


@pytest.mark.parametrize("plugin_path", [
    "plugins.v3/yiyimedia/__init__.py",
    "plugins.v2/yiyimedia/__init__.py",
])
def test_v3_host_subscribe_uses_unified_identity_dedupes_and_handles_pydantic_events(
    load_v3_host_plugin, monkeypatch, plugin_path
):
    """严格 V3 宿主下：订阅写入使用 media_source/media_id（不触发 SubscriptionPatch 未知字段），去重忽略非 TMDB 来源，事件兼容 Pydantic 模型"""
    module, plugin = load_v3_host_plugin(plugin_path)
    doubles = module._test_doubles
    calls = install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])

    # 预置同数值但属于豆瓣来源的订阅，不应被误判为同 TMDB 订阅
    doubles.subscribes_store[1] = SimpleNamespace(
        id=1, type="电视剧", media_source="douban", media_id="298546", season=1, state="R"
    )

    created = plugin.create_subscribe(
        {
            "requestId": 101,
            "mediaType": "TV",
            "tmdbId": 298546,
            "title": "新手上路",
            "year": 2025,
            "season": 1,
            "bestVersion": True,
            "username": "alice",
        },
        auth,
    )
    assert created["success"] is True
    assert created["action"] == "CREATED"
    sub_id = created["mpSubscribeId"]
    assert len(doubles.subscribe_calls) == 1
    first_call = doubles.subscribe_calls[0]
    assert first_call["media_source"] == doubles.MediaSource.TMDB
    assert first_call["media_id"] == "298546"
    assert "tmdbid" not in first_call["kwargs"]
    assert "tmdb_id" not in first_call["kwargs"]
    assert first_call["kwargs"]["best_version"] == 1

    # 再次推送同一 TMDB 剧集同季，应命中 V3 的 media_source/media_id 幂等挂载
    attached = plugin.create_subscribe(
        {"requestId": 102, "mediaType": "TV", "tmdbId": 298546, "title": "新手上路", "season": 1},
        auth,
    )
    assert attached["success"] is True
    assert attached["action"] == "ATTACHED"
    assert attached["mpSubscribeId"] == sub_id
    assert len(doubles.subscribe_calls) == 1

    # V3 订阅完成事件可能传递 Pydantic BaseEventData（带 model_dump），且 subscribe_info 仅含 media_source/media_id
    class FakeV3EventData:
        def model_dump(self):
            return {
                "subscribe_id": sub_id,
                "subscribe_info": {
                    "id": sub_id,
                    "media_source": "themoviedb",
                    "media_id": "298546",
                    "type": "电视剧",
                    "season": 1,
                    "total_episode": 12,
                },
                "mediainfo": {
                    "media_source": "themoviedb",
                    "media_id": "298546",
                    "tmdb_id": 298546,
                    "type": "电视剧",
                },
            }

    calls.clear()
    plugin.on_subscribe_event(doubles.Event(doubles.EventType.SubscribeComplete, FakeV3EventData()))
    assert len(calls) == 1
    _, url, _, payload = calls[0]
    assert url.endswith(f"/client/{CONNECTION_ID}/subscribes/events")
    assert payload == {
        "eventType": "COMPLETED",
        "mpSubscribeId": sub_id,
        "requestId": 102,
        "mediaType": "TV",
        "tmdbId": 298546,
        "season": 1,
        "state": "COMPLETED",
        "totalEpisode": 12,
        "lackEpisode": 0,
    }

    # 未在本地跟踪表中的 V3 订阅事件，也能直接从 subscribe_info.media_source / media_id 提取 tmdbId
    calls.clear()
    plugin.on_subscribe_event(
        doubles.Event(
            doubles.EventType.SubscribeModified,
            {
                "subscribe_id": 999,
                "subscribe_info": {
                    "id": 999,
                    "media_source": "themoviedb",
                    "media_id": "1399",
                    "type": "电视剧",
                    "season": 2,
                    "state": "R",
                    "total_episode": 10,
                    "lack_episode": 3,
                },
            },
        )
    )
    assert len(calls) == 1
    assert calls[0][3] == {
        "eventType": "UPDATED",
        "mpSubscribeId": 999,
        "requestId": None,
        "mediaType": "TV",
        "tmdbId": 1399,
        "season": 2,
        "state": "R",
        "totalEpisode": 10,
        "lackEpisode": 3,
    }


@pytest.mark.parametrize("plugin_path", [
    "plugins.v3/yiyimedia/__init__.py",
    "plugins.v2/yiyimedia/__init__.py",
])
def test_v3_host_search_auto_download_and_torrent_upload_use_unified_identity(
    load_v3_host_plugin, monkeypatch, plugin_path
):
    """严格 V3 宿主下：检索、AUTO 自动下载与用户种子投递均使用 media_source/media_id 及 tmdb:<id> 缺集键"""
    module, plugin = load_v3_host_plugin(plugin_path)
    doubles = module._test_doubles
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])

    ctx = SimpleNamespace(
        torrent_info=SimpleNamespace(
            site_name="MTeam", title="Show.S02E03.2160p", description="ep3",
            size=4_000_000_000, seeders=30, peers=2, grabs=10,
            pubdate="2026-11-28", volume_factor="免费", hit_and_run=False, labels=[], pri_order=20,
        ),
        meta_info=SimpleNamespace(
            season_list=[2], episode_list=[3], season_episode="S02E03",
            resource_pix="2160p", edition="WEB-DL", video_encode="H265", audio_encode="AAC", resource_team="TEAM",
        ),
    )
    doubles.search_contexts[:] = [ctx]

    # 1. 手动检索：严格 V3 search_by_id(media_source=TMDB, media_id="1399")
    searched = plugin.search_downloads({"mediaType": "TV", "tmdbId": 1399, "season": 2, "episodes": [3]}, auth)
    assert searched["success"] is True
    assert searched["totalCount"] == 1
    assert doubles.search_calls[-1] == ("search_by_id", {
        "media_source": doubles.MediaSource.TMDB,
        "media_id": "1399",
        "mtype": doubles.MediaType.TV,
        "season": 2,
    })

    # 2. AUTO 补全：recognize_media 使用 V3 统一身份，no_exists 与 lefts 键为 tmdb:1399
    doubles.DownloadChain.batch_downloaded_override = [ctx]
    doubles.DownloadChain.batch_lefts = {
        "tmdb:1399": {2: doubles.NotExistMediaInfo(season=2, episodes=[7], total_episode=10, start_episode=3)}
    }
    auto_res = plugin.trigger_download(
        {"mode": "AUTO", "mediaType": "TV", "tmdbId": 1399, "title": "权力的游戏", "season": 2, "episodes": [3, 7], "totalEpisode": 10},
        auth,
    )
    assert auto_res["success"] is True
    assert auto_res["remainingEpisodes"] == [7]
    assert doubles.recognize_calls[-1] == ("SearchChain", {
        "mtype": doubles.MediaType.TV,
        "media_source": doubles.MediaSource.TMDB,
        "media_id": "1399",
    })
    assert "tmdb:1399" in doubles.batch_download_calls[-1]["no_exists"]
    assert doubles.batch_download_calls[-1]["no_exists"]["tmdb:1399"][2].episodes == [3, 7]

    # 3. 审核种子投递：识别失败回退构造 MediaInfo 时也带齐 V3 media_source / media_id
    doubles.MediaChain.return_none = True
    monkeypatch.setattr(doubles.SearchChain, "recognize_media", lambda self, **kw: None)
    sample_torrent = b"d8:announce19:https://pt.test/ann4:infod4:name12:Show.S02E07.e"
    up_res = plugin.upload_torrent_download(
        {
            "mediaType": "TV",
            "tmdbId": 1399,
            "title": "权力的游戏",
            "year": 2012,
            "season": 2,
            "episodes": [7],
            "torrentName": "Show.S02E07.2160p",
            "torrentBase64": base64.b64encode(sample_torrent).decode("ascii"),
            "label": "YiYi用户投递",
        },
        auth,
    )
    assert up_res["success"] is True
    last_dl = doubles.download_single_calls[-1]
    assert last_dl["label"] == "MOVIEPILOT,YiYi用户投递"
    assert last_dl["context"].media_info.media_source == doubles.MediaSource.TMDB
    assert last_dl["context"].media_info.media_id == "1399"
    assert last_dl["context"].media_info.tmdb_id == 1399
    assert last_dl["context"].meta_info.media_source == doubles.MediaSource.TMDB
    assert last_dl["context"].meta_info.media_id == "1399"
    assert last_dl["context"].torrent_info.media_source == doubles.MediaSource.TMDB
    assert last_dl["context"].torrent_info.media_id == "1399"


def test_subscribe_fallback_recovers_when_host_rejects_identity_kwargs(load_v3_host_plugin, monkeypatch):
    """若异构宿主拒绝身份参数并返回“订阅分类设置无效”或未知字段错误，自动回退按标题/年份/季写入"""
    module, plugin = load_v3_host_plugin("plugins.v2/yiyimedia/__init__.py")
    doubles = module._test_doubles
    install_request(monkeypatch, plugin)
    plugin.synchronize()
    auth = plugin.verify_integration_key(plugin.get_data("connection")["callback_key"])

    seen = []

    def strict_fork_add(self, title, year, mtype=None, season=None, best_version=0, exist_ok=False, username=None, **kwargs):
        seen.append(dict(kwargs))
        if kwargs:
            # 模拟拒绝所有额外身份字段并返回通用分类错误
            return None, "订阅分类设置无效，请重新选择分类后重试"
        item = SimpleNamespace(
            id=305, type="电视剧", media_source="themoviedb", media_id="298546",
            season=season, name=title, state="R", total_episode=16, lack_episode=16,
        )
        doubles.subscribes_store[305] = item
        return 305, "新增订阅成功"

    monkeypatch.setattr(doubles.SubscribeChain, "add", strict_fork_add)
    res = plugin.create_subscribe(
        {"requestId": 201, "mediaType": "TV", "tmdbId": 298546, "title": "那年那兔那些事儿", "year": 2015, "season": 1},
        auth,
    )
    assert res["success"] is True
    assert res["action"] == "CREATED"
    assert res["mpSubscribeId"] == 305
    assert seen[0] == {"media_source": doubles.MediaSource.TMDB, "media_id": "298546"}
    assert seen[1] == {}


def test_package_indexes_match_plugin_versions():
    """package.v2.json / package.v3.json 元数据与对应插件代码版本及历史置顶规则严格一致"""
    import json
    import re
    from pathlib import Path

    root = Path(__file__).parents[1]
    pkg_v2 = json.loads((root / "package.v2.json").read_text(encoding="utf-8"))["YiYiMedia"]
    pkg_v3 = json.loads((root / "package.v3.json").read_text(encoding="utf-8"))["YiYiMedia"]

    v2_src = (root / "plugins.v2/yiyimedia/__init__.py").read_text(encoding="utf-8")
    v3_src = (root / "plugins.v3/yiyimedia/__init__.py").read_text(encoding="utf-8")

    v2_ver = re.search(r'plugin_version\s*=\s*"([^"]+)"', v2_src).group(1)
    v3_ver = re.search(r'plugin_version\s*=\s*"([^"]+)"', v3_src).group(1)

    assert pkg_v2["version"] == v2_ver
    assert pkg_v2["v3"] is False
    assert next(iter(pkg_v2["history"])) == f"v{v2_ver}"

    assert pkg_v3["version"] == v3_ver
    assert pkg_v3["system_version"] == ">=3.0.0"
    assert next(iter(pkg_v3["history"])) == f"v{v3_ver}"
