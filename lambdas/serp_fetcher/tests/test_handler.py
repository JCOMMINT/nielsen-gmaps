import json
import os

from lambdas.serp_fetcher.src import handler


def test_parse_sqs_event():
    event = {"Records": [{"body": json.dumps({"id": "1"})}]}
    items = list(handler.parse_sqs_event(event))
    assert items == [{"id": "1"}]


def test_build_proxy_config_missing_env(monkeypatch):
    monkeypatch.delenv("BRIGHTDATA_USERNAME", raising=False)
    monkeypatch.delenv("BRIGHTDATA_PASSWORD", raising=False)
    assert handler.build_proxy_config() is None


def test_build_proxy_config_with_env(monkeypatch):
    monkeypatch.setenv("BRIGHTDATA_USERNAME", "user")
    monkeypatch.setenv("BRIGHTDATA_PASSWORD", "pass")
    monkeypatch.setenv("BRIGHTDATA_PORT", "123")
    proxy = handler.build_proxy_config()
    assert proxy["http"].startswith("http://user-session-")
    assert ":pass@" in proxy["http"]


def test_fetch_serp_uses_requests(monkeypatch):
    class FakeResponse:
        def __init__(self):
            self.text = "<html></html>"

        def raise_for_status(self):
            return None

        def json(self):
            raise ValueError("not json")

    def fake_get(url, params, proxies, timeout, verify, headers):
        assert "maps/search" in url
        return FakeResponse()

    monkeypatch.setattr(handler.requests, "get", fake_get)
    monkeypatch.setenv("SERP_BASE_URL", "https://www.google.com/maps/search/")

    result = handler.fetch_serp("coffee", proxies=None)
    assert result == {"raw_html": "<html></html>"}
