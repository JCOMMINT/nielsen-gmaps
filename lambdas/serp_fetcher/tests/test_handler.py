import json

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


def test_normalize_tags_groups_values():
    tags = [
        {"group_title": "Highlights", "value_title": "Great coffee"},
        {"group_title": "Service options", "value_title_short": "Takeout"},
        {"group_title": "Other", "value_title": "Ignore"},
    ]

    grouped = handler.normalize_tags(tags)

    assert grouped["highlights"] == ["Great coffee"]
    assert grouped["serviceOptions"] == ["Takeout"]


def test_derive_trading_status():
    assert handler.derive_trading_status("Permanently closed") == "permanently_closed"
    assert handler.derive_trading_status("Closed · Opens 7:30 AM") == "closed"
    assert handler.derive_trading_status("Open now") == "open"
    assert handler.derive_trading_status("") is None


def test_build_place_item_fills_required_fields():
    serp_place = handler.build_place_item({"title": "Cafe", "map_id_encoded": "abc"})
    assert serp_place["title"] == "Cafe"
    assert serp_place["menu"] == "Not Found"
    assert serp_place["busy_times"] == "Not Found"


def test_normalize_place_id():
    assert handler.normalize_place_id(" abc ") == "abc"
    assert handler.normalize_place_id("") is None


class FakePaginator:
    def __init__(self, store):
        self.store = store

    def paginate(self, Bucket, Prefix):
        contents = [
            {"Key": key} for key in sorted(self.store.keys()) if key.startswith(Prefix)
        ]
        return [{"Contents": contents}]


class FakeS3Client:
    def __init__(self):
        self.store = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.store[Key] = Body
        return {}

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.store[Key])}

    def get_paginator(self, name):
        return FakePaginator(self.store)


class FakeBody:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return self.payload


def test_lambda_handler_offline(monkeypatch):
    s3_client = FakeS3Client()

    def fake_client(service):
        assert service == "s3"
        return s3_client

    def fake_post(url, headers=None, params=None, json=None, timeout=None):
        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {"snapshot_id": "snap-1"}

        return Response()

    def fake_get(url, params=None, proxies=None, timeout=None, verify=None, headers=None):
        class Response:
            def raise_for_status(self):
                return None

            def json(self_inner):
                return {
                    "organic": [
                        {
                            "map_id_encoded": " ChIJabc ",
                            "title": "Cafe",
                            "price": "$",
                            "rating": 4.8,
                            "reviews_cnt": 10,
                            "link": "https://example.com",
                            "address": "123 St",
                            "phone": "123",
                            "work_status": "Open now",
                            "tags": [{"group_title": "Highlights", "value_title": "Great coffee"}],
                            "category": [{"title": "Coffee shop"}],
                        }
                    ]
                }

        return Response()

    monkeypatch.setattr(handler.boto3, "client", fake_client)
    monkeypatch.setattr(handler.requests, "get", fake_get)
    monkeypatch.setattr(handler.requests, "post", fake_post)
    monkeypatch.setenv("RESULTS_BUCKET", "results-bucket")
    monkeypatch.setenv("MAX_MESSAGES", "1")
    monkeypatch.setenv("MAX_RESULTS", "5")
    monkeypatch.setenv("BRIGHTDATA_S3_BUCKET", "nielsen-enrichment")
    monkeypatch.setenv("BRIGHTDATA_ROLE_ARN", "arn:aws:iam::123:role/bd")
    monkeypatch.setenv("BRIGHTDATA_EXTERNAL_ID", "external-id")
    monkeypatch.setenv("BRIGHTDATA_DATASET_ID", "dataset")
    monkeypatch.setenv("BRIGHTDATA_TOKEN", "token")

    event = {
        "Records": [
            {"body": json.dumps(
                {
                    "id": "dry-1",
                    "search_term": "Coffee shop Cork IE",
                    "country": "IE",
                    "source_key": "input/sample.csv",
                    "total_rows": 1,
                }
            )}
        ]
    }

    result = handler.lambda_handler(event, None)
    assert result["processed"][0]["query_id"] == "dry-1"
    assert any("/batch_" in key for key in s3_client.store)
    assert any("/serp/query=" in key for key in s3_client.store)
    assert any("_enrichment_request.json" in key for key in s3_client.store)
