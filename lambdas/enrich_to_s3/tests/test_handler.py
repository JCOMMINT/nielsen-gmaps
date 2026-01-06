from lambdas.enrich_to_s3.src import handler


def test_extract_urls_limits_and_prioritizes():
    item = {
        "serp_results": [
            {"website": "https://example.com"},
            {"url": "https://maps.example.com"},
        ]
    }

    urls = handler.extract_urls(item, limit=1)

    assert urls == ["https://example.com"]


def test_build_s3_key_contains_id():
    key = handler.build_s3_key("abc123")

    assert "id=abc123" in key
    assert key.endswith(".parquet")
