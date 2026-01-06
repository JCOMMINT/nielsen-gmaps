from lambdas.enrichment_to_dynamo.src import handler


def test_parse_event_s3_record():
    event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": "bucket"},
                    "object": {"key": "enrichment/id=123/file.parquet"},
                },
            }
        ]
    }

    payloads = list(handler.parse_event(event))

    assert payloads == [{"bucket": "bucket", "key": "enrichment/id=123/file.parquet"}]


def test_parse_event_direct():
    event = {"s3_bucket": "bucket", "s3_key": "key", "id": "999"}

    payloads = list(handler.parse_event(event))

    assert payloads == [{"bucket": "bucket", "key": "key", "id": "999"}]


def test_parse_id_from_key():
    assert handler.parse_id_from_key("prefix/id=abc123/file.parquet") == "abc123"
