import json

from lambdas.s3_ingest_to_sqs.src import handler


def test_parse_s3_event_decodes_key():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "bucket"},
                    "object": {"key": "folder%2Ffile.csv"},
                }
            }
        ]
    }

    results = list(handler.parse_s3_event(event))

    assert results == [("bucket", "folder/file.csv")]


def test_build_messages_trims_fields():
    rows = [{"search_term": " Cafe ", "id": " 123 ", "country": " IE "}]

    messages = handler.build_messages(rows, "src", "key")

    assert messages[0]["search_term"] == "Cafe"
    assert messages[0]["id"] == "123"
    assert messages[0]["country"] == "IE"
    assert messages[0]["total_rows"] == 1


def test_send_sqs_batches_groups_payloads():
    sent = []

    class FakeSQS:
        def send_message_batch(self, QueueUrl, Entries):
            sent.append((QueueUrl, Entries))
            return {"Successful": Entries}

    messages = [{"id": "1"}, {"id": "2"}, {"id": "3"}]

    handler.send_sqs_batches(FakeSQS(), "url", messages, batch_size=2)

    assert len(sent) == 2
    assert json.loads(sent[0][1][0]["MessageBody"]) == {"id": "1"}
