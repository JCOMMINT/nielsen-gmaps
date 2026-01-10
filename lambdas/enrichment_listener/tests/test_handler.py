"""Tests for enrichment_listener Lambda handler."""

import json
import os
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set environment variables
os.environ["LOG_LEVEL"] = "INFO"
os.environ["SERVICE_NAME"] = "enrichment_listener"
os.environ["DYNAMODB_ENRICHMENT_TABLE"] = "GMapsEnrichment"
os.environ["DYNAMODB_BATCH_TABLE"] = "GMapsCSVBatch"
os.environ["AWS_REGION"] = "us-east-1"

from src.handler import (
    lambda_handler,
    parse_sqs_event,
    parse_s3_key,
    read_enrichment_from_s3,
)


@pytest.fixture
def sqs_event():
    """Sample SQS event with SNS message."""
    sns_message = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "enrichment-bucket"},
                    "object": {"key": "wsapi/123/csv_name=test.csv/ID=place_1/results.json"}
                }
            }
        ]
    }
    return {
        "Records": [
            {
                "body": json.dumps({
                    "Message": json.dumps(sns_message)
                })
            }
        ]
    }


@pytest.fixture
def lambda_context():
    """Mock Lambda context."""
    context = MagicMock()
    context.aws_request_id = "test-request-id"
    return context


class TestParseS3Key:
    """Tests for parse_s3_key function."""

    def test_parse_valid_key(self):
        """Test parsing valid S3 key."""
        key = "wsapi/123/csv_name=test.csv/ID=place_1/results.json"
        csv_name, query_id = parse_s3_key(key)

        assert csv_name == "test.csv"
        assert query_id == "place_1"

    def test_parse_key_with_encoded_values(self):
        """Test parsing S3 key with URL-encoded values."""
        key = "wsapi/123/csv_name=test%20file.csv/ID=place%201/results.json"
        csv_name, query_id = parse_s3_key(key)

        assert csv_name == "test file.csv"
        assert query_id == "place 1"

    def test_parse_invalid_key(self):
        """Test parsing invalid S3 key."""
        key = "invalid/path/structure.json"
        csv_name, query_id = parse_s3_key(key)

        assert csv_name is None
        assert query_id is None


class TestReadEnrichmentFromS3:
    """Tests for read_enrichment_from_s3 function."""

    @patch("src.handler.s3_client")
    def test_read_valid_json(self, mock_s3):
        """Test reading valid enrichment JSON."""
        enrichment_data = {"url": "https://example.com", "data": "test"}
        mock_response = {
            "Body": MagicMock(read=lambda: json.dumps(enrichment_data).encode())
        }
        mock_s3.get_object.return_value = mock_response

        result = read_enrichment_from_s3("bucket", "key")

        assert result == enrichment_data

    @patch("src.handler.s3_client")
    def test_read_invalid_json(self, mock_s3):
        """Test reading invalid JSON from S3."""
        mock_response = {
            "Body": MagicMock(read=lambda: b"invalid json")
        }
        mock_s3.get_object.return_value = mock_response

        result = read_enrichment_from_s3("bucket", "key")

        assert result is None


class TestParseSQSEvent:
    """Tests for parse_sqs_event function."""

    def test_parse_valid_event(self, sqs_event):
        """Test parsing valid SQS event."""
        result = parse_sqs_event(sqs_event)

        assert result["s3_bucket"] == "enrichment-bucket"
        assert "csv_name=test.csv" in result["s3_key"]

    def test_parse_invalid_event(self):
        """Test parsing invalid event."""
        event = {"Records": [{"body": "invalid"}]}
        result = parse_sqs_event(event)

        assert result["s3_bucket"] is None
        assert result["s3_key"] is None


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @patch("src.handler.save_enrichment_result")
    @patch("src.handler.update_batch_status")
    @patch("src.handler.read_enrichment_from_s3")
    def test_handler_success(
        self,
        mock_read,
        mock_update,
        mock_save,
        sqs_event,
        lambda_context,
    ):
        """Test successful lambda handler execution."""
        enrichment_data = {"url": "https://example.com", "data": "test"}
        mock_read.return_value = enrichment_data

        response = lambda_handler(sqs_event, lambda_context)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["csv_name"] == "test.csv"
        assert body["query_id"] == "place_1"

    def test_handler_missing_s3_info(self, lambda_context):
        """Test handler with missing S3 info."""
        event = {"Records": [{"body": json.dumps({})}]}

        response = lambda_handler(event, lambda_context)

        assert response["statusCode"] == 400


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
