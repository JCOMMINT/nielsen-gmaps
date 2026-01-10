"""Tests for serp_fetcher Lambda handler."""

import json
import os
import sys
from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch, call

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set environment variables before importing
os.environ["LOG_LEVEL"] = "INFO"
os.environ["SERVICE_NAME"] = "serp_fetcher"
os.environ["SERP_BASE_URL"] = "https://www.google.com/maps/search/"
os.environ["REQUEST_TIMEOUT"] = "20"
os.environ["VERIFY_TLS"] = "true"
os.environ["USER_AGENT"] = "Mozilla/5.0"
os.environ["MAX_SERP_RETRIES"] = "3"
os.environ["MAX_RESULTS_PER_QUERY"] = "5"
os.environ["RETRY_DELAY_SECONDS"] = "1"
os.environ["DYNAMODB_QUERIES_TABLE"] = "GMapsQueries"
os.environ["DYNAMODB_BATCH_TABLE"] = "GMapsCSVBatch"
os.environ["BD_TRIGGER_SNS_TOPIC"] = "arn:aws:sns:us-east-1:123456789012:bd"
os.environ["AWS_REGION"] = "us-east-1"

from src.handler import (
    lambda_handler,
    parse_sqs_event,
    fetch_serp_with_retry,
    build_place_item,
    normalize_tags,
    normalize_place_id,
)


@pytest.fixture
def sqs_event():
    """Sample SQS event."""
    return {
        "Records": [
            {
                "body": json.dumps({
                    "csv_name": "test_2024_01_09.csv",
                    "ID": "place_123",
                    "search_term": "coffee near london",
                    "country": "UK",
                    "retry_attempt": 0,
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


@pytest.fixture
def mock_serp_response():
    """Sample SERP response."""
    return {
        "organic": [
            {
                "position": 1,
                "title": "Coffee Shop A",
                "map_id_encoded": "place_1",
                "link": "https://coffeeshopa.com",
                "rating": 4.5,
                "reviews_cnt": 123,
                "address": "123 Main St",
                "phone": "+44 123",
                "work_status": "Open now",
                "latitude": 51.5074,
                "longitude": -0.1278,
                "category": [{"title": "Cafe"}],
                "tags": [
                    {"group_title": "Amenities", "value_title": "Wi-Fi"},
                ],
            }
        ]
    }


class TestParseSQSEvent:
    """Tests for parse_sqs_event."""

    def test_parse_valid_event(self, sqs_event):
        """Test parsing valid SQS event."""
        messages = parse_sqs_event(sqs_event)
        assert len(messages) == 1
        assert messages[0]["csv_name"] == "test_2024_01_09.csv"

    def test_parse_empty_event(self):
        """Test parsing empty event."""
        event = {"Records": []}
        messages = parse_sqs_event(event)
        assert len(messages) == 0


class TestNormalizePlaceId:
    """Tests for normalize_place_id."""

    def test_valid_id(self):
        """Test normalizing valid place ID."""
        result = normalize_place_id("place_123")
        assert result == "place_123"

    def test_none_id(self):
        """Test normalizing None."""
        result = normalize_place_id(None)
        assert result is None


class TestNormalizeTags:
    """Tests for normalize_tags."""

    def test_valid_tags(self):
        """Test normalizing valid tags."""
        tags = [{"group_title": "Amenities", "value_title": "Wi-Fi"}]
        result = normalize_tags(tags)
        assert "amenities" in result
        assert "Wi-Fi" in result["amenities"]

    def test_empty_tags(self):
        """Test empty tags."""
        result = normalize_tags([])
        assert result == {}


class TestBuildPlaceItem:
    """Tests for build_place_item."""

    def test_build_full_item(self, mock_serp_response):
        """Test building place item."""
        result = mock_serp_response["organic"][0]
        place = build_place_item(result)

        assert place["title"] == "Coffee Shop A"
        assert place["googleplaceid"] == "place_1"


class TestFetchSerpWithRetry:
    """Tests for fetch_serp_with_retry."""

    @patch("src.handler.requests.get")
    def test_successful_fetch(self, mock_get, mock_serp_response):
        """Test successful SERP fetch."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_serp_response
        mock_get.return_value = mock_response

        result = fetch_serp_with_retry(
            search_term="coffee near london",
            proxies=None,
        )

        assert result == mock_serp_response

    @patch("src.handler.requests.get")
    def test_fetch_timeout(self, mock_get):
        """Test fetch with all retries failing."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()

        result = fetch_serp_with_retry(
            search_term="coffee",
            proxies=None,
            max_retries=1,
        )

        assert result is None


class TestLambdaHandler:
    """Tests for lambda_handler."""

    @patch("src.handler.save_serp_result")
    @patch("src.handler.update_batch_status")
    @patch("src.handler.trigger_brightdata_enrichment")
    @patch("src.handler.fetch_serp_with_retry")
    def test_handler_success(
        self,
        mock_fetch,
        mock_trigger,
        mock_update_batch,
        mock_save,
        sqs_event,
        lambda_context,
        mock_serp_response,
    ):
        """Test successful lambda handler execution."""
        mock_fetch.return_value = mock_serp_response

        response = lambda_handler(sqs_event, lambda_context)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["processed"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
