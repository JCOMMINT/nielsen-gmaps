"""Tests for shared DynamoDB utilities."""

import json
import os
import sys
import pytest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Add shared to path
sys.path.insert(0, str(Path(__file__).parent))

# Set environment variables before importing
os.environ["DYNAMODB_QUERIES_TABLE"] = "GMapsQueries"
os.environ["DYNAMODB_ENRICHMENT_TABLE"] = "GMapsEnrichment"
os.environ["DYNAMODB_BATCH_TABLE"] = "GMapsCSVBatch"
os.environ["AWS_REGION"] = "us-east-1"

from dynamo_utils import (
    get_ttl_timestamp,
    save_serp_result,
    update_serp_status,
    save_enrichment_result,
    update_batch_status,
    query_by_csv,
    get_batch_info,
)


@pytest.fixture
def mock_dynamodb():
    """Mock boto3 DynamoDB resource."""
    with patch("dynamo_utils.dynamodb") as mock:
        yield mock


@pytest.fixture
def sample_serp_result():
    """Sample SERP result dict."""
    return {
        "search_term": "coffee near london",
        "country": "UK",
        "places": [
            {
                "title": "Coffee Shop A",
                "rating": 4.5,
                "address": "123 Main St",
            }
        ],
        "place_count": 1,
    }


@pytest.fixture
def sample_enrichment_result():
    """Sample enrichment result dict."""
    return {
        "url": "https://example.com",
        "title": "Coffee Shop",
        "reviews": ["Great coffee!", "Amazing service"],
    }


class TestGetTTLTimestamp:
    """Tests for get_ttl_timestamp function."""

    def test_default_30_days(self):
        """Test TTL calculation with default 30 days."""
        ttl = get_ttl_timestamp(days=30)
        now = int(datetime.utcnow().timestamp())
        expected_min = now + (29 * 86400)
        expected_max = now + (31 * 86400)

        assert expected_min <= ttl <= expected_max

    def test_custom_days(self):
        """Test TTL calculation with custom days."""
        ttl = get_ttl_timestamp(days=7)
        now = int(datetime.utcnow().timestamp())
        expected_min = now + (6 * 86400)
        expected_max = now + (8 * 86400)

        assert expected_min <= ttl <= expected_max


class TestSaveSerpResult:
    """Tests for save_serp_result function."""

    def test_save_serp_result_success(self, mock_dynamodb, sample_serp_result):
        """Test successful SERP result save."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        result = save_serp_result(
            csv_name="test.csv",
            query_id="place_123",
            serp_result=sample_serp_result,
            search_term="coffee near london",
            country="UK",
        )

        mock_table.put_item.assert_called_once()
        call_args = mock_table.put_item.call_args
        item = call_args[1]["Item"]

        assert item["csv_name"] == "test.csv"
        assert item["ID"] == "place_123"
        assert item["search_term"] == "coffee near london"
        assert item["SERP_Status"] == "success"

    def test_save_serp_result_with_failed_places(
        self, mock_dynamodb, sample_serp_result
    ):
        """Test SERP result save with failed places list."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        failed = ["place_a", "place_b"]
        save_serp_result(
            csv_name="test.csv",
            query_id="place_123",
            serp_result=sample_serp_result,
            failed_places=failed,
        )

        call_args = mock_table.put_item.call_args
        item = call_args[1]["Item"]

        assert item["failed_places"] == failed


class TestUpdateSerpStatus:
    """Tests for update_serp_status function."""

    def test_update_status_success(self, mock_dynamodb):
        """Test successful SERP status update."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        update_serp_status(
            csv_name="test.csv",
            query_id="place_123",
            status="failed",
            retry_count=2,
        )

        mock_table.update_item.assert_called_once()
        call_args = mock_table.update_item.call_args

        assert call_args[1]["Key"]["csv_name"] == "test.csv"
        assert call_args[1]["Key"]["ID"] == "place_123"

    def test_update_status_with_failed_places(self, mock_dynamodb):
        """Test status update with failed places."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        failed = ["place_a"]
        update_serp_status(
            csv_name="test.csv",
            query_id="place_123",
            status="retrying",
            retry_count=1,
            failed_places=failed,
        )

        call_args = mock_table.update_item.call_args
        expr_vals = call_args[1]["ExpressionAttributeValues"]

        assert expr_vals[":failed"] == failed


class TestSaveEnrichmentResult:
    """Tests for save_enrichment_result function."""

    def test_save_enrichment_success(
        self, mock_dynamodb, sample_enrichment_result
    ):
        """Test successful enrichment result save."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        save_enrichment_result(
            csv_name="test.csv",
            query_id="place_123",
            enrichment_result=sample_enrichment_result,
        )

        mock_table.put_item.assert_called_once()
        call_args = mock_table.put_item.call_args
        item = call_args[1]["Item"]

        assert item["csv_name"] == "test.csv"
        assert item["ID"] == "place_123"
        assert item["Enrichment_Status"] == "received"


class TestUpdateBatchStatus:
    """Tests for update_batch_status function."""

    def test_update_batch_status_basic(self, mock_dynamodb):
        """Test basic batch status update."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        update_batch_status(
            csv_name="test.csv",
            status="in_progress",
        )

        mock_table.update_item.assert_called_once()
        call_args = mock_table.update_item.call_args

        assert call_args[1]["Key"]["csv_name"] == "test.csv"

    def test_update_batch_status_with_increments(self, mock_dynamodb):
        """Test batch status update with counter increments."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        update_batch_status(
            csv_name="test.csv",
            serp_increment=5,
            enrichment_increment=3,
        )

        call_args = mock_table.update_item.call_args
        expr_vals = call_args[1]["ExpressionAttributeValues"]

        assert expr_vals[":serp"] == 5
        assert expr_vals[":enrich"] == 3


class TestQueryByCsv:
    """Tests for query_by_csv function."""

    def test_query_by_csv_success(self, mock_dynamodb):
        """Test successful query by csv_name."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.query.return_value = {
            "Items": [
                {"csv_name": "test.csv", "ID": "place_1"},
                {"csv_name": "test.csv", "ID": "place_2"},
            ]
        }

        result = query_by_csv("GMapsQueries", "test.csv")

        assert len(result) == 2
        assert result[0]["ID"] == "place_1"


class TestGetBatchInfo:
    """Tests for get_batch_info function."""

    def test_get_batch_info_exists(self, mock_dynamodb):
        """Test retrieving existing batch info."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.return_value = {
            "Item": {
                "csv_name": "test.csv",
                "expected_count": 10,
                "status": "in_progress",
            }
        }

        result = get_batch_info("test.csv")

        assert result["csv_name"] == "test.csv"
        assert result["expected_count"] == 10

    def test_get_batch_info_not_exists(self, mock_dynamodb):
        """Test retrieving non-existent batch info."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.return_value = {}

        result = get_batch_info("nonexistent.csv")

        assert result == {}
