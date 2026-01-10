"""Tests for merge_and_finalize Lambda handler."""

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
os.environ["SERVICE_NAME"] = "merge_and_finalize"
os.environ["DYNAMODB_QUERIES_TABLE"] = "GMapsQueries"
os.environ["DYNAMODB_ENRICHMENT_TABLE"] = "GMapsEnrichment"
os.environ["DYNAMODB_BATCH_TABLE"] = "GMapsCSVBatch"
os.environ["FINAL_BUCKET"] = "final-bucket"
os.environ["NOTIFICATION_SNS_TOPIC"] = "arn:aws:sns:us-east-1:123456789012:notify"
os.environ["AWS_REGION"] = "us-east-1"

from src.handler import (
    lambda_handler,
    merge_serp_enrichment,
    write_final_json_to_s3,
    get_active_csvs,
    process_csv,
)


@pytest.fixture
def lambda_context():
    """Mock Lambda context."""
    context = MagicMock()
    context.aws_request_id = "test-request-id"
    return context


@pytest.fixture
def serp_items():
    """Sample SERP items from DynamoDB."""
    return [
        {
            "csv_name": "test.csv",
            "ID": "place_1",
            "SERP_Result": json.dumps({
                "search_term": "coffee",
                "country": "UK",
                "places": [{"title": "Coffee Shop A"}],
            }),
            "search_term": "coffee",
            "country": "UK",
            "created_at": 1000,
            "updated_at": 1001,
        }
    ]


@pytest.fixture
def enrichment_items():
    """Sample enrichment items from DynamoDB."""
    return [
        {
            "csv_name": "test.csv",
            "ID": "place_1",
            "Enrichment_Result": json.dumps({
                "url": "https://example.com",
                "data": "enrichment data",
            }),
        }
    ]


class TestMergeSerpEnrichment:
    """Tests for merge_serp_enrichment function."""

    def test_merge_with_enrichment(self, serp_items, enrichment_items):
        """Test merging SERP with enrichment."""
        result = merge_serp_enrichment(serp_items, enrichment_items)

        assert len(result) == 1
        assert result[0]["ID"] == "place_1"
        assert result[0]["status"] == "complete"
        assert result[0]["Enrichment"] is not None

    def test_merge_without_enrichment(self, serp_items):
        """Test merging SERP without enrichment."""
        result = merge_serp_enrichment(serp_items, [])

        assert len(result) == 1
        assert result[0]["status"] == "enrichment_pending"
        assert result[0]["Enrichment"] is None

    def test_merge_invalid_json(self):
        """Test merging with invalid JSON in items."""
        serp_items = [
            {
                "csv_name": "test.csv",
                "ID": "place_1",
                "SERP_Result": "invalid json",
                "search_term": "coffee",
            }
        ]
        result = merge_serp_enrichment(serp_items, [])

        assert len(result) == 1
        assert result[0]["SERP"] == {}


class TestWriteFinalJsonToS3:
    """Tests for write_final_json_to_s3 function."""

    @patch("src.handler.s3_client")
    def test_write_success(self, mock_s3):
        """Test successful write to S3."""
        final_json = {
            "csv_name": "test.csv",
            "status": "complete",
            "records": [],
        }

        result = write_final_json_to_s3("test.csv", final_json)

        assert result is True
        assert mock_s3.put_object.call_count == 2  # LATEST + timestamped


class TestGetActiveCsvs:
    """Tests for get_active_csvs function."""

    @patch("src.handler.get_batch_table")
    def test_get_active_csvs(self, mock_get_table):
        """Test retrieving active CSVs."""
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table
        mock_table.scan.return_value = {
            "Items": [
                {"csv_name": "test1.csv"},
                {"csv_name": "test2.csv"},
            ]
        }

        result = get_active_csvs()

        assert len(result) == 2
        assert "test1.csv" in result


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @patch("src.handler.process_csv")
    @patch("src.handler.get_active_csvs")
    def test_handler_success(
        self, mock_get_csvs, mock_process, lambda_context
    ):
        """Test successful lambda handler."""
        mock_get_csvs.return_value = ["test.csv"]

        response = lambda_handler({}, lambda_context)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["csvs_processed"] == 1

    @patch("src.handler.get_active_csvs")
    def test_handler_no_csvs(self, mock_get_csvs, lambda_context):
        """Test handler with no active CSVs."""
        mock_get_csvs.return_value = []

        response = lambda_handler({}, lambda_context)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["csvs_processed"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
