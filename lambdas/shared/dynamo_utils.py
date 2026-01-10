"""DynamoDB utility functions for GMaps pipeline.

This module provides helper functions for reading/writing to DynamoDB tables
that manage SERP and enrichment data.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Initialize DynamoDB resource
dynamodb = boto3.resource(
    "dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")
)


def get_queries_table() -> Any:
    """Get GMapsQueries table resource.

    Returns:
        boto3 DynamoDB Table resource for GMapsQueries.
    """
    table_name = os.getenv("DYNAMODB_QUERIES_TABLE", "GMapsQueries")
    return dynamodb.Table(table_name)


def get_enrichment_table() -> Any:
    """Get GMapsEnrichment table resource.

    Returns:
        boto3 DynamoDB Table resource for GMapsEnrichment.
    """
    table_name = os.getenv("DYNAMODB_ENRICHMENT_TABLE", "GMapsEnrichment")
    return dynamodb.Table(table_name)


def get_batch_table() -> Any:
    """Get GMapsCSVBatch table resource.

    Returns:
        boto3 DynamoDB Table resource for GMapsCSVBatch.
    """
    table_name = os.getenv("DYNAMODB_BATCH_TABLE", "GMapsCSVBatch")
    return dynamodb.Table(table_name)


def get_ttl_timestamp(days: int = 30) -> int:
    """Calculate TTL timestamp for DynamoDB item.

    Args:
        days: Number of days from now until TTL expiration.

    Returns:
        Unix timestamp of TTL expiration date.
    """
    expiration = datetime.utcnow() + timedelta(days=days)
    return int(expiration.timestamp())


def save_serp_result(
    csv_name: str,
    query_id: str,
    serp_result: Dict[str, Any],
    s3_key: Optional[str] = None,
    retry_count: int = 0,
    failed_places: Optional[List[str]] = None,
    search_term: Optional[str] = None,
    country: Optional[str] = None,
    ttl_days: int = 30,
) -> Dict[str, Any]:
    """Save SERP result to DynamoDB GMapsQueries table.

    Args:
        csv_name: Name of the input CSV file.
        query_id: Unique identifier for the query.
        serp_result: Parsed SERP JSON result dict.
        s3_key: Optional S3 key where full result is persisted.
        retry_count: Number of retries attempted.
        failed_places: List of place IDs that failed to fetch.
        search_term: Original search query term.
        country: Country code for the search.
        ttl_days: Days until item expires in DynamoDB.

    Returns:
        DynamoDB put_item response.

    Raises:
        ClientError: If DynamoDB write fails.
    """
    table = get_queries_table()
    now = int(datetime.utcnow().timestamp())
    ttl = get_ttl_timestamp(ttl_days)

    item = {
        "csv_name": csv_name,
        "ID": query_id,
        "SERP_Result": json.dumps(serp_result),
        "SERP_Status": "success",
        "S3_Key": s3_key or "",
        "retry_count": retry_count,
        "failed_places": failed_places or [],
        "search_term": search_term or "",
        "country": country or "",
        "updated_at": now,
        "created_at": now,
        "TTL": ttl,
    }

    try:
        response = table.put_item(Item=item)
        logger.info(
            "SERP result saved to DynamoDB",
            extra={
                "csv_name": csv_name,
                "query_id": query_id,
                "status": "success",
            },
        )
        return response
    except ClientError as e:
        logger.error(
            "Failed to save SERP result to DynamoDB",
            extra={
                "csv_name": csv_name,
                "query_id": query_id,
                "error": str(e),
            },
        )
        raise


def update_serp_status(
    csv_name: str,
    query_id: str,
    status: str,
    retry_count: int,
    failed_places: Optional[List[str]] = None,
    ttl_days: int = 30,
) -> Dict[str, Any]:
    """Update SERP status for a query in DynamoDB.

    Args:
        csv_name: Name of the input CSV file.
        query_id: Unique identifier for the query.
        status: Status value ('success', 'failed', 'retrying').
        retry_count: Current retry count.
        failed_places: List of place IDs that failed.
        ttl_days: Days until item expires.

    Returns:
        DynamoDB update_item response.

    Raises:
        ClientError: If DynamoDB update fails.
    """
    table = get_queries_table()
    now = int(datetime.utcnow().timestamp())
    ttl = get_ttl_timestamp(ttl_days)

    update_expr = "SET SERP_Status = :status, retry_count = :retry, updated_at = :updated, TTL = :ttl"
    expr_vals = {
        ":status": status,
        ":retry": retry_count,
        ":updated": now,
        ":ttl": ttl,
    }

    if failed_places:
        update_expr += ", failed_places = :failed"
        expr_vals[":failed"] = failed_places

    try:
        response = table.update_item(
            Key={"csv_name": csv_name, "ID": query_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_vals,
            ReturnValues="ALL_NEW",
        )
        logger.info(
            "SERP status updated in DynamoDB",
            extra={"csv_name": csv_name, "query_id": query_id, "status": status},
        )
        return response
    except ClientError as e:
        logger.error(
            "Failed to update SERP status in DynamoDB",
            extra={
                "csv_name": csv_name,
                "query_id": query_id,
                "error": str(e),
            },
        )
        raise


def save_enrichment_result(
    csv_name: str,
    query_id: str,
    enrichment_result: Dict[str, Any],
    s3_key: Optional[str] = None,
    ttl_days: int = 30,
) -> Dict[str, Any]:
    """Save enrichment result to DynamoDB GMapsEnrichment table.

    Args:
        csv_name: Name of the input CSV file.
        query_id: Unique identifier for the query.
        enrichment_result: Enrichment JSON result dict from Bright Data.
        s3_key: S3 key where full result is persisted.
        ttl_days: Days until item expires.

    Returns:
        DynamoDB put_item response.

    Raises:
        ClientError: If DynamoDB write fails.
    """
    table = get_enrichment_table()
    now = int(datetime.utcnow().timestamp())
    ttl = get_ttl_timestamp(ttl_days)

    item = {
        "csv_name": csv_name,
        "ID": query_id,
        "Enrichment_Result": json.dumps(enrichment_result),
        "Enrichment_Status": "received",
        "S3_Key": s3_key or "",
        "received_at": now,
        "updated_at": now,
        "TTL": ttl,
    }

    try:
        response = table.put_item(Item=item)
        logger.info(
            "Enrichment result saved to DynamoDB",
            extra={"csv_name": csv_name, "query_id": query_id},
        )
        return response
    except ClientError as e:
        logger.error(
            "Failed to save enrichment result to DynamoDB",
            extra={
                "csv_name": csv_name,
                "query_id": query_id,
                "error": str(e),
            },
        )
        raise


def query_by_csv(table_name: str, csv_name: str) -> List[Dict[str, Any]]:
    """Query DynamoDB table by csv_name.

    Args:
        table_name: Name of the table to query.
        csv_name: CSV name to filter by.

    Returns:
        List of items matching the csv_name.

    Raises:
        ClientError: If DynamoDB query fails.
    """
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression="csv_name = :csv",
            ExpressionAttributeValues={":csv": csv_name},
        )

        items = response.get("Items", [])
        logger.info(
            "Queried DynamoDB table",
            extra={"table": table_name, "csv_name": csv_name, "count": len(items)},
        )
        return items

    except ClientError as e:
        logger.error(
            "Failed to query DynamoDB table",
            extra={"table": table_name, "csv_name": csv_name, "error": str(e)},
        )
        raise


def get_batch_info(csv_name: str) -> Dict[str, Any]:
    """Get batch metadata from GMapsCSVBatch table.

    Args:
        csv_name: CSV name to retrieve metadata for.

    Returns:
        Batch metadata dict, empty dict if not found.
    """
    table = get_batch_table()

    try:
        response = table.get_item(Key={"csv_name": csv_name})
        item = response.get("Item", {})
        logger.info(
            "Retrieved batch info from DynamoDB",
            extra={"csv_name": csv_name, "found": bool(item)},
        )
        return item
    except ClientError as e:
        logger.error(
            "Failed to get batch info from DynamoDB",
            extra={"csv_name": csv_name, "error": str(e)},
        )
        return {}


def update_batch_status(
    csv_name: str,
    expected_count: Optional[int] = None,
    status: Optional[str] = None,
    serp_increment: int = 0,
    enrichment_increment: int = 0,
    last_merge_id: Optional[str] = None,
    ttl_days: int = 30,
) -> Dict[str, Any]:
    """Update batch-level metadata in GMapsCSVBatch table.

    Args:
        csv_name: CSV name identifying the batch.
        expected_count: Total expected queries in batch (optional).
        status: Batch status ('in_progress', 'complete', 'partial', 'failed').
        serp_increment: Number to add to serp_completed_count.
        enrichment_increment: Number to add to enrichment_completed_count.
        last_merge_id: ID of the most recent merge operation.
        ttl_days: Days until item expires.

    Returns:
        DynamoDB update_item response.

    Raises:
        ClientError: If DynamoDB update fails.
    """
    table = get_batch_table()
    now = int(datetime.utcnow().timestamp())
    ttl = get_ttl_timestamp(ttl_days)

    update_expr_parts = ["SET updated_at = :now", "TTL = :ttl"]
    expr_vals = {":now": now, ":ttl": ttl}

    if expected_count is not None:
        update_expr_parts.append("expected_count = :expected")
        expr_vals[":expected"] = expected_count

    if status:
        update_expr_parts.append("#status = :status")
        expr_vals[":status"] = status

    if serp_increment > 0:
        update_expr_parts.append("ADD serp_completed_count :serp")
        expr_vals[":serp"] = serp_increment

    if enrichment_increment > 0:
        update_expr_parts.append("ADD enrichment_completed_count :enrich")
        expr_vals[":enrich"] = enrichment_increment

    if last_merge_id:
        update_expr_parts.append("last_merge_id = :merge_id")
        expr_vals[":merge_id"] = last_merge_id

    update_expr = ", ".join(update_expr_parts)
    attr_names = {}

    if "#status" in update_expr:
        attr_names["#status"] = "status"

    try:
        update_kwargs = {
            "Key": {"csv_name": csv_name},
            "UpdateExpression": update_expr,
            "ExpressionAttributeValues": expr_vals,
            "ReturnValues": "ALL_NEW",
        }

        if attr_names:
            update_kwargs["ExpressionAttributeNames"] = attr_names

        response = table.update_item(**update_kwargs)
        logger.info(
            "Batch status updated in DynamoDB",
            extra={
                "csv_name": csv_name,
                "status": status,
                "serp_increment": serp_increment,
                "enrichment_increment": enrichment_increment,
            },
        )
        return response
    except ClientError as e:
        logger.error(
            "Failed to update batch status in DynamoDB",
            extra={"csv_name": csv_name, "error": str(e)},
        )
        raise
