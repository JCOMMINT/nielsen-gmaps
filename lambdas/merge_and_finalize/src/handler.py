"""Merge and Finalize Lambda: Merge SERP and enrichment results.

This Lambda runs on a schedule (every 2 hours) and:
1. Queries DynamoDB for completed SERP and enrichment results
2. Merges SERP and enrichment data by query ID
3. Writes final JSON output to S3
4. Updates batch status
5. Sends notifications to SNS
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

# Add shared utilities to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../shared'))

from dynamo_utils import (
    get_queries_table,
    get_enrichment_table,
    get_batch_table,
    query_by_csv,
    get_batch_info,
    update_batch_status,
)

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "merge_and_finalize")

# AWS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
dynamodb = boto3.resource(
    "dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")
)


def log_event(level: int, message: str, **fields) -> None:
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant (e.g., logging.INFO).
        message: Short event message describing what happened.
        **fields: Additional structured fields for context.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def get_active_csvs() -> List[str]:
    """Get list of active CSV batches from DynamoDB.

    Scans GMapsCSVBatch table for items that haven't expired.

    Returns:
        List of csv_name values for active batches.
    """
    batch_table = get_batch_table()

    try:
        response = batch_table.scan(
            FilterExpression="attribute_not_exists(TTL) OR #ttl > :now",
            ExpressionAttributeNames={"#ttl": "TTL"},
            ExpressionAttributeValues={":now": int(datetime.utcnow().timestamp())},
            ProjectionExpression="csv_name",
        )

        csv_names = [item["csv_name"] for item in response.get("Items", [])]
        log_event(
            logging.INFO,
            "retrieved_active_csvs",
            count=len(csv_names),
        )
        return csv_names

    except ClientError as e:
        log_event(logging.ERROR, "failed_to_get_active_csvs", error=str(e))
        return []


def merge_serp_enrichment(
    serp_items: List[Dict[str, Any]],
    enrichment_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge SERP and enrichment results by query ID.

    Creates a list of records with both SERP and enrichment data.
    Records missing enrichment are marked as 'enrichment_pending'.

    Args:
        serp_items: List of SERP items from DynamoDB.
        enrichment_items: List of enrichment items from DynamoDB.

    Returns:
        List of merged record dicts.
    """
    # Build map of enrichment by ID for fast lookup
    enrichment_map = {
        item["ID"]: item for item in enrichment_items
    }

    merged = []
    for serp in serp_items:
        query_id = serp["ID"]
        enrichment = enrichment_map.get(query_id)

        # Parse JSON strings from DynamoDB
        try:
            serp_result = json.loads(serp.get("SERP_Result", "{}"))
        except json.JSONDecodeError:
            log_event(
                logging.WARNING,
                "failed_to_parse_serp_json",
                query_id=query_id,
            )
            serp_result = {}

        try:
            enrichment_result = (
                json.loads(enrichment["Enrichment_Result"])
                if enrichment
                else None
            )
        except json.JSONDecodeError:
            log_event(
                logging.WARNING,
                "failed_to_parse_enrichment_json",
                query_id=query_id,
            )
            enrichment_result = None

        record = {
            "ID": query_id,
            "search_term": serp.get("search_term"),
            "country": serp.get("country"),
            "SERP": serp_result,
            "Enrichment": enrichment_result,
            "status": "complete" if enrichment else "enrichment_pending",
            "created_at": serp.get("created_at"),
            "updated_at": serp.get("updated_at"),
        }

        merged.append(record)

    log_event(
        logging.INFO,
        "merged_serp_and_enrichment",
        count=len(merged),
    )
    return merged


def write_final_json_to_s3(
    csv_name: str,
    final_json: Dict[str, Any],
) -> bool:
    """Write final merged JSON to S3.

    Writes both a timestamped version and LATEST.json for easy access.

    Args:
        csv_name: CSV name for S3 key partitioning.
        final_json: Final merged JSON object.

    Returns:
        True if write successful, False otherwise.
    """
    final_bucket = os.getenv("FINAL_BUCKET")
    if not final_bucket:
        log_event(logging.ERROR, "final_bucket_not_configured")
        return False

    # Write LATEST.json (always overwrite)
    latest_key = f"final/{csv_name}/LATEST.json"

    # Write timestamped version (append-only)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    timestamped_key = f"final/{csv_name}/results_{timestamp}.json"

    try:
        s3_client.put_object(
            Bucket=final_bucket,
            Key=latest_key,
            Body=json.dumps(final_json, indent=2),
            ContentType="application/json",
        )

        s3_client.put_object(
            Bucket=final_bucket,
            Key=timestamped_key,
            Body=json.dumps(final_json, indent=2),
            ContentType="application/json",
        )

        log_event(
            logging.INFO,
            "final_json_written_to_s3",
            csv_name=csv_name,
            latest_key=latest_key,
            timestamped_key=timestamped_key,
        )
        return True

    except ClientError as e:
        log_event(
            logging.ERROR,
            "failed_to_write_final_json",
            csv_name=csv_name,
            error=str(e),
        )
        return False


def send_notification(
    csv_name: str,
    status: str,
    records_count: int,
    expected_count: int,
) -> None:
    """Send SNS notification about merge completion.

    Args:
        csv_name: CSV name being processed.
        status: Processing status ('complete', 'partial', etc.).
        records_count: Number of merged records.
        expected_count: Expected number of records.
    """
    notification_topic = os.getenv("NOTIFICATION_SNS_TOPIC")
    if not notification_topic:
        log_event(logging.DEBUG, "notification_disabled")
        return

    coverage_pct = (
        (records_count / expected_count * 100)
        if expected_count > 0
        else 0
    )

    message = (
        f"GMaps Pipeline Notification\n\n"
        f"CSV: {csv_name}\n"
        f"Status: {status.upper()}\n"
        f"Records Merged: {records_count}/{expected_count} ({coverage_pct:.1f}%)\n"
        f"\n"
        f"Final results available at:\n"
        f"s3://{os.getenv('FINAL_BUCKET')}/final/{csv_name}/LATEST.json"
    )

    try:
        sns_client.publish(
            TopicArn=notification_topic,
            Subject=f"GMaps Pipeline: {csv_name} - {status.upper()}",
            Message=message,
        )
        log_event(
            logging.INFO,
            "notification_sent",
            csv_name=csv_name,
            status=status,
        )
    except ClientError as e:
        log_event(
            logging.ERROR,
            "failed_to_send_notification",
            csv_name=csv_name,
            error=str(e),
        )


def process_csv(csv_name: str) -> None:
    """Process a single CSV: merge SERP + enrichment and finalize output.

    Args:
        csv_name: CSV name to process.
    """
    log_event(logging.INFO, "processing_csv", csv_name=csv_name)

    # Query SERP results by csv_name
    try:
        serp_items = query_by_csv("GMapsQueries", csv_name)
        log_event(
            logging.INFO,
            "retrieved_serp_items",
            csv_name=csv_name,
            count=len(serp_items),
        )
    except Exception as e:
        log_event(
            logging.ERROR,
            "failed_to_query_serp_items",
            csv_name=csv_name,
            error=str(e),
        )
        return

    # Query enrichment results by csv_name
    try:
        enrichment_items = query_by_csv("GMapsEnrichment", csv_name)
        log_event(
            logging.INFO,
            "retrieved_enrichment_items",
            csv_name=csv_name,
            count=len(enrichment_items),
        )
    except Exception as e:
        log_event(
            logging.ERROR,
            "failed_to_query_enrichment_items",
            csv_name=csv_name,
            error=str(e),
        )
        return

    # Get batch metadata
    batch_info = get_batch_info(csv_name)
    expected_count = batch_info.get("expected_count", len(serp_items))

    # Merge SERP and enrichment
    merged_records = merge_serp_enrichment(serp_items, enrichment_items)

    # Determine completion status
    serp_complete = len(serp_items) >= expected_count
    enrichment_complete = len(enrichment_items) >= len(serp_items)

    if serp_complete and enrichment_complete:
        status = "complete"
    elif len(enrichment_items) > 0:
        status = "partial"
    else:
        status = "pending"

    # Create final JSON
    merge_id = datetime.utcnow().isoformat() + "Z"
    final_json = {
        "csv_name": csv_name,
        "merge_id": merge_id,
        "expected_count": expected_count,
        "serp_received": len(serp_items),
        "enrichment_received": len(enrichment_items),
        "status": status,
        "coverage_pct": (
            len(enrichment_items) / len(serp_items) * 100
            if len(serp_items) > 0
            else 0
        ),
        "records": merged_records,
        "created_at": datetime.utcnow().isoformat(),
    }

    # Write final JSON to S3
    write_success = write_final_json_to_s3(csv_name, final_json)

    if write_success:
        # Update batch status
        try:
            update_batch_status(
                csv_name=csv_name,
                status=status,
                last_merge_id=merge_id,
            )
        except Exception as e:
            log_event(
                logging.ERROR,
                "failed_to_update_batch_status",
                csv_name=csv_name,
                error=str(e),
            )

        # Send notification
        send_notification(
            csv_name=csv_name,
            status=status,
            records_count=len(merged_records),
            expected_count=expected_count,
        )

        log_event(
            logging.INFO,
            "csv_processing_complete",
            csv_name=csv_name,
            status=status,
            records=len(merged_records),
        )
    else:
        log_event(
            logging.ERROR,
            "csv_processing_failed_to_write_output",
            csv_name=csv_name,
        )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entrypoint for merge and finalize job.

    Runs on a schedule and processes all active CSV batches:
    1. Queries DynamoDB for SERP and enrichment results
    2. Merges data by query ID
    3. Writes final JSON to S3
    4. Sends notifications

    Args:
        event: Lambda event (from CloudWatch Events).
        context: Lambda context object.

    Returns:
        Dict with statusCode and body containing processing summary.
    """
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    try:
        # Get list of active CSVs
        active_csvs = get_active_csvs()

        if not active_csvs:
            log_event(logging.INFO, "no_active_csvs")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "no_active_csvs",
                    "csvs_processed": 0,
                }),
            }

        log_event(logging.INFO, "processing_csvs", count=len(active_csvs))

        # Process each CSV
        for csv_name in active_csvs:
            try:
                process_csv(csv_name)
            except Exception as e:
                log_event(
                    logging.ERROR,
                    "error_processing_csv",
                    csv_name=csv_name,
                    error=str(e),
                )
                continue

        log_event(
            logging.INFO,
            "lambda_complete",
            csvs_processed=len(active_csvs),
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "merge_and_finalize_completed",
                "csvs_processed": len(active_csvs),
            }),
        }

    except Exception as e:
        log_event(logging.CRITICAL, "lambda_error", error=str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
