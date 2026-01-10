"""Enrichment Listener Lambda: Process Bright Data enrichment results.

This Lambda is triggered by S3 events (via SNS → SQS) when Bright Data
delivers enrichment results. It parses the enrichment JSON and saves
results to DynamoDB for later merging.
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import unquote

import boto3
from botocore.exceptions import ClientError

# Add shared utilities to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../shared'))

from dynamo_utils import (
    save_enrichment_result,
    update_batch_status,
)

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "enrichment_listener")

# AWS clients
s3_client = boto3.client("s3")


def log_event(level: int, message: str, **fields) -> None:
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant (e.g., logging.INFO).
        message: Short event message describing what happened.
        **fields: Additional structured fields for context.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def parse_sqs_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse SQS message containing S3 event notification.

    Args:
        event: Lambda event from SQS containing SNS message.

    Returns:
        Dict with s3_bucket and s3_key extracted from event.
    """
    try:
        record = event["Records"][0]
        message_body = json.loads(record["body"])
        
        # Handle SNS wrapped message
        if "Message" in message_body:
            s3_event = json.loads(message_body["Message"])
        else:
            s3_event = message_body

        # Extract S3 info from event
        s3_info = s3_event.get("Records", [{}])[0].get("s3", {})
        s3_bucket = s3_info.get("bucket", {}).get("name")
        s3_key = s3_info.get("object", {}).get("key")

        if s3_key:
            s3_key = unquote(s3_key)

        return {
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
        }
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        log_event(logging.ERROR, "failed_to_parse_sqs_event", error=str(e))
        return {"s3_bucket": None, "s3_key": None}


def parse_s3_key(s3_key: str) -> tuple[Optional[str], Optional[str]]:
    """Parse S3 key to extract csv_name and query_id.

    Expected key format:
        wsapi/{dataset_id}/{timestamp}/csv_name={csv}/ID={id}/results.json
    Or:
        enrichment/{date}/csv_name={csv}/ID={id}.json

    Args:
        s3_key: S3 object key path.

    Returns:
        Tuple of (csv_name, query_id) or (None, None) if parsing fails.
    """
    if not s3_key:
        return None, None

    # Try both patterns
    patterns = [
        r"csv_name=([^/]+)/ID=([^/]+)/",
        r"csv_name=([^/]+)/ID=([^/]+)\.",
    ]

    for pattern in patterns:
        match = re.search(pattern, s3_key)
        if match:
            csv_name = unquote(match.group(1))
            query_id = unquote(match.group(2))
            return csv_name, query_id

    log_event(logging.WARNING, "unable_to_parse_s3_key", s3_key=s3_key)
    return None, None


def read_enrichment_from_s3(
    bucket: str,
    key: str,
) -> Optional[Dict[str, Any]]:
    """Read and parse enrichment JSON from S3.

    Args:
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        Parsed JSON dict, or None if read/parse fails.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        enrichment_json = response["Body"].read().decode("utf-8")

        # Validate JSON
        parsed = json.loads(enrichment_json)
        log_event(
            logging.INFO,
            "read_enrichment_from_s3_success",
            bucket=bucket,
            key=key,
        )
        return parsed

    except ClientError as e:
        log_event(
            logging.ERROR,
            "s3_read_error",
            bucket=bucket,
            key=key,
            error=str(e),
        )
        return None
    except json.JSONDecodeError as e:
        log_event(
            logging.ERROR,
            "json_parse_error",
            bucket=bucket,
            key=key,
            error=str(e),
        )
        return None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entrypoint for enrichment listener.

    Reads enrichment JSON from S3 (triggered by S3 event via SNS → SQS),
    parses csv_name and query_id from S3 key, saves to DynamoDB,
    and updates batch status.

    Args:
        event: Lambda event from SQS.
        context: Lambda context object.

    Returns:
        Dict with statusCode and body containing processing result.
    """
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    try:
        # Parse SQS/SNS event
        s3_event = parse_sqs_event(event)
        s3_bucket = s3_event.get("s3_bucket")
        s3_key = s3_event.get("s3_key")

        if not s3_bucket or not s3_key:
            log_event(logging.ERROR, "missing_s3_bucket_or_key")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "missing_s3_bucket_or_key"}),
            }

        log_event(
            logging.INFO,
            "processing_enrichment",
            s3_bucket=s3_bucket,
            s3_key=s3_key,
        )

        # Parse S3 key to get csv_name and query_id
        csv_name, query_id = parse_s3_key(s3_key)

        if not csv_name or not query_id:
            log_event(
                logging.ERROR,
                "failed_to_parse_csv_and_query_id",
                s3_key=s3_key,
            )
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "failed_to_parse_s3_key"}),
            }

        log_event(
            logging.INFO,
            "parsed_s3_key",
            csv_name=csv_name,
            query_id=query_id,
        )

        # Read enrichment JSON from S3
        enrichment_json = read_enrichment_from_s3(s3_bucket, s3_key)

        if not enrichment_json:
            log_event(
                logging.ERROR,
                "failed_to_read_enrichment",
                csv_name=csv_name,
                query_id=query_id,
            )
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "failed_to_read_enrichment"}),
            }

        # Save enrichment to DynamoDB
        try:
            save_enrichment_result(
                csv_name=csv_name,
                query_id=query_id,
                enrichment_result=enrichment_json,
                s3_key=f"s3://{s3_bucket}/{s3_key}",
            )
        except Exception as e:
            log_event(
                logging.ERROR,
                "failed_to_save_enrichment_to_dynamo",
                csv_name=csv_name,
                query_id=query_id,
                error=str(e),
            )
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "failed_to_save_enrichment"}),
            }

        # Update batch status
        try:
            update_batch_status(
                csv_name=csv_name,
                enrichment_increment=1,
            )
        except Exception as e:
            log_event(
                logging.ERROR,
                "failed_to_update_batch_status",
                csv_name=csv_name,
                error=str(e),
            )
            # Don't fail the Lambda if batch update fails

        log_event(
            logging.INFO,
            "enrichment_processed_success",
            csv_name=csv_name,
            query_id=query_id,
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "enrichment_processed",
                "csv_name": csv_name,
                "query_id": query_id,
            }),
        }

    except Exception as e:
        log_event(logging.CRITICAL, "lambda_error", error=str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
