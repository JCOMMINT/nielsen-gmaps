"""S3 ingest Lambda: read CSV files and publish rows to SQS."""

import csv
import io
import json
import logging
import os
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "s3_ingest_to_sqs")

DEFAULT_REQUIRED_COLUMNS = ["search_term", "id", "country"]


def parse_s3_event(event):
    """Yield bucket/key pairs from an S3 event payload.

    Args:
        event: Lambda event containing S3 records.

    Yields:
        Tuple of (bucket, key) for each record.
    """
    records = event.get("Records", [])
    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        if bucket and key:
            yield bucket, unquote_plus(key)


def read_csv_from_s3(s3_client, bucket, key):
    """Load a CSV object from S3 into a list of dicts.

    Args:
        s3_client: Boto3 S3 client.
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        List of CSV rows as dictionaries.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(body))
    return list(reader)


def validate_columns(rows, required_columns):
    """Validate that required columns exist in the CSV header.

    Args:
        rows: List of CSV rows as dictionaries.
        required_columns: Column names required for processing.

    Raises:
        ValueError: If any required columns are missing.
    """
    if not rows:
        return
    missing = [col for col in required_columns if col not in rows[0]]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def build_messages(rows, source_bucket, source_key):
    """Build SQS message payloads from CSV rows.

    Args:
        rows: List of CSV rows as dictionaries.
        source_bucket: Source S3 bucket name.
        source_key: Source S3 key.

    Returns:
        List of message payloads.
    """
    messages = []
    for idx, row in enumerate(rows):
        payload = {
            "id": str(row.get("id", "")).strip(),
            "search_term": (row.get("search_term") or "").strip(),
            "country": (row.get("country") or "").strip(),
            "source_bucket": source_bucket,
            "source_key": source_key,
            "row_number": idx + 1,
        }
        messages.append(payload)
    return messages


def send_sqs_batches(sqs_client, queue_url, messages, batch_size=10):
    """Send messages to SQS in batches.

    Args:
        sqs_client: Boto3 SQS client.
        queue_url: Destination SQS queue URL.
        messages: List of message payloads.
        batch_size: Maximum messages per batch.

    Raises:
        RuntimeError: When any batch entries fail to send.
    """
    for start in range(0, len(messages), batch_size):
        batch = messages[start : start + batch_size]
        entries = [
            {"Id": str(idx), "MessageBody": json.dumps(message)}
            for idx, message in enumerate(batch)
        ]
        response = sqs_client.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries,
        )
        failures = response.get("Failed", [])
        if failures:
            raise RuntimeError(f"Failed to send {len(failures)} SQS messages")

def log_event(level, message, **fields):
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant.
        message: Short event message.
        **fields: Additional structured fields.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def lambda_handler(event, context):
    """Lambda entrypoint for S3 CSV ingestion.

    Args:
        event: Lambda event payload.
        context: Lambda context object.

    Returns:
        Summary of processed files and messages.

    Raises:
        ValueError: When required environment variables are missing.
    """
    queue_url = os.getenv("SQS_QUEUE_URL")
    if not queue_url:
        log_event(logging.CRITICAL, "missing_env", env="SQS_QUEUE_URL")
        raise ValueError("SQS_QUEUE_URL is required")
    required_columns = os.getenv("REQUIRED_COLUMNS")
    if required_columns:
        required_columns = [col.strip() for col in required_columns.split(",") if col.strip()]
    else:
        required_columns = DEFAULT_REQUIRED_COLUMNS

    s3_client = boto3.client("s3")
    sqs_client = boto3.client("sqs")

    processed_files = []
    total_messages = 0
    request_id = getattr(context, "aws_request_id", None)

    log_event(logging.INFO, "lambda_start", request_id=request_id)

    try:
        records = list(parse_s3_event(event))
        if not records:
            log_event(logging.WARNING, "no_records")

        for bucket, key in records:
            rows = read_csv_from_s3(s3_client, bucket, key)
            if not rows:
                log_event(logging.WARNING, "empty_csv", bucket=bucket, key=key)
                continue
            validate_columns(rows, required_columns)
            messages = build_messages(rows, bucket, key)
            send_sqs_batches(sqs_client, queue_url, messages)
            processed_files.append({"bucket": bucket, "key": key, "rows": len(rows)})
            total_messages += len(messages)
            log_event(logging.INFO, "file_processed", bucket=bucket, key=key, rows=len(rows))

        log_event(logging.INFO, "lambda_complete", sent=total_messages)
        return {"processed": processed_files, "sent": total_messages}
    except Exception as exc:
        logger.exception(
            json.dumps(
                {
                    "service": SERVICE_NAME,
                    "message": "lambda_failed",
                    "error": str(exc),
                    "request_id": request_id,
                },
                separators=(",", ":"),
            )
        )
        raise
