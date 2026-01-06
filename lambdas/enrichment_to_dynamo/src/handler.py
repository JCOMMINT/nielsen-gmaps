"""Enrichment ingest Lambda: read Parquet from S3 and update DynamoDB."""

import io
import json
import logging
import os
import re
from urllib.parse import unquote_plus

import boto3
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "enrichment_to_dynamo")

ID_IN_KEY = re.compile(r"id=([^/]+)/")


def parse_event(event):
    """Yield payloads from S3 or direct invocation events.

    Args:
        event: Lambda event payload.

    Yields:
        Dicts containing bucket, key, and optional id.
    """
    if "Records" in event:
        for record in event.get("Records", []):
            if record.get("eventSource") != "aws:s3":
                continue
            bucket = record.get("s3", {}).get("bucket", {}).get("name")
            key = record.get("s3", {}).get("object", {}).get("key")
            if bucket and key:
                yield {"bucket": bucket, "key": unquote_plus(key)}
    else:
        bucket = event.get("s3_bucket")
        key = event.get("s3_key")
        if bucket and key:
            yield {"bucket": bucket, "key": key, "id": event.get("id")}


def parse_id_from_key(key):
    """Extract the item ID from a structured S3 key.

    Args:
        key: S3 object key.

    Returns:
        Extracted item ID or None.
    """
    match = ID_IN_KEY.search(key)
    if match:
        return match.group(1)
    return None


def read_parquet_from_s3(s3_client, bucket, key):
    """Read Parquet data from S3 into Python objects.

    Args:
        s3_client: Boto3 S3 client.
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        List of records from the Parquet file.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return table.to_pylist()


def update_item(table, item_id, bucket, key, records):
    """Update a DynamoDB item with enrichment metadata.

    Args:
        table: DynamoDB table resource.
        item_id: Item primary key value.
        bucket: S3 bucket containing enrichment output.
        key: S3 key containing enrichment output.
        records: Parsed enrichment records.
    """
    sample = records[0] if records else None
    table.update_item(
        Key={"id": item_id},
        UpdateExpression=(
            "SET enrichment_s3_bucket = :bucket, enrichment_s3_key = :key, "
            "enrichment_count = :count, enrichment_sample = :sample, status = :status"
        ),
        ExpressionAttributeValues={
            ":bucket": bucket,
            ":key": key,
            ":count": len(records),
            ":sample": sample,
            ":status": "ENRICHED",
        },
    )

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
    """Lambda entrypoint for enrichment ingestion.

    Args:
        event: Lambda event payload.
        context: Lambda context object.

    Returns:
        Summary of updated items.

    Raises:
        ValueError: When required environment variables are missing.
    """
    table_name = os.getenv("DDB_TABLE")
    if not table_name:
        log_event(logging.CRITICAL, "missing_env", env="DDB_TABLE")
        raise ValueError("DDB_TABLE is required")
    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)
    s3_client = boto3.client("s3")

    updated = []
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    try:
        payloads = list(parse_event(event))
        if not payloads:
            log_event(logging.WARNING, "no_records")

        for payload in payloads:
            bucket = payload["bucket"]
            key = payload["key"]
            item_id = payload.get("id") or parse_id_from_key(key)
            if not item_id:
                log_event(logging.CRITICAL, "missing_item_id", bucket=bucket, key=key)
                raise ValueError("missing item id")

            records = read_parquet_from_s3(s3_client, bucket, key)
            update_item(table, item_id, bucket, key, records)
            updated.append({"id": item_id, "bucket": bucket, "key": key})
            log_event(logging.INFO, "enrichment_updated", id=item_id, bucket=bucket, key=key)

        log_event(logging.INFO, "lambda_complete", updated=len(updated))
        return {"updated": updated}
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
