"""Enrichment Lambda: call Bright Data and store results in S3 Parquet."""

import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "enrich_to_s3")

def get_ddb_item(table, item_id):
    """Fetch a single item from DynamoDB.

    Args:
        table: DynamoDB table resource.
        item_id: Primary key value.

    Returns:
        DynamoDB item or None.
    """
    response = table.get_item(Key={"id": item_id})
    return response.get("Item")


def extract_urls(item, limit=None):
    """Extract place URLs from stored SERP results.

    Args:
        item: DynamoDB item containing SERP results.
        limit: Optional maximum number of URLs to return.

    Returns:
        List of place URLs.
    """
    if not item:
        return []
    results = item.get("serp_results") or []
    if isinstance(results, dict):
        results = [results]
    urls = []
    for result in results:
        if not isinstance(result, dict):
            continue
        for key in ("place_url", "url", "link", "website"):
            value = result.get(key)
            if value:
                urls.append(value)
                break
    if limit:
        return urls[:limit]
    return urls


def trigger_dataset(urls):
    """Trigger a Bright Data dataset enrichment job.

    Args:
        urls: List of place URLs to enrich.

    Returns:
        Bright Data API response JSON.
    """
    endpoint = os.getenv("BRIGHTDATA_TRIGGER_URL", "https://api.brightdata.com/datasets/v3/trigger")
    dataset_id = os.environ["BRIGHTDATA_DATASET_ID"]
    token = os.environ["BRIGHTDATA_TOKEN"]
    params = {"dataset_id": dataset_id, "include_errors": "true"}
    payload = [{"url": url} for url in urls]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.post(endpoint, headers=headers, params=params, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def request_direct(url):
    """Request enrichment directly from Bright Data.

    Args:
        url: Place URL to enrich.

    Returns:
        Dict containing URL and raw response content.
    """
    endpoint = os.getenv("BRIGHTDATA_REQUEST_URL", "https://api.brightdata.com/request")
    token = os.environ["BRIGHTDATA_TOKEN"]
    zone = os.environ["BRIGHTDATA_ZONE"]
    payload = {
        "zone": zone,
        "url": url,
        "format": "raw",
        "method": "GET",
        "direct": True,
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return {"url": url, "response": response.text}


def write_parquet_to_s3(s3_client, bucket, key, records):
    """Write records to S3 as a Parquet file.

    Args:
        s3_client: Boto3 S3 client.
        bucket: Destination bucket name.
        key: Destination object key.
        records: List of records to serialize.
    """
    table = pa.Table.from_pylist(records)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def build_s3_key(item_id):
    """Build an S3 key for enrichment output.

    Args:
        item_id: Identifier for the current item.

    Returns:
        S3 key path including date and item ID.
    """
    prefix = os.getenv("ENRICHMENT_PREFIX", "enrichment")
    date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
    return f"{prefix}/date={date_part}/id={item_id}/part-{timestamp}.parquet"

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
    """Lambda entrypoint for Bright Data enrichment.

    Args:
        event: Lambda event payload.
        context: Lambda context object.

    Returns:
        Summary of enrichment output location.

    Raises:
        ValueError: When required environment variables are missing.
    """
    item_id = event.get("id") or event.get("item_id")
    if not item_id:
        log_event(logging.CRITICAL, "missing_item_id")
        raise ValueError("event must include id")

    table_name = os.getenv("DDB_TABLE")
    if not table_name:
        log_event(logging.CRITICAL, "missing_env", env="DDB_TABLE")
        raise ValueError("DDB_TABLE is required")
    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)
    item = get_ddb_item(table, item_id)

    max_urls = int(os.getenv("MAX_URLS", "5"))
    urls = extract_urls(item, limit=max_urls)
    if not urls:
        log_event(logging.WARNING, "no_urls", id=item_id)
        return {"id": item_id, "status": "no_urls"}

    mode = os.getenv("BRIGHTDATA_MODE", "dataset")
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", id=item_id, request_id=request_id, mode=mode)

    try:
        if not os.getenv("BRIGHTDATA_TOKEN"):
            log_event(logging.CRITICAL, "missing_env", env="BRIGHTDATA_TOKEN")
            raise ValueError("BRIGHTDATA_TOKEN is required")
        if mode == "direct":
            if not os.getenv("BRIGHTDATA_ZONE"):
                log_event(logging.CRITICAL, "missing_env", env="BRIGHTDATA_ZONE")
                raise ValueError("BRIGHTDATA_ZONE is required for direct mode")
            records = [request_direct(url) for url in urls]
        else:
            if not os.getenv("BRIGHTDATA_DATASET_ID"):
                log_event(logging.CRITICAL, "missing_env", env="BRIGHTDATA_DATASET_ID")
                raise ValueError("BRIGHTDATA_DATASET_ID is required for dataset mode")
            trigger_response = trigger_dataset(urls)
            records = [{"id": item_id, "trigger": trigger_response, "urls": urls}]

        s3_bucket = os.getenv("ENRICHMENT_BUCKET")
        if not s3_bucket:
            log_event(logging.CRITICAL, "missing_env", env="ENRICHMENT_BUCKET")
            raise ValueError("ENRICHMENT_BUCKET is required")
        s3_key = build_s3_key(item_id)
        s3_client = boto3.client("s3")

        write_parquet_to_s3(s3_client, s3_bucket, s3_key, records)

        log_event(logging.INFO, "enrichment_stored", id=item_id, bucket=s3_bucket, key=s3_key)
        return {"id": item_id, "status": "enriched", "s3_bucket": s3_bucket, "s3_key": s3_key}
    except Exception as exc:
        logger.exception(
            json.dumps(
                {
                    "service": SERVICE_NAME,
                    "message": "lambda_failed",
                    "error": str(exc),
                    "request_id": request_id,
                    "id": item_id,
                },
                separators=(",", ":"),
            )
        )
        raise
