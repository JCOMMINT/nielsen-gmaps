"""SERP fetcher Lambda: query Google Maps and store results in DynamoDB."""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "serp_fetcher")


def parse_sqs_event(event):
    """Yield message bodies from an SQS event payload.

    Args:
        event: Lambda event containing SQS records.

    Yields:
        Parsed message dictionaries.
    """
    for record in event.get("Records", []):
        body = record.get("body") or record.get("Body")
        if not body:
            continue
        yield json.loads(body)


def build_proxy_config():
    """Build proxy configuration for Bright Data superproxy.

    Returns:
        Proxies dict for requests, or None if credentials are missing.
    """
    username = os.getenv("BRIGHTDATA_USERNAME")
    password = os.getenv("BRIGHTDATA_PASSWORD")
    port = os.getenv("BRIGHTDATA_PORT", "33335")
    host = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")

    if not username or not password:
        log_event(logging.WARNING, "proxy_disabled_missing_credentials")
        return None

    session_id = uuid.uuid4().hex
    proxy_url = f"http://{username}-session-{session_id}:{password}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}


def build_serp_params():
    """Build SERP request query parameters.

    Returns:
        Dictionary of query parameters.
    """
    params = {"lum_json": 1}
    extra = os.getenv("SERP_PARAMS_JSON")
    if extra:
        try:
            params.update(json.loads(extra))
        except json.JSONDecodeError:
            logger.warning("invalid SERP_PARAMS_JSON")
    return params


def fetch_serp(search_term, proxies):
    """Fetch SERP results for a search term.

    Args:
        search_term: Query string used for Maps search.
        proxies: Requests proxy configuration or None.

    Returns:
        JSON response if available, otherwise raw HTML wrapped in a dict.
    """
    base_url = os.getenv("SERP_BASE_URL", "https://www.google.com/maps/search/")
    url = base_url.rstrip("/") + "/" + quote(search_term)
    timeout = int(os.getenv("REQUEST_TIMEOUT", "10"))
    verify_tls = os.getenv("VERIFY_TLS", "true").lower() == "true"
    headers = {"User-Agent": os.getenv("USER_AGENT", "Mozilla/5.0")}

    response = requests.get(
        url,
        params=build_serp_params(),
        proxies=proxies,
        timeout=timeout,
        verify=verify_tls,
        headers=headers,
    )
    response.raise_for_status()

    try:
        return response.json()
    except ValueError:
        return {"raw_html": response.text}

def log_event(level, message, **fields):
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant.
        message: Short event message.
        **fields: Additional structured fields.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def write_to_dynamo(table, item_id, payload, message):
    """Write SERP results to DynamoDB.

    Args:
        table: DynamoDB table resource.
        item_id: Primary key value.
        payload: SERP results payload to store.
        message: Original SQS message.
    """
    table.put_item(
        Item={
            "id": item_id,
            "search_term": message.get("search_term"),
            "country": message.get("country"),
            "source_bucket": message.get("source_bucket"),
            "source_key": message.get("source_key"),
            "serp_results": payload,
            "serp_fetched_at": datetime.now(timezone.utc).isoformat(),
            "status": "SERP_FETCHED",
        }
    )


def lambda_handler(event, context):
    """Lambda entrypoint for SERP fetching.

    Args:
        event: Lambda event payload.
        context: Lambda context object.

    Returns:
        Summary of processed items.

    Raises:
        ValueError: When required environment variables are missing.
    """
    table_name = os.getenv("DDB_TABLE")
    if not table_name:
        log_event(logging.CRITICAL, "missing_env", env="DDB_TABLE")
        raise ValueError("DDB_TABLE is required")
    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    proxies = build_proxy_config()
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    results = []
    try:
        messages = list(parse_sqs_event(event))
        if not messages:
            log_event(logging.WARNING, "no_records")

        for message in messages:
            item_id = message.get("id") or str(uuid.uuid4())
            search_term = message.get("search_term", "")
            if not search_term:
                log_event(logging.WARNING, "missing_search_term", id=item_id)
            serp_payload = fetch_serp(search_term, proxies)
            serp_results = serp_payload.get("organic", serp_payload)
            write_to_dynamo(table, item_id, serp_results, message)
            results.append({"id": item_id, "status": "stored"})
            log_event(logging.INFO, "serp_stored", id=item_id)

        log_event(logging.INFO, "lambda_complete", processed=len(results))
        return {"processed": results}
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
