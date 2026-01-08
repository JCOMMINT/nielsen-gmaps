"""SERP parsing Lambda: fetch and persist normalized SERP results."""

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
TAG_GROUP_MAP = {
    "highlights": "highlights",
    "service options": "serviceOptions",
    "offerings": "offerings",
    "dining options": "diningOptions",
    "amenities": "amenities",
    "atmosphere": "atmosphere",
    "crowd": "crowd",
    "payments": "payments",
    "children": "children",
    "accessibility": "accessibility",
}
REQUIRED_FIELDS = [
    "title",
    "googleplaceid",
    "priceBracket",
    "starRating",
    "number_of_reviews",
    "venueType",
    "website",
    "address",
    "openingHours",
    "menu",
    "telephoneNumber",
    "highlights",
    "serviceOptions",
    "offerings",
    "diningOptions",
    "amenities",
    "atmosphere",
    "crowd",
    "payments",
    "children",
    "trading_status",
    "busy_times",
    "accessibility",
    "latitude",
    "longitude",
    "hotel_features",
]


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
    timeout = int(os.getenv("REQUEST_TIMEOUT", "20"))
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


def normalize_tags(tags):
    """Group tag values by normalized category names.

    Args:
        tags: List of tag dictionaries from SERP results.

    Returns:
        Dict mapping category keys to lists of tag titles.
    """
    grouped = {}
    for tag in tags or []:
        if not isinstance(tag, dict):
            continue
        group_title = (tag.get("group_title") or "").strip().lower()
        if not group_title:
            continue
        key = TAG_GROUP_MAP.get(group_title)
        if not key:
            continue
        value = tag.get("value_title") or tag.get("value_title_short")
        if not value:
            continue
        grouped.setdefault(key, set()).add(value)
    return {key: sorted(values) for key, values in grouped.items()}


def derive_trading_status(work_status):
    """Derive a trading status from the work status text.

    Args:
        work_status: Status string from SERP results.

    Returns:
        Normalized status string or None.
    """
    if not work_status:
        return None
    status = work_status.lower()
    if "permanently closed" in status:
        return "permanently_closed"
    if status.startswith("closed"):
        return "closed"
    if status.startswith("open"):
        return "open"
    return None


def ensure_required_fields(place):
    """Ensure required fields exist in a place record.

    Args:
        place: Place record to normalize.

    Returns:
        Place record with missing required fields filled.
    """
    for field in REQUIRED_FIELDS:
        value = place.get(field)
        if value is None or value == []:
            place[field] = "Not Found"
    return place


def normalize_place_id(place_id):
    """Normalize a place identifier for enrichment calls.

    Args:
        place_id: Raw place identifier.

    Returns:
        Normalized place identifier or None.
    """
    if not place_id:
        return None
    return str(place_id).strip()


def build_place_item(result):
    """Build a base place record from a SERP result entry.

    Args:
        result: SERP result dictionary.

    Returns:
        Dictionary with SERP-derived fields.
    """
    tags = normalize_tags(result.get("tags"))
    work_status = result.get("work_status")
    categories = result.get("category", [])
    venuetype = [c.get("title") for c in categories if isinstance(c, dict)]
    raw_place_id = result.get("map_id_encoded") or result.get("map_id") or result.get("fid")
    place_id = normalize_place_id(raw_place_id)
    google_place_id = normalize_place_id(result.get("map_id_encoded") or result.get("map_id"))

    place = {
        "place_id": place_id,
        "googleplaceid": google_place_id,
        "title": result.get("title"),
        "priceBracket": result.get("price"),
        "starRating": result.get("rating"),
        "number_of_reviews": result.get("reviews_cnt"),
        "venueType": venuetype,
        "website": result.get("link"),
        "address": result.get("address"),
        "openingHours": work_status,
        "telephoneNumber": result.get("phone"),
        "trading_status": derive_trading_status(work_status),
        "latitude": result.get("latitude"),
        "longitude": result.get("longitude"),
        "hotel_features": result.get("hotel_features"),
        "busy_times": None,
        "menu": None,
        **tags,
    }
    return ensure_required_fields(place)


def build_enrichment_request(urls, directory):
    """Build a Bright Data dataset trigger payload for URL enrichment.

    Args:
        urls: List of Google Maps place URLs.

    Returns:
        Payload dict containing enrichment inputs and delivery settings.
    """
    bucket = os.getenv("BRIGHTDATA_S3_BUCKET")
    role_arn = os.getenv("BRIGHTDATA_ROLE_ARN")
    external_id = os.getenv("BRIGHTDATA_EXTERNAL_ID")
    directory = directory or ""
    if not bucket or not role_arn or not external_id:
        raise ValueError("BRIGHTDATA_S3_BUCKET, BRIGHTDATA_ROLE_ARN, and BRIGHTDATA_EXTERNAL_ID are required")

    payload = {
        "input": [{"url": url} for url in urls],
        "deliver": {
            "type": "s3",
            "filename": {
                "extension": "json",
                "template": "results",
            },
            "bucket": bucket,
            "credentials": {
                "role_arn": role_arn,
                "external_id": external_id,
            },
            "directory": directory,
        },
    }
    return payload


def trigger_enrichment(payload):
    """Trigger a Bright Data dataset enrichment job.

    Args:
        payload: Enrichment payload dict.

    Returns:
        Parsed response JSON.
    """
    dataset_id = os.getenv("BRIGHTDATA_DATASET_ID")
    token = os.getenv("BRIGHTDATA_TOKEN")
    if not dataset_id:
        raise ValueError("BRIGHTDATA_DATASET_ID is required")
    if not token:
        raise ValueError("BRIGHTDATA_TOKEN is required")

    endpoint = "https://api.brightdata.com/datasets/v3/trigger"
    params = {"dataset_id": dataset_id, "notify": "false", "include_errors": "true"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    timeout = int(os.getenv("BRIGHTDATA_TRIGGER_TIMEOUT", "30"))

    response = requests.post(endpoint, headers=headers, params=params, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def log_event(level, message, **fields):
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant.
        message: Short event message.
        **fields: Additional structured fields.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def write_json_to_s3(s3_client, bucket, key, payload):
    """Write JSON content to S3.

    Args:
        s3_client: Boto3 S3 client.
        bucket: Destination bucket.
        key: Destination key.
        payload: JSON-serializable payload.
    """
    body = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def build_query_key(prefix, run_date, csv_name, query_id, suffix):
    """Build a persistence key for a query output.

    Args:
        prefix: Prefix for persistence output.
        run_date: Date partition string.
        csv_name: CSV filename.
        query_id: CSV query identifier.
        suffix: File suffix for the payload type.

    Returns:
        S3 key for persistence output.
    """
    base = os.path.splitext(os.path.basename(csv_name))[0]
    return (
        f"{prefix}/date={run_date}/csv_name={csv_name}/"
        f"query={query_id}_{suffix}_{base}.json"
    )


def build_manifest_key(prefix, run_date, csv_name):
    """Build the manifest key for a CSV.

    Args:
        prefix: Prefix for persistence output.
        run_date: Date partition string.
        csv_name: CSV filename.

    Returns:
        S3 key for the manifest.
    """
    base = os.path.splitext(os.path.basename(csv_name))[0]
    return f"{prefix}/date={run_date}/csv_name={csv_name}/serp_manifest_{base}.json"


def build_enrichment_directory(run_date, csv_name):
    """Build the fixed enrichment output directory for Bright Data."""
    dataset_id = os.getenv("BRIGHTDATA_DATASET_ID") or "dataset"
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"wsapi/{dataset_id}/{run_ts}/csv_name={csv_name}"


def lambda_handler(event, context):
    """Lambda entrypoint for SERP parsing and persistence."""
    results_bucket = os.getenv("RESULTS_BUCKET")
    if not results_bucket:
        log_event(logging.CRITICAL, "missing_env", env="RESULTS_BUCKET")
        raise ValueError("RESULTS_BUCKET is required")

    max_messages = int(os.getenv("MAX_MESSAGES", "10"))
    max_results = int(os.getenv("MAX_RESULTS", "5"))

    s3_client = boto3.client("s3")
    proxies = build_proxy_config()

    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    results = []
    try:
        messages = list(parse_sqs_event(event))[:max_messages]
        if not messages:
            log_event(logging.WARNING, "no_records")
            return {"processed": []}

        grouped = {}
        for message in messages:
            source_key = message.get("source_key") or "unknown.csv"
            csv_name = message.get("csv_name") or os.path.basename(source_key)
            grouped.setdefault(csv_name, []).append(message)

        persist_prefix = os.getenv("PERSIST_PREFIX", "persistence")

        for csv_name, csv_messages in grouped.items():
            records = []
            for message in csv_messages:
                search_id = message.get("id") or str(uuid.uuid4())
                search_term = message.get("search_term", "")
                if not search_term:
                    log_event(logging.WARNING, "missing_search_term", id=search_id)

                serp_payload = fetch_serp(search_term, proxies)
                serp_results = serp_payload.get("organic", serp_payload)
                if isinstance(serp_results, list):
                    serp_results = serp_results[:max_results]
                else:
                    serp_results = [serp_results]

                places = []
                for result in serp_results:
                    if not isinstance(result, dict):
                        continue
                    serp_place = build_place_item(result)
                    place_id = serp_place.get("googleplaceid")
                    place = dict(serp_place)
                    place["place_id"] = place_id
                    place["enrichment_url"] = result.get("map_link") or result.get("link")
                    places.append(place)

                record = {
                    "id": search_id,
                    "search_term": search_term,
                    "country": message.get("country"),
                    "source_key": message.get("source_key"),
                    "results": places,
                }
                records.append(record)

            run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            manifest_key = build_manifest_key(persist_prefix, run_date, csv_name)
            write_json_to_s3(
                s3_client,
                results_bucket,
                manifest_key,
                {"csv_name": csv_name, "records": records},
            )

            urls = []
            for record in records:
                for place in record.get("results", []):
                    url = place.get("enrichment_url")
                    if url:
                        urls.append(url)

            enrichment_directory = build_enrichment_directory(run_date, csv_name)
            enrichment_payload = build_enrichment_request(urls, enrichment_directory)
            enrichment_response = trigger_enrichment(enrichment_payload)
            enrichment_key = build_query_key(
                persist_prefix,
                run_date,
                csv_name,
                "manifest",
                "enrichment_request",
            )
            write_json_to_s3(
                s3_client,
                results_bucket,
                enrichment_key,
                {
                    "csv_name": csv_name,
                    "manifest_key": manifest_key,
                    "enrichment_directory": enrichment_directory,
                    "payload": enrichment_payload,
                    "response": enrichment_response,
                },
            )
            log_event(
                logging.INFO,
                "enrichment_request_built",
                csv_name=csv_name,
                queries=len(records),
                inputs=len(enrichment_payload.get("input", [])),
            )

            results.append({"csv_name": csv_name, "processed": len(csv_messages)})

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
