"""Enrichment merge Lambda: join SERP and Bright Data results and persist output."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError
import requests
import smtplib
from email.message import EmailMessage

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "enrich_to_s3")

SERVICE_KEYWORDS = {
    "highlights": ["great coffee", "great dessert", "great tea selection", "live music", "live performances"],
    "serviceOptions": ["delivery", "takeaway", "takeout", "dine-in", "outdoor seating", "on-site", "onsite"],
    "offerings": ["coffee", "dessert", "quick bite", "tea", "wine", "alcohol"],
    "diningOptions": ["breakfast", "brunch", "lunch", "dinner", "seating", "table service", "dessert"],
    "amenities": ["restroom", "wi-fi", "wifi", "bar"],
    "atmosphere": ["casual", "cozy", "trendy", "romantic", "historic"],
    "crowd": ["tourists", "students", "family-friendly", "groups"],
    "payments": ["credit cards", "debit cards", "nfc"],
    "children": ["good for kids", "kids' menu", "high chairs"],
    "accessibility": ["wheelchair"],
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


def log_event(level, message, **fields):
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant.
        message: Short event message.
        **fields: Additional structured fields.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def parse_notification_event(event):
    """Parse a Bright Data notification event.

    Args:
        event: Lambda event payload.

    Returns:
        Dict with enrichment and SERP locations.
    """
    # TODO: Map to the final Bright Data notification schema.
    if "Records" in event:
        record = event["Records"][0]
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        if key:
            key = unquote_plus(key)
        return {
            "enrichment_bucket": bucket,
            "enrichment_key": key,
        }

    return {
        "enrichment_bucket": event.get("enrichment_bucket") or event.get("bucket"),
        "enrichment_key": event.get("enrichment_key") or event.get("key"),
        "serp_bucket": event.get("serp_bucket"),
        "serp_key": event.get("serp_key"),
        "final_bucket": event.get("final_bucket"),
        "final_prefix": event.get("final_prefix") or os.getenv("FINAL_PREFIX", "final"),
    }


def read_json_from_s3(s3_client, bucket, key):
    """Read JSON content from S3.

    Args:
        s3_client: Boto3 S3 client.
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        Parsed JSON object.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


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


def build_final_key(prefix, csv_name):
    """Build the final output key for a CSV.

    Args:
        prefix: Final output prefix.
        csv_name: CSV filename.

    Returns:
        S3 key for final output.
    """
    return f"{prefix}/{csv_name}_enriched_results.json"


def parse_csv_name_from_key(key):
    """Extract the CSV name from an enrichment key."""
    marker = "csv_name="
    if marker not in key:
        return None
    start = key.index(marker) + len(marker)
    tail = key[start:]
    return tail.split("/", 1)[0]


def parse_dataset_id_from_key(key):
    """Extract dataset id from a wsapi key."""
    parts = key.split("/")
    if len(parts) < 2 or parts[0] != "wsapi":
        return None
    return parts[1]


def parse_query_id_from_key(key):
    """Extract query id from an enrichment key."""
    marker = "query="
    if marker not in key:
        return None
    start = key.index(marker) + len(marker)
    tail = key[start:]
    return tail.split("/", 1)[0]


def parse_run_date_from_key(key):
    """Extract run date partition from a persistence key."""
    marker = "date="
    if marker not in key:
        return None
    start = key.index(marker) + len(marker)
    tail = key[start:]
    return tail.split("/", 1)[0]


def list_s3_keys(s3_client, bucket, prefix):
    """List keys under a prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield item


def find_latest_batch_manifest(s3_client, bucket, persist_prefix, csv_name):
    """Find the latest batch manifest key for a CSV name."""
    prefix = f"{persist_prefix}/"
    candidates = []
    for item in list_s3_keys(s3_client, bucket, prefix):
        key = item.get("Key", "")
        if f"csv_name={csv_name}/batch_" in key:
            candidates.append(item)
    if not candidates:
        return None
    latest = max(candidates, key=lambda entry: entry.get("LastModified"))
    return latest.get("Key")


def build_status_key(batch_key):
    """Build a status key from a batch manifest key."""
    if not batch_key:
        return None
    return batch_key.replace("batch_", "status_")


def build_partial_key(batch_key):
    """Build a partial merge key from a batch manifest key."""
    if not batch_key:
        return None
    return batch_key.replace("batch_", "merged_partial_")


def build_query_serp_key(prefix, run_date, csv_name, query_id):
    """Build the SERP key for a single query."""
    return (
        f"{prefix}/date={run_date}/csv_name={csv_name}/serp/"
        f"query={query_id}.json"
    )


def build_query_enriched_key(prefix, run_date, csv_name, query_id):
    """Build the enriched key for a single query."""
    return (
        f"{prefix}/date={run_date}/csv_name={csv_name}/enriched/"
        f"query={query_id}.json"
    )


def list_query_keys(s3_client, bucket, prefix):
    """List query keys under a prefix."""
    return [item.get("Key") for item in list_s3_keys(s3_client, bucket, prefix)]


def collect_enrichment_records(s3_client, bucket, dataset_id, csv_name):
    """Collect all enrichment records for a CSV."""
    records = []
    if not dataset_id:
        return records
    prefix = f"wsapi/{dataset_id}/"
    for item in list_s3_keys(s3_client, bucket, prefix):
        key = item.get("Key", "")
        if f"/csv_name={csv_name}/" not in key:
            continue
        if not key.endswith("results.json"):
            continue
        payload = read_json_from_s3(s3_client, bucket, key)
        if isinstance(payload, list):
            records.extend(payload)
        elif isinstance(payload, dict):
            records.append(payload)
    return records


def build_place_key(place):
    """Build a stable key for a place record."""
    return place.get("googleplaceid") or place.get("place_id") or place.get("enrichment_url")


def build_enrichment_key(record):
    """Build a stable key for an enrichment record."""
    return record.get("place_id") or record.get("input", {}).get("url") or record.get("url")


def load_status(s3_client, bucket, status_key):
    """Load the status JSON if present."""
    try:
        return read_json_from_s3(s3_client, bucket, status_key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
            return None
        raise


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

    response = requests.post(
        endpoint,
        params=params,
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def should_finalize(status, now, max_retries, timeout_hours):
    """Determine if a batch should be finalized."""
    if status.get("complete"):
        return True
    if status.get("missing_ids") == []:
        return True
    if status.get("retry_count", 0) >= max_retries:
        return True
    started_at = status.get("started_at")
    if started_at:
        started = datetime.fromisoformat(started_at)
        if now - started >= timedelta(hours=timeout_hours):
            return True
    return False


def extract_business_detail(record, field_name):
    """Extract a business detail value from enrichment records.

    Args:
        record: Enrichment record dict.
        field_name: Field name to match.

    Returns:
        Detail value or None.
    """
    for detail in record.get("business_details", []) or []:
        if detail.get("field_name") == field_name:
            return detail.get("details") or detail.get("link")
    return None


def extract_services(services):
    """Group services into client categories.

    Args:
        services: List of service strings.

    Returns:
        Dict with grouped service lists.
    """
    grouped = {key: [] for key in SERVICE_KEYWORDS}
    for service in services or []:
        if not service:
            continue
        lower = service.lower()
        for key, keywords in SERVICE_KEYWORDS.items():
            if any(keyword in lower for keyword in keywords):
                grouped[key].append(service)
    return {key: sorted(set(values)) for key, values in grouped.items()}


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


def compute_coverage(records, expected_queries=None, expected_places=None, expected_per_query=5):
    """Compute enrichment coverage metrics.

    Args:
        records: List of merged query records.
        expected_queries: Expected query count override.
        expected_places: Expected place count override.
        expected_per_query: Expected places per query.

    Returns:
        Dict with coverage metrics.
    """
    if expected_queries is None:
        expected_queries = len(records)
    if expected_places is None:
        expected_places = expected_queries * expected_per_query
    total_places = 0
    enriched_places = 0

    for record in records:
        places = record.get("results", [])
        total_places += len(places)
        for place in places:
            if (
                place.get("googleplaceid") not in (None, "Not Found")
                and place.get("starRating") not in (None, "Not Found")
                and place.get("number_of_reviews") not in (None, "Not Found")
            ):
                enriched_places += 1

    coverage = (enriched_places / total_places * 100) if total_places else 0.0
    return {
        "expected_queries": expected_queries,
        "expected_places": expected_places,
        "total_places": total_places,
        "enriched_places": enriched_places,
        "coverage_percent": round(coverage, 2),
    }


def merge_place(serp_place, enrichment_record):
    """Merge SERP and enrichment payloads into a final place record.

    Args:
        serp_place: SERP-derived place dict.
        enrichment_record: Enrichment record dict or None.

    Returns:
        Merged place record dict.
    """
    merged = dict(serp_place)
    if not enrichment_record:
        return ensure_required_fields(merged)

    services = extract_services(enrichment_record.get("services_provided"))
    merged.update(
        {
            "title": enrichment_record.get("name") or merged.get("title"),
            "googleplaceid": enrichment_record.get("place_id") or merged.get("googleplaceid"),
            "priceBracket": extract_business_detail(enrichment_record, "price_range") or merged.get("priceBracket"),
            "starRating": enrichment_record.get("rating") or merged.get("starRating"),
            "number_of_reviews": enrichment_record.get("reviews_count") or merged.get("number_of_reviews"),
            "venueType": enrichment_record.get("all_categories")
            or enrichment_record.get("category")
            or merged.get("venueType"),
            "website": enrichment_record.get("open_website")
            or extract_business_detail(enrichment_record, "public")
            or merged.get("website"),
            "address": enrichment_record.get("address") or merged.get("address"),
            "openingHours": enrichment_record.get("open_hours") or merged.get("openingHours"),
            "telephoneNumber": enrichment_record.get("phone_number") or merged.get("telephoneNumber"),
            "busy_times": enrichment_record.get("popular_times") or merged.get("busy_times"),
            "latitude": enrichment_record.get("lat") or merged.get("latitude"),
            "longitude": enrichment_record.get("lon") or merged.get("longitude"),
        }
    )

    if enrichment_record.get("permanently_closed") is True:
        merged["trading_status"] = "permanently_closed"
    elif enrichment_record.get("temporarily_closed") is True:
        merged["trading_status"] = "closed"

    for key in SERVICE_KEYWORDS:
        if merged.get(key) in (None, "Not Found", []):
            merged[key] = services.get(key)

    return ensure_required_fields(merged)


def send_completion_notification(payload):
    """Send completion notification.

    Args:
        payload: Notification payload.
    """
    if os.getenv("ENABLE_EMAIL", "false").lower() != "true":
        logger.info("notification_disabled")
        return
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USERNAME")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    to_email = os.getenv("NOTIFY_EMAIL", "accounts2@mint-data.co")

    if not smtp_user or not smtp_pass:
        logger.warning("notification_skipped_missing_smtp")
        return

    subject = "Google Maps Scraping Tool Failed/Success"
    level = payload.get("level", "INFO")
    body = "\n".join(
        [
            f"Level: {level}",
            f"CSV: {payload.get('csv_name')}",
            f"Status: {payload.get('status')}",
            f"Expected: {payload.get('expected')}",
            f"Received: {payload.get('received')}",
            f"Missing: {payload.get('missing')}",
            f"Retries: {payload.get('retries')}",
            f"Reason: {payload.get('reason')}",
            f"Expected queries: {payload.get('expected_queries')}",
            f"Expected places: {payload.get('expected_places')}",
            f"Total places: {payload.get('total_places')}",
            f"Enriched places: {payload.get('enriched_places')}",
            f"Coverage: {payload.get('coverage_percent')}%",
        ]
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
        smtp.login(smtp_user, smtp_pass)
        smtp.send_message(msg)


def lambda_handler(event, context):
    """Lambda entrypoint for merging SERP and enrichment data."""
    s3_client = boto3.client("s3")
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    config = parse_notification_event(event)
    enrichment_bucket = config.get("enrichment_bucket")
    enrichment_key = config.get("enrichment_key")
    serp_bucket = config.get("serp_bucket") or os.getenv("RESULTS_BUCKET")
    final_bucket = config.get("final_bucket") or os.getenv("FINAL_BUCKET", "nielsen-input")
    final_prefix = config.get("final_prefix") or os.getenv("FINAL_PREFIX", "final")
    persist_prefix = os.getenv("PERSIST_PREFIX", "persistence")
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    timeout_hours = int(os.getenv("BATCH_TIMEOUT_HOURS", "24"))

    if not enrichment_bucket or not enrichment_key:
        log_event(logging.CRITICAL, "missing_enrichment_location")
        raise ValueError("Missing enrichment bucket/key")
    if not serp_bucket:
        log_event(logging.CRITICAL, "missing_serp_location")
        raise ValueError("Missing SERP bucket")
    if not final_bucket:
        log_event(logging.CRITICAL, "missing_final_bucket")
        raise ValueError("Missing final bucket")

    csv_name = parse_csv_name_from_key(enrichment_key)
    if not csv_name:
        raise ValueError("Unable to determine csv_name from enrichment key")

    dataset_id = parse_dataset_id_from_key(enrichment_key) or os.getenv("BRIGHTDATA_DATASET_ID")
    query_id = parse_query_id_from_key(enrichment_key)
    if not query_id:
        raise ValueError("Unable to determine query_id from enrichment key")

    batch_key = find_latest_batch_manifest(s3_client, serp_bucket, persist_prefix, csv_name)
    if not batch_key:
        raise ValueError("Missing batch manifest for csv_name")

    run_date = parse_run_date_from_key(batch_key) or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    serp_key = build_query_serp_key(persist_prefix, run_date, csv_name, query_id)
    serp_record = read_json_from_s3(s3_client, serp_bucket, serp_key)
    serp_places = serp_record.get("results") or []

    enrichment_payload = read_json_from_s3(s3_client, enrichment_bucket, enrichment_key)
    if isinstance(enrichment_payload, list):
        enrichment_records = enrichment_payload
    else:
        enrichment_records = [enrichment_payload]

    enrichment_index = {}
    for record in enrichment_records:
        key = build_enrichment_key(record)
        if key:
            enrichment_index[key] = record

    expected_keys = []
    url_lookup = {}
    for place in serp_places:
        key = build_place_key(place)
        if not key:
            continue
        expected_keys.append(key)
        url_lookup[key] = place.get("enrichment_url")

    expected_set = sorted(set(expected_keys))
    received_set = sorted(set(enrichment_index.keys()))
    missing_ids = sorted(set(expected_set) - set(received_set))

    now = datetime.now(timezone.utc)
    status_key = build_status_key(batch_key)
    status = load_status(s3_client, serp_bucket, status_key) or {}
    status.setdefault("queries", {})

    if not status.get("started_at"):
        status["started_at"] = now.isoformat()

    query_state = status["queries"].get(query_id, {})
    retry_count = int(query_state.get("retry_count", 0))
    query_state.update(
        {
            "missing_place_ids": missing_ids,
            "expected": len(expected_set),
            "received": len(received_set),
            "retry_count": retry_count,
            "last_update": now.isoformat(),
        }
    )
    query_state["complete"] = missing_ids == []
    status["queries"][query_id] = query_state
    status.update(
        {
            "csv_name": csv_name,
            "last_update": now.isoformat(),
        }
    )

    if missing_ids and retry_count < max_retries:
        retry_urls = [url_lookup.get(item) for item in missing_ids if url_lookup.get(item)]
        if retry_urls:
            payload = {
                "input": [{"url": url} for url in retry_urls],
                "deliver": {
                    "type": "s3",
                    "filename": {"extension": "json", "template": "results"},
                    "bucket": enrichment_bucket,
                    "credentials": {
                        "role_arn": os.getenv("BRIGHTDATA_ROLE_ARN"),
                        "external_id": os.getenv("BRIGHTDATA_EXTERNAL_ID"),
                    },
                    "directory": (
                        f"wsapi/{dataset_id}/{now.strftime('%Y-%m-%dT%H-%M-%SZ')}/"
                        f"csv_name={csv_name}/query={query_id}/retry={retry_count + 1}"
                    ),
                },
            }
            trigger_enrichment(payload)
            query_state["retry_count"] = retry_count + 1
            query_state["last_triggered_at"] = now.isoformat()
            status["queries"][query_id] = query_state

    write_json_to_s3(s3_client, serp_bucket, status_key, status)

    merged_places = []
    for place in serp_places:
        key = build_place_key(place)
        merged_places.append(merge_place(place, enrichment_index.get(key)))

    merged_record = dict(serp_record)
    merged_record["results"] = merged_places
    merged_record["enriched_at"] = now.isoformat()
    enriched_key = build_query_enriched_key(persist_prefix, run_date, csv_name, query_id)
    write_json_to_s3(s3_client, serp_bucket, enriched_key, merged_record)

    serp_prefix = f"{persist_prefix}/date={run_date}/csv_name={csv_name}/serp/"
    serp_keys = list_query_keys(s3_client, serp_bucket, serp_prefix)
    expected_query_ids = sorted({parse_query_id_from_key(key) for key in serp_keys if parse_query_id_from_key(key)})
    enriched_prefix = f"{persist_prefix}/date={run_date}/csv_name={csv_name}/enriched/"
    enriched_keys = list_query_keys(s3_client, serp_bucket, enriched_prefix)
    enriched_query_ids = sorted({parse_query_id_from_key(key) for key in enriched_keys if parse_query_id_from_key(key)})

    started = datetime.fromisoformat(status["started_at"])
    timed_out = now - started >= timedelta(hours=timeout_hours)

    terminal_queries = []
    for query in expected_query_ids:
        state = status["queries"].get(query, {})
        terminal_queries.append(
            state.get("complete") or timed_out or int(state.get("retry_count", 0)) >= max_retries
        )

    finalize_reason = None
    if expected_query_ids and all(
        status["queries"].get(query, {}).get("complete") for query in expected_query_ids
    ):
        finalize_reason = "complete"
    elif timed_out:
        finalize_reason = "timeout"
    elif expected_query_ids and all(terminal_queries):
        finalize_reason = "retries_exhausted"

    partial_key = build_partial_key(batch_key)
    write_json_to_s3(
        s3_client,
        serp_bucket,
        partial_key,
        {
            "csv_name": csv_name,
            "queries": len(expected_query_ids),
            "enriched_queries": len(enriched_query_ids),
        },
    )

    if finalize_reason:
        merged_records = []
        for serp_key in serp_keys:
            record = read_json_from_s3(s3_client, serp_bucket, serp_key)
            query = parse_query_id_from_key(serp_key)
            if not query:
                continue
            enriched_key = build_query_enriched_key(persist_prefix, run_date, csv_name, query)
            try:
                record = read_json_from_s3(s3_client, serp_bucket, enriched_key)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") != "NoSuchKey":
                    raise
            merged_records.append(record)

        merged_records.sort(key=lambda item: item.get("row_number") or 0)
        final_key = build_final_key(final_prefix, csv_name)
        write_json_to_s3(
            s3_client,
            final_bucket,
            final_key,
            {"csv_name": csv_name, "records": merged_records},
        )
        status["complete"] = True
        status["finalize_reason"] = finalize_reason
        write_json_to_s3(s3_client, serp_bucket, status_key, status)

        batch_manifest = read_json_from_s3(s3_client, serp_bucket, batch_key)
        expected_queries = batch_manifest.get("total_rows") or len(expected_query_ids)
        expected_places = batch_manifest.get("expected_places")
        metrics = compute_coverage(
            merged_records,
            expected_queries=expected_queries,
            expected_places=expected_places,
            expected_per_query=batch_manifest.get("max_results", 5),
        )
        level = "INFO" if finalize_reason == "complete" else "WARNING"
        send_completion_notification(
            {
                "level": level,
                "csv_name": csv_name,
                "status": "complete",
                "expected": len(expected_query_ids),
                "received": len(enriched_query_ids),
                "missing": len(expected_query_ids) - len(enriched_query_ids),
                "retries": sum(
                    int(state.get("retry_count", 0)) for state in status["queries"].values()
                ),
                "reason": finalize_reason,
                **metrics,
            }
        )
        log_event(logging.INFO, "lambda_complete", final_key=final_key)
        return {"status": "ok", "final_bucket": final_bucket, "final_key": final_key}

    log_event(
        logging.INFO,
        "lambda_partial",
        csv_name=csv_name,
        missing_queries=len(expected_query_ids) - len(enriched_query_ids),
    )
    return {"status": "partial", "csv_name": csv_name}
