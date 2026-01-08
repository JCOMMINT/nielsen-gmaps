"""Enrichment merge Lambda: join SERP and Bright Data results and persist output."""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

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
    return {
        "enrichment_bucket": event.get("enrichment_bucket") or event.get("bucket"),
        "enrichment_key": event.get("enrichment_key") or event.get("key"),
        "serp_bucket": event.get("serp_bucket") or event.get("bucket"),
        "serp_key": event.get("serp_key"),
        "final_bucket": event.get("final_bucket") or event.get("bucket"),
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


def build_final_key(prefix, csv_name, query_id):
    """Build the final output key for a query.

    Args:
        prefix: Final output prefix.
        csv_name: CSV filename.
        query_id: Query identifier.

    Returns:
        S3 key for final output.
    """
    base = os.path.splitext(os.path.basename(csv_name))[0]
    return f"{prefix}/{base}/query={query_id}_enriched.json"


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
    # TODO: Configure email notifications (SES/SNS/etc) and populate recipients.
    logger.info("notification_placeholder")


def lambda_handler(event, context):
    """Lambda entrypoint for merging SERP and enrichment data."""
    s3_client = boto3.client("s3")
    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    config = parse_notification_event(event)
    enrichment_bucket = config.get("enrichment_bucket")
    enrichment_key = config.get("enrichment_key")
    serp_bucket = config.get("serp_bucket")
    serp_key = config.get("serp_key")
    final_bucket = config.get("final_bucket")
    final_prefix = config.get("final_prefix")

    if not enrichment_bucket or not enrichment_key:
        log_event(logging.CRITICAL, "missing_enrichment_location")
        raise ValueError("Missing enrichment bucket/key")
    if not serp_bucket or not serp_key:
        log_event(logging.CRITICAL, "missing_serp_location")
        raise ValueError("Missing SERP bucket/key")
    if not final_bucket:
        log_event(logging.CRITICAL, "missing_final_bucket")
        raise ValueError("Missing final bucket")

    enrichment_records = read_json_from_s3(s3_client, enrichment_bucket, enrichment_key)
    serp_payload = read_json_from_s3(s3_client, serp_bucket, serp_key)
    serp_records = serp_payload.get("records") or []
    if not serp_records:
        raise ValueError("SERP payload missing records")
    serp_record = serp_records[0]

    enrichment_index = {}
    for record in enrichment_records or []:
        place_id = record.get("place_id")
        if place_id:
            enrichment_index[place_id] = record

    merged_places = []
    for place in serp_record.get("results", []):
        place_id = place.get("googleplaceid") or place.get("place_id")
        merged_places.append(merge_place(place, enrichment_index.get(place_id)))

    output_record = dict(serp_record)
    output_record["results"] = merged_places
    output_record["enriched_at"] = datetime.now(timezone.utc).isoformat()

    csv_name = serp_payload.get("csv_name") or "unknown.csv"
    query_id = serp_record.get("id") or "unknown"
    final_key = build_final_key(final_prefix, csv_name, query_id)
    write_json_to_s3(
        s3_client,
        final_bucket,
        final_key,
        {"csv_name": csv_name, "records": [output_record]},
    )

    send_completion_notification(
        {
            "csv_name": csv_name,
            "query_id": query_id,
            "final_bucket": final_bucket,
            "final_key": final_key,
        }
    )

    log_event(logging.INFO, "lambda_complete", final_key=final_key)
    return {"status": "ok", "final_bucket": final_bucket, "final_key": final_key}
