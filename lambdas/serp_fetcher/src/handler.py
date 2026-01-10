"""SERP Fetcher Lambda: Fetch Google Maps SERP and persist to DynamoDB.

This Lambda processes SQS messages containing search queries,
fetches SERP results, and saves them to DynamoDB with backup to S3.
It also triggers Bright Data enrichment for the retrieved places.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import boto3
import requests

# Add shared utilities to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../shared'))

from dynamo_utils import (
    save_serp_result,
    update_serp_status,
    update_batch_status,
)

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "serp_fetcher")

# Configuration constants
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

# AWS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def log_event(level: int, message: str, **fields) -> None:
    """Emit a structured JSON log line.

    Args:
        level: Logging level constant (e.g., logging.INFO).
        message: Short event message describing what happened.
        **fields: Additional structured fields for context.
    """
    payload = {"service": SERVICE_NAME, "message": message, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":")))


def parse_sqs_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse SQS messages from Lambda event.

    Args:
        event: Lambda event containing SQS records.

    Returns:
        List of parsed message body dictionaries.
    """
    messages = []
    for record in event.get("Records", []):
        body = record.get("body") or record.get("Body")
        if not body:
            continue
        try:
            messages.append(json.loads(body))
        except json.JSONDecodeError:
            log_event(logging.WARNING, "invalid_json_in_sqs_message")
    return messages


def build_proxy_config() -> Optional[Dict[str, str]]:
    """Build proxy configuration for Bright Data superproxy.

    Returns:
        Proxies dict for requests with HTTP/HTTPS URLs,
        or None if credentials are missing.
    """
    username = os.getenv("BRIGHTDATA_USERNAME")
    password = os.getenv("BRIGHTDATA_PASSWORD")
    port = os.getenv("BRIGHTDATA_PORT", "33335")
    host = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")

    if not username or not password:
        log_event(logging.DEBUG, "proxy_disabled_missing_credentials")
        return None

    session_id = uuid.uuid4().hex
    proxy_url = f"http://{username}-session-{session_id}:{password}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}


def build_serp_params() -> Dict[str, Any]:
    """Build SERP request query parameters.

    Returns:
        Dictionary of query parameters for SERP API.
    """
    params = {"lum_json": 1}
    extra = os.getenv("SERP_PARAMS_JSON")
    if extra:
        try:
            params.update(json.loads(extra))
        except json.JSONDecodeError:
            log_event(logging.WARNING, "invalid_serp_params_json")
    return params


def fetch_serp_with_retry(
    search_term: str,
    proxies: Optional[Dict[str, str]],
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """Fetch SERP results with exponential backoff on timeout.

    Args:
        search_term: Query string for Google Maps search.
        proxies: Proxy configuration dict or None.
        max_retries: Maximum number of retry attempts.

    Returns:
        JSON response dict if successful, None if all retries failed.
    """
    base_url = os.getenv("SERP_BASE_URL", "https://www.google.com/maps/search/")
    url = base_url.rstrip("/") + "/" + quote(search_term)
    timeout = int(os.getenv("REQUEST_TIMEOUT", "20"))
    verify_tls = os.getenv("VERIFY_TLS", "true").lower() == "true"
    headers = {"User-Agent": os.getenv("USER_AGENT", "Mozilla/5.0")}

    for attempt in range(max_retries):
        try:
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
                log_event(
                    logging.WARNING,
                    "serp_response_not_json",
                    search_term=search_term,
                )
                return {"raw_html": response.text}

        except requests.exceptions.Timeout:
            log_event(
                logging.WARNING,
                "serp_fetch_timeout",
                search_term=search_term,
                attempt=attempt,
            )
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * int(os.getenv("RETRY_DELAY_SECONDS", "2"))
                time.sleep(wait_time)
            continue

        except requests.exceptions.RequestException as e:
            log_event(
                logging.ERROR,
                "serp_fetch_error",
                search_term=search_term,
                error=str(e),
                attempt=attempt,
            )
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * int(os.getenv("RETRY_DELAY_SECONDS", "2"))
                time.sleep(wait_time)
            continue

    log_event(logging.ERROR, "serp_fetch_failed_all_retries", search_term=search_term)
    return None


def normalize_tags(tags: Optional[List[Dict[str, Any]]]) -> Dict[str, List[str]]:
    """Group tag values by normalized category names.

    Args:
        tags: List of tag dictionaries from SERP results.

    Returns:
        Dict mapping category keys to sorted lists of tag values.
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


def derive_trading_status(work_status: Optional[str]) -> Optional[str]:
    """Derive normalized trading status from work status text.

    Args:
        work_status: Status string from SERP results.

    Returns:
        Normalized status ('permanently_closed', 'closed', 'open')
        or None if status cannot be determined.
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


def ensure_required_fields(place: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure required fields exist in place record.

    Missing fields are filled with "Not Found" value.

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


def normalize_place_id(place_id: Optional[str]) -> Optional[str]:
    """Normalize a place identifier for enrichment calls.

    Args:
        place_id: Raw place identifier from SERP.

    Returns:
        Normalized place identifier string, or None if invalid.
    """
    if not place_id:
        return None
    return str(place_id).strip()


def build_place_item(result: Dict[str, Any]) -> Dict[str, Any]:
    """Build normalized place record from SERP result entry.

    Args:
        result: SERP result dictionary.

    Returns:
        Normalized place dict with required fields populated.
    """
    tags = normalize_tags(result.get("tags"))
    work_status = result.get("work_status")
    categories = result.get("category", [])
    venuetype = [c.get("title") for c in categories if isinstance(c, dict)]
    
    raw_place_id = (
        result.get("map_id_encoded")
        or result.get("map_id")
        or result.get("fid")
    )
    place_id = normalize_place_id(raw_place_id)
    google_place_id = normalize_place_id(
        result.get("map_id_encoded") or result.get("map_id")
    )

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


def save_serp_backup_to_s3(
    csv_name: str,
    query_id: str,
    serp_result: Dict[str, Any],
) -> Optional[str]:
    """Save SERP result as backup to S3.

    Args:
        csv_name: CSV name for partitioning.
        query_id: Query ID for organizing output.
        serp_result: SERP JSON result.

    Returns:
        S3 key where backup was saved, or None if save failed.
    """
    serp_bucket = os.getenv("SERP_BACKUP_BUCKET")
    if not serp_bucket:
        log_event(logging.DEBUG, "serp_backup_disabled")
        return None

    timestamp = datetime.utcnow().strftime("%Y-%m-%d")
    s3_key = (
        f"serp/{timestamp}/csv_name={quote(csv_name, safe='')}"
        f"/ID={quote(query_id, safe='')}.json"
    )

    try:
        s3_client.put_object(
            Bucket=serp_bucket,
            Key=s3_key,
            Body=json.dumps(serp_result),
            ContentType="application/json",
        )
        log_event(
            logging.INFO,
            "serp_backup_saved",
            csv_name=csv_name,
            query_id=query_id,
            s3_key=s3_key,
        )
        return f"s3://{serp_bucket}/{s3_key}"
    except Exception as e:
        log_event(
            logging.ERROR,
            "serp_backup_failed",
            csv_name=csv_name,
            query_id=query_id,
            error=str(e),
        )
        return None


def trigger_brightdata_enrichment(
    csv_name: str,
    query_id: str,
    places: List[Dict[str, Any]],
) -> None:
    """Trigger Bright Data enrichment via SNS.

    Args:
        csv_name: CSV name for batch tracking.
        query_id: Query ID for enrichment linking.
        places: List of place dicts with enrichment URLs.
    """
    bd_trigger_sns_topic = os.getenv("BD_TRIGGER_SNS_TOPIC")
    if not bd_trigger_sns_topic:
        log_event(logging.WARNING, "bd_trigger_disabled_no_topic")
        return

    # Extract URLs from places
    urls = []
    for place in places:
        url = place.get("website") or place.get("enrichment_url")
        if url:
            urls.append(url)

    if not urls:
        log_event(
            logging.WARNING,
            "no_enrichment_urls_found",
            csv_name=csv_name,
            query_id=query_id,
        )
        return

    payload = {
        "csv_name": csv_name,
        "query_id": query_id,
        "urls": urls,
        "timestamp": datetime.utcnow().isoformat(),
        "place_count": len(places),
    }

    try:
        sns_client.publish(
            TopicArn=bd_trigger_sns_topic,
            Message=json.dumps(payload),
            Subject=f"Bright Data Enrichment: {csv_name}/{query_id}",
        )
        log_event(
            logging.INFO,
            "brightdata_enrichment_triggered",
            csv_name=csv_name,
            query_id=query_id,
            url_count=len(urls),
        )
    except Exception as e:
        log_event(
            logging.ERROR,
            "brightdata_trigger_failed",
            csv_name=csv_name,
            query_id=query_id,
            error=str(e),
        )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entrypoint for SERP fetching and persistence.

    Processes SQS messages containing search queries,
    fetches SERP results, saves to DynamoDB, and triggers enrichment.

    Args:
        event: Lambda event from SQS.
        context: Lambda context object.

    Returns:
        Dict with statusCode and body containing summary of processed records.
    """
    max_retries = int(os.getenv("MAX_SERP_RETRIES", "3"))
    max_results = int(os.getenv("MAX_RESULTS_PER_QUERY", "5"))

    request_id = getattr(context, "aws_request_id", None)
    log_event(logging.INFO, "lambda_start", request_id=request_id)

    try:
        # Parse SQS messages
        messages = parse_sqs_event(event)
        if not messages:
            log_event(logging.WARNING, "no_sqs_records")
            return {
                "statusCode": 200,
                "body": json.dumps({"processed": 0, "message": "no_records"}),
            }

        log_event(logging.INFO, "processing_messages", count=len(messages))

        processed_count = 0
        failed_count = 0

        for message in messages:
            csv_name = message.get("csv_name")
            query_id = message.get("ID")
            search_term = message.get("search_term", "").strip()
            country = message.get("country", "").strip()
            retry_attempt = message.get("retry_attempt", 0)

            if not csv_name or not query_id:
                log_event(logging.WARNING, "missing_required_fields_in_message")
                failed_count += 1
                continue

            if not search_term:
                log_event(
                    logging.WARNING,
                    "missing_search_term",
                    csv_name=csv_name,
                    query_id=query_id,
                )
                failed_count += 1
                continue

            log_event(
                logging.INFO,
                "processing_query",
                csv_name=csv_name,
                query_id=query_id,
                search_term=search_term,
            )

            # Build proxy
            proxies = build_proxy_config()

            # Fetch SERP with retries
            serp_result = fetch_serp_with_retry(
                search_term=search_term,
                proxies=proxies,
                max_retries=max_retries,
            )

            if not serp_result:
                log_event(
                    logging.ERROR,
                    "serp_fetch_failed",
                    csv_name=csv_name,
                    query_id=query_id,
                )
                try:
                    update_serp_status(
                        csv_name=csv_name,
                        query_id=query_id,
                        status="failed",
                        retry_count=max_retries,
                    )
                except Exception as e:
                    log_event(
                        logging.ERROR,
                        "failed_to_update_serp_status",
                        error=str(e),
                    )
                failed_count += 1
                continue

            # Extract places from SERP
            serp_results = serp_result.get("organic", [])
            if not isinstance(serp_results, list):
                serp_results = [serp_result]

            serp_results = serp_results[:max_results]

            places = []
            for result in serp_results:
                if not isinstance(result, dict):
                    continue
                try:
                    place = build_place_item(result)
                    place["enrichment_url"] = (
                        result.get("map_link") or result.get("link")
                    )
                    places.append(place)
                except Exception as e:
                    log_event(
                        logging.WARNING,
                        "failed_to_build_place_item",
                        error=str(e),
                    )
                    continue

            # Save SERP result to DynamoDB
            serp_json = {
                "search_term": search_term,
                "country": country,
                "places": places,
                "place_count": len(places),
                "fetched_at": datetime.utcnow().isoformat(),
            }

            # Save backup to S3
            s3_key = save_serp_backup_to_s3(csv_name, query_id, serp_json)

            # Save to DynamoDB
            try:
                save_serp_result(
                    csv_name=csv_name,
                    query_id=query_id,
                    serp_result=serp_json,
                    s3_key=s3_key,
                    retry_count=retry_attempt,
                    search_term=search_term,
                    country=country,
                )
            except Exception as e:
                log_event(
                    logging.ERROR,
                    "failed_to_save_serp_to_dynamo",
                    csv_name=csv_name,
                    query_id=query_id,
                    error=str(e),
                )
                failed_count += 1
                continue

            # Update batch status
            try:
                update_batch_status(
                    csv_name=csv_name,
                    serp_increment=1,
                )
            except Exception as e:
                log_event(
                    logging.ERROR,
                    "failed_to_update_batch_status",
                    csv_name=csv_name,
                    error=str(e),
                )

            # Trigger Bright Data enrichment
            trigger_brightdata_enrichment(csv_name, query_id, places)

            processed_count += 1

            log_event(
                logging.INFO,
                "query_processed_success",
                csv_name=csv_name,
                query_id=query_id,
                place_count=len(places),
            )

        log_event(
            logging.INFO,
            "lambda_complete",
            processed=processed_count,
            failed=failed_count,
            total=len(messages),
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "processed": processed_count,
                "failed": failed_count,
                "total": len(messages),
            }),
        }

    except Exception as e:
        log_event(logging.CRITICAL, "lambda_error", error=str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
