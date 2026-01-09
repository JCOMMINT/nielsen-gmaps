from lambdas.enrich_to_s3.src import handler


def test_build_final_key_contains_csv_name():
    key = handler.build_final_key("final", "sample.csv")

    assert key == "final/sample.csv_enriched_results.json"


def test_parse_query_id_from_key():
    key = "wsapi/dataset/2026-01-01T00-00-00Z/csv_name=sample.csv/query=abc123/results.json"

    assert handler.parse_query_id_from_key(key) == "abc123"


def test_merge_place_prefers_enrichment():
    serp_place = {
        "title": "Cafe",
        "googleplaceid": "ChIJabc",
        "priceBracket": "Not Found",
        "starRating": 4.0,
        "number_of_reviews": 10,
        "venueType": ["Cafe"],
        "website": "https://serp.example.com",
        "address": "123 St",
        "openingHours": "Open now",
        "telephoneNumber": "Not Found",
        "busy_times": "Not Found",
        "trading_status": "open",
        "highlights": "Not Found",
        "serviceOptions": "Not Found",
        "offerings": "Not Found",
        "diningOptions": "Not Found",
        "amenities": "Not Found",
        "atmosphere": "Not Found",
        "crowd": "Not Found",
        "payments": "Not Found",
        "children": "Not Found",
        "accessibility": "Not Found",
        "latitude": "Not Found",
        "longitude": "Not Found",
        "hotel_features": "Not Found",
        "menu": "Not Found",
    }
    enrichment = {
        "place_id": "ChIJabc",
        "name": "Cafe Enriched",
        "rating": 4.8,
        "reviews_count": 42,
        "address": "456 Ave",
        "open_website": "https://enriched.example.com",
        "lat": 1.23,
        "lon": 4.56,
        "services_provided": ["Great coffee", "Wheelchair-accessible entrance"],
        "business_details": [{"field_name": "price_range", "details": "$$"}],
    }

    merged = handler.merge_place(serp_place, enrichment)

    assert merged["title"] == "Cafe Enriched"
    assert merged["priceBracket"] == "$$"
    assert merged["starRating"] == 4.8
    assert merged["number_of_reviews"] == 42
    assert merged["website"] == "https://enriched.example.com"
    assert merged["address"] == "456 Ave"
    assert merged["accessibility"] != "Not Found"
