"""
Business discovery across Google Places and optional social/directory sources.
"""
from __future__ import annotations

import time
from typing import Dict, List

import requests

import config


GOOGLE_TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def _request_json(url: str, *, params: dict, timeout: float, retries: int = 2) -> Dict:
    last_error = ""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code >= 500:
                last_error = f"http_{resp.status_code}"
                time.sleep(min(2 ** attempt, 4))
                continue
            return resp.json()
        except requests.exceptions.Timeout:
            last_error = "timeout"
        except requests.exceptions.RequestException as exc:
            last_error = f"request_error:{exc}"

        if attempt < retries:
            time.sleep(min(2 ** attempt, 4))

    return {"status": "REQUEST_FAILED", "error_message": last_error}


def search_google_places(query: str, location: str | None = None, max_results: int = 60, api_key: str | None = None) -> Dict:
    """
    Query Google Places Text Search and normalize result rows.
    """
    key = (api_key or config.GOOGLE_PLACES_API_KEY or "").strip()
    if not key:
        return {"error": "Google Places API key not configured. Add it in Settings.", "results": []}

    search_query = query.strip()
    if location:
        search_query = f"{query.strip()} in {location.strip()}"

    params = {"query": search_query, "key": key}
    data = _request_json(GOOGLE_TEXTSEARCH_URL, params=params, timeout=15)

    status = data.get("status")
    if status == "REQUEST_DENIED":
        return {
            "error": f"API Error: {data.get('error_message', 'Request denied')}",
            "results": [],
        }
    if status not in {"OK", "ZERO_RESULTS"}:
        return {"error": f"API Error: {status}", "results": []}

    all_results = list(data.get("results", []))

    pages = 1
    next_page_token = data.get("next_page_token")
    while next_page_token and len(all_results) < max_results and pages < 3:
        token_ready = False
        token_data = None

        for _ in range(4):
            time.sleep(2)
            token_params = {"pagetoken": next_page_token, "key": key}
            token_data = _request_json(GOOGLE_TEXTSEARCH_URL, params=token_params, timeout=15)
            token_status = token_data.get("status")
            if token_status == "INVALID_REQUEST":
                continue
            token_ready = True
            break

        if not token_ready:
            break

        if token_data.get("status") not in {"OK", "ZERO_RESULTS"}:
            break

        all_results.extend(token_data.get("results", []))
        next_page_token = token_data.get("next_page_token")
        pages += 1

    businesses = []
    for place in all_results[:max_results]:
        address = place.get("formatted_address", "")
        city_name = ""
        if address:
            parts = [part.strip() for part in address.split(",") if part.strip()]
            if len(parts) >= 2:
                candidate = parts[-2]
                cleaned = []
                for token in candidate.split():
                    if any(char.isdigit() for char in token):
                        continue
                    if len(token) == 2 and token.isupper():
                        continue
                    cleaned.append(token)
                city_name = " ".join(cleaned).strip()

        businesses.append(
            {
                "business_name": place.get("name", ""),
                "address": address,
                "google_rating": place.get("rating", 0.0),
                "review_count": place.get("user_ratings_total", 0),
                "place_id": place.get("place_id", ""),
                "types": place.get("types", []),
                "source": "google_maps",
                "city": city_name or (location or ""),
                "website": "",
                "phone": "",
                "email": "",
                "category": query,
            }
        )

    return {"error": None, "results": businesses, "total": len(businesses)}


def get_place_details(place_id: str, api_key: str | None = None) -> Dict:
    key = (api_key or config.GOOGLE_PLACES_API_KEY or "").strip()
    if not key or not place_id:
        return {}

    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,url,opening_hours,business_status",
        "key": key,
    }
    data = _request_json(GOOGLE_DETAILS_URL, params=params, timeout=12)
    if data.get("status") != "OK":
        return {}

    result = data.get("result", {})
    return {
        "website": result.get("website", ""),
        "phone": result.get("formatted_phone_number", ""),
        "business_status": result.get("business_status", ""),
    }


def enrich_with_details(businesses: List[dict], max_detail_lookups: int = 30, api_key: str | None = None) -> List[dict]:
    enriched = []
    detail_count = 0

    for biz in businesses:
        if biz.get("place_id") and detail_count < max_detail_lookups:
            details = get_place_details(biz["place_id"], api_key=api_key)
            if details:
                biz["website"] = details.get("website", biz.get("website", ""))
                biz["phone"] = details.get("phone", biz.get("phone", ""))
            detail_count += 1
            time.sleep(0.15)

        enriched.append(biz)

    return enriched


def search_businesses(business_type: str, location: str, source_choice: str = "all", max_results: int = 60) -> Dict:
    """
    Main entry for search pipeline.
    """
    businesses = []
    errors = []

    query = (business_type or "").strip()
    location = (location or "").strip()

    api_key = config.GOOGLE_PLACES_API_KEY
    try:
        from database import get_settings

        settings = get_settings()
        api_key = settings.get("google_places_api_key", api_key)
    except Exception:
        pass

    if source_choice in ("all", "google_maps"):
        gm_res = search_google_places(query, location=location, max_results=max_results, api_key=api_key)
        if gm_res.get("error"):
            errors.append(f"Google Maps error: {gm_res['error']}")
        else:
            businesses.extend(gm_res.get("results", []))

    if source_choice in ("all", "instagram"):
        try:
            from source_instagram import search_instagram

            ig_res = search_instagram(query, location, max_results=20 if source_choice == "all" else max_results)
            if ig_res.get("error"):
                errors.append(f"Instagram error: {ig_res['error']}")
            else:
                businesses.extend(ig_res.get("results", []))
        except Exception as exc:
            errors.append(f"Instagram module error: {exc}")

    if source_choice in ("all", "facebook"):
        try:
            from source_facebook import search_facebook

            fb_res = search_facebook(query, location, max_results=20 if source_choice == "all" else max_results)
            if fb_res.get("error"):
                errors.append(f"Facebook error: {fb_res['error']}")
            else:
                businesses.extend(fb_res.get("results", []))
        except Exception as exc:
            errors.append(f"Facebook module error: {exc}")

    if source_choice in ("all", "directories"):
        try:
            from source_directories import search_directories

            dir_res = search_directories(query, location, max_results=20 if source_choice == "all" else max_results)
            if dir_res.get("error"):
                errors.append(f"Directories error: {dir_res['error']}")
            else:
                businesses.extend(dir_res.get("results", []))
        except Exception as exc:
            errors.append(f"Directories module error: {exc}")

    deduped = []
    seen = {}
    for biz in businesses:
        name_key = "".join(ch for ch in (biz.get("business_name", "").lower().strip()) if ch.isalnum())
        city_key = (biz.get("city") or "").strip().lower()
        key = f"{name_key}|{city_key}"
        if not name_key:
            continue

        if key not in seen:
            seen[key] = biz
            deduped.append(biz)
        else:
            existing = seen[key]
            for field in ("instagram_url", "facebook_url", "whatsapp", "website", "phone", "email"):
                if biz.get(field) and not existing.get(field):
                    existing[field] = biz[field]

    gm_only = [b for b in deduped if b.get("source") == "google_maps"]
    non_gm = [b for b in deduped if b.get("source") != "google_maps"]
    if gm_only:
        deduped = enrich_with_details(gm_only, max_detail_lookups=max_results, api_key=api_key) + non_gm

    if not deduped and errors:
        return {"error": " | ".join(errors), "results": [], "total": 0, "debug_errors": errors}

    limit = max_results if source_choice != "all" else max_results * 4
    return {
        "error": None,
        "results": deduped[:limit],
        "total": len(deduped),
        "debug_errors": errors,
    }
