"""
Business Discovery Module.
Find businesses using Google Places API or web search fallback.
"""
import requests
import json
import time
from config import GOOGLE_PLACES_API_KEY


def search_google_places(query, location=None, max_results=60):
    """
    Search for businesses using Google Places API Text Search.
    Returns list of business dicts.
    """
    if not GOOGLE_PLACES_API_KEY:
        return {"error": "Google Places API key not configured. Add it in Settings.", "results": []}

    all_results = []
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

    search_query = query
    if location:
        search_query = f"{query} in {location}"

    params = {
        "query": search_query,
        "key": GOOGLE_PLACES_API_KEY,
    }

    try:
        # First page
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()

        if data.get("status") == "REQUEST_DENIED":
            return {"error": f"API Error: {data.get('error_message', 'Request denied')}", "results": []}

        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            return {"error": f"API Error: {data.get('status')}", "results": []}

        all_results.extend(data.get("results", []))

        # Get additional pages (up to 3 pages = ~60 results)
        pages = 1
        while data.get("next_page_token") and len(all_results) < max_results and pages < 3:
            time.sleep(2)  # Google requires delay between page requests
            params["pagetoken"] = data["next_page_token"]
            if "query" in params:
                del params["query"]
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
            all_results.extend(data.get("results", []))
            pages += 1

    except Exception as e:
        return {"error": f"Request failed: {str(e)}", "results": []}

    # Get details for each place
    businesses = []
    for place in all_results[:max_results]:
        biz = {
            "business_name": place.get("name", ""),
            "address": place.get("formatted_address", ""),
            "google_rating": place.get("rating", 0.0),
            "review_count": place.get("user_ratings_total", 0),
            "place_id": place.get("place_id", ""),
            "types": place.get("types", []),
            "source": "google_maps",
            "city": location,
            "website": "",
            "phone": "",
            "email": "",
        }

        # Extract clean city from address (Ignore Postcodes)
        addr_parts = biz["address"].split(",")
        city_name = ""
        if len(addr_parts) >= 2:
            # Usually the second to last part contains the city and postcode
            raw_city_part = addr_parts[-2].strip()
            # Remove any postcodes (numbers or mixed letters/numbers like 2QW)
            # Also remove exactly 2-letter fully uppercase words (Italian province codes like RM, MI)
            clean_words = []
            for word in raw_city_part.split():
                if any(char.isdigit() for char in word):
                    continue
                if len(word) == 2 and word.isupper():
                    continue
                clean_words.append(word)
            city_name = " ".join(clean_words)
        
        biz["city"] = city_name

        businesses.append(biz)

    return {"error": None, "results": businesses, "total": len(businesses)}


def get_place_details(place_id):
    """
    Get detailed info for a specific place (website, phone, etc).
    """
    if not GOOGLE_PLACES_API_KEY:
        return {}

    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,url,opening_hours,business_status",
        "key": GOOGLE_PLACES_API_KEY,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()

        if data.get("status") != "OK":
            return {}

        result = data.get("result", {})
        return {
            "website": result.get("website", ""),
            "phone": result.get("formatted_phone_number", ""),
            "business_status": result.get("business_status", ""),
        }
    except Exception:
        return {}


def enrich_with_details(businesses, max_detail_lookups=30):
    """
    Enrich a list of businesses with detailed info (website, phone).
    Only does detail lookups for up to max_detail_lookups businesses to save API calls.
    """
    enriched = []
    detail_count = 0

    for biz in businesses:
        if biz.get("place_id") and detail_count < max_detail_lookups:
            details = get_place_details(biz["place_id"])
            if details:
                biz["website"] = details.get("website", biz.get("website", ""))
                biz["phone"] = details.get("phone", biz.get("phone", ""))
            detail_count += 1
            time.sleep(0.2)  # Rate limiting

        enriched.append(biz)

    return enriched


def search_businesses(business_type, location, source_choice="all", max_results=60):
    """
    Main entry point: search for businesses across multiple sources and enrich with details.
    source_choice can be 'all', 'google_maps', 'instagram', 'facebook', 'directories'.
    """
    businesses = []
    errors = []
    
    query = f"{business_type} {location}" if location else business_type

    # 1. Google Maps
    if source_choice in ("all", "google_maps"):
        api_key = GOOGLE_PLACES_API_KEY
        try:
            from database import get_settings
            settings = get_settings()
            api_key = settings.get("google_places_api_key", api_key)
        except Exception:
            pass

        if not api_key:
            errors.append("Google Maps skipped: No API key.")
        else:
            import config
            original_key = config.GOOGLE_PLACES_API_KEY
            config.GOOGLE_PLACES_API_KEY = api_key
            
            gm_res = search_google_places(query, max_results=max_results)
            if gm_res.get("error"):
                errors.append(f"Google Maps error: {gm_res['error']}")
            else:
                businesses.extend(gm_res["results"])
                
            config.GOOGLE_PLACES_API_KEY = original_key

    # 2. Instagram
    if source_choice in ("all", "instagram"):
        try:
            from source_instagram import search_instagram
            ig_res = search_instagram(business_type, location, max_results=20 if source_choice=="all" else max_results)
            if ig_res.get("error"):
                errors.append(f"Instagram error: {ig_res['error']}")
            else:
                businesses.extend(ig_res["results"])
        except Exception as e:
            errors.append(f"IG module error: {e}")
            
    # 3. Facebook
    if source_choice in ("all", "facebook"):
        try:
            from source_facebook import search_facebook
            fb_res = search_facebook(business_type, location, max_results=20 if source_choice=="all" else max_results)
            if fb_res.get("error"):
                errors.append(f"Facebook error: {fb_res['error']}")
            else:
                businesses.extend(fb_res["results"])
        except Exception as e:
            errors.append(f"FB module error: {e}")

    # 4. Directories (Yelp/YP)
    if source_choice in ("all", "directories"):
        try:
            from source_directories import search_directories
            dir_res = search_directories(business_type, location, max_results=20 if source_choice=="all" else max_results)
            if dir_res.get("error"):
                errors.append(f"Directories error: {dir_res['error']}")
            else:
                businesses.extend(dir_res["results"])
        except Exception as e:
            errors.append(f"Directories module error: {e}")

    # Deduplicate by business_name (fuzzy approach via lowercasing and standardizing spacing)
    seen = {}
    deduped = []
    
    for biz in businesses:
        name_key = biz.get("business_name", "").lower().strip()
        # Remove special chars for deduplication key
        name_key = ''.join(e for e in name_key if e.isalnum())
        
        if not name_key:
            continue
            
        if name_key not in seen:
            seen[name_key] = biz
            deduped.append(biz)
        else:
            # Merge useful info from duplicates
            existing = seen[name_key]
            for field in ["instagram_url", "facebook_url", "whatsapp", "website", "phone"]:
                if biz.get(field) and not existing.get(field):
                    existing[field] = biz[field]

    # Google Maps results require detail lookups for website/phone
    gm_only = [b for b in deduped if b.get("source") == "google_maps"]
    non_gm = [b for b in deduped if b.get("source") != "google_maps"]
    
    if gm_only:
        enriched_gm = enrich_with_details(gm_only, max_detail_lookups=max_results)
        deduped = enriched_gm + non_gm

    if not deduped and errors:
        return {
            "error": " | ".join(errors),
            "results": [],
            "total": 0,
        }

    return {
        "error": None,
        "results": deduped[:max_results if source_choice != "all" else max_results*4],
        "total": len(deduped),
        "debug_errors": errors
    }
