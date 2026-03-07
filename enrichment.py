"""
Lead enrichment and qualification helpers.
"""
from __future__ import annotations

import time
from urllib.parse import urlparse

import requests


TYPE_KEYWORDS = {
    "restaurant": [
        "restaurant",
        "ristorante",
        "pizzeria",
        "trattoria",
        "osteria",
        "bistro",
        "cafe",
        "cafeteria",
        "bar",
        "pub",
        "food",
        "bakery",
        "gelateria",
    ],
    "hotel": [
        "hotel",
        "albergo",
        "b&b",
        "bed and breakfast",
        "hostel",
        "resort",
        "motel",
        "agriturismo",
        "lodge",
        "inn",
        "aparthotel",
    ],
    "service": [
        "salon",
        "salone",
        "parrucchiere",
        "barber",
        "spa",
        "beauty",
        "gym",
        "fitness",
        "yoga",
        "pilates",
        "clinic",
        "dentist",
        "massage",
        "nail",
    ],
    "ecommerce": [
        "shop",
        "store",
        "negozio",
        "boutique",
        "market",
        "retail",
        "fashion",
        "clothing",
        "jewelry",
        "gioielleria",
    ],
    "local_service": [
        "plumber",
        "idraulico",
        "electrician",
        "elettricista",
        "carpenter",
        "mechanic",
        "officina",
        "cleaning",
        "contractor",
        "painter",
        "locksmith",
        "mover",
    ],
}

business_types = {
    "restaurant": {"label": "Restaurants & Cafes"},
    "hotel": {"label": "Hotels & Lodging"},
    "service": {"label": "Beauty & Wellness Services"},
    "ecommerce": {"label": "Retail & E-Commerce"},
    "local_service": {"label": "Home & Local Services"},
    "other": {"label": "Other Businesses"},
}


def detect_business_type(category: str) -> str:
    if not category:
        return "other"
    text = category.lower()
    for btype, keywords in TYPE_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            return btype
    return "other"


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    value = str(url).strip().strip("\"' ")
    parsed = urlparse(value)
    if not parsed.scheme and not parsed.netloc:
        parsed = urlparse(f"https://{value}")
    scheme = parsed.scheme.lower() if parsed.scheme else "https"
    if scheme not in ("http", "https"):
        scheme = "https"
    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""
    if "/" in netloc and not path:
        netloc, rest = netloc.split("/", 1)
        path = f"/{rest}"
    if not netloc:
        return ""
    return f"{scheme}://{netloc}{path}"


def _url_variants(url: str):
    normalized = _normalize_url(url)
    if not normalized:
        return []
    parsed = urlparse(normalized)
    alt_scheme = "http" if parsed.scheme == "https" else "https"
    primary = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    alternate = f"{alt_scheme}://{parsed.netloc}{parsed.path}"
    return [primary] if primary == alternate else [primary, alternate]


def check_website(url: str, timeout: float = 6.0, connect_timeout: float = 4.0, read_timeout: float = 8.0) -> dict:
    """
    Website quality check with URL normalization, redirect handling,
    SSL fallback, and debug metadata.
    """
    result = {
        "status": "none",
        "response_time_ms": 0,
        "has_ssl": False,
        "has_mobile": False,
        "cms_detected": None,
        "has_contact_form": False,
        "has_cta": False,
        "website_score": 0,
        "status_code": 0,
        "final_url": "",
        "fetch_error": "",
    }

    if not url:
        result["fetch_error"] = "missing_website"
        return result

    timeout_tuple = (max(1.0, float(connect_timeout)), max(1.0, float(read_timeout)))
    response = None
    fetch_error = ""

    for candidate in _url_variants(url):
        try:
            start = time.time()
            response = requests.get(
                candidate,
                timeout=timeout_tuple,
                allow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
            )
            result["response_time_ms"] = int((time.time() - start) * 1000)
            if response.status_code < 500:
                break
            response = None
        except requests.exceptions.SSLError:
            try:
                start = time.time()
                response = requests.get(
                    candidate,
                    timeout=timeout_tuple,
                    allow_redirects=True,
                    verify=False,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    },
                )
                result["response_time_ms"] = int((time.time() - start) * 1000)
                if response.status_code < 500:
                    break
                response = None
            except Exception as exc:
                fetch_error = f"ssl_error:{exc}"
        except requests.exceptions.Timeout:
            fetch_error = "timeout"
        except Exception as exc:
            fetch_error = f"request_error:{exc}"

    if response is None:
        result["status"] = "error"
        result["fetch_error"] = fetch_error or "unreachable"
        return result

    result["status_code"] = int(response.status_code)
    result["final_url"] = response.url
    result["has_ssl"] = response.url.startswith("https://")

    if response.status_code >= 400:
        result["status"] = "error"
        result["fetch_error"] = f"http_{response.status_code}"
        return result

    html = (response.text or "").lower()
    result["has_mobile"] = "meta name=\"viewport\"" in html or "meta name='viewport'" in html

    cms = None
    if "wix.com" in html or "x-wix" in response.headers.get("x-wix-request-id", "").lower():
        cms = "wix"
    elif "wp-content" in html or "wordpress" in html:
        cms = "wordpress"
    elif "squarespace" in html:
        cms = "squarespace"
    elif "shopify" in html:
        cms = "shopify"
    elif "weebly" in html:
        cms = "weebly"
    elif "godaddy" in html:
        cms = "godaddy"
    result["cms_detected"] = cms

    result["has_contact_form"] = "<form" in html and (
        "email" in html or "message" in html or "contact" in html or "kontakt" in html
    )

    cta_keywords = ["book now", "schedule", "appointment", "buy now", "get started", "call us", "contact us"]
    result["has_cta"] = any(keyword in html for keyword in cta_keywords)

    elapsed = result["response_time_ms"]
    result["status"] = "ok" if elapsed < 3000 else "slow"

    score = 0
    if result["has_ssl"]:
        score += 15
    if result["has_mobile"]:
        score += 20
    if result["has_contact_form"]:
        score += 15
    if result["has_cta"]:
        score += 15

    if elapsed < 2000:
        score += 20
    elif elapsed < 4000:
        score += 10

    if result["cms_detected"] not in {"wix", "weebly", "godaddy", "squarespace"}:
        score += 15

    result["website_score"] = min(100, score)
    return result


def score_business(biz: dict) -> dict:
    """
    Score a business (0-100) based on website quality and market signals.
    """
    score = 0
    details = []

    ws = biz.get("website_check", {})
    has_website = bool((biz.get("website") or "").strip())

    if not has_website:
        score += 25
        details.append("No website")
        if biz.get("instagram_url") or biz.get("facebook_url"):
            score += 20
            details.append("Social active without website")
    else:
        if ws.get("status") in {"error", "slow"}:
            score += 15
            details.append("Broken or slow website")
        if not ws.get("has_mobile"):
            score += 10
            details.append("Not mobile friendly")
        if ws.get("cms_detected") in {"wix", "weebly", "godaddy", "squarespace"}:
            score += 20
            details.append(f"Basic CMS ({ws.get('cms_detected')})")

    rating = float(biz.get("google_rating", 0) or 0)
    if rating >= 4.0:
        score += 10
        details.append(f"Good rating ({rating})")

    reviews = int(biz.get("review_count", 0) or 0)
    if 10 <= reviews <= 500:
        score += 10
        details.append(f"Active reviews ({reviews})")

    if biz.get("is_new_business"):
        score += 15
        details.append("New business")

    return {
        "score": min(score, 100),
        "details": details,
        "pain_points": detect_pain_points(biz),
    }


def assign_tier(score: int) -> int:
    if score >= 60:
        return 1
    if score >= 35:
        return 2
    return 3


def choose_channel(biz: dict) -> str:
    if not biz.get("website") and biz.get("instagram_url"):
        return "instagram_dm"
    if not biz.get("website") and biz.get("whatsapp"):
        return "whatsapp"

    ws = biz.get("website_check", {})
    if biz.get("website") and ws.get("website_score", 100) < 50:
        return "email"
    return "email"


def detect_pain_points(biz: dict):
    pains = []
    ws = biz.get("website_check", {})

    if not biz.get("website"):
        pains.append("No website")
    elif ws.get("status") == "error":
        pains.append("Website has errors")
    elif ws.get("status") == "slow":
        pains.append("Slow website")

    if ws and not ws.get("has_mobile"):
        pains.append("Not mobile friendly")

    rating = float(biz.get("google_rating", 0) or 0)
    reviews = int(biz.get("review_count", 0) or 0)
    if rating > 0 and rating < 4.0:
        pains.append("Below-average rating")
    if 0 < reviews < 20:
        pains.append("Few reviews")

    return pains


def is_good_business(biz: dict):
    """
    Qualification gate for automatic sending.
    """
    if not biz.get("email"):
        return False, "No email"

    reviews = int(biz.get("review_count", 0) or 0)
    if reviews > 2500:
        return False, f"Too famous ({reviews} reviews)"

    if not is_good_email(biz.get("email", ""), biz.get("website", "")):
        return False, "Bad email"

    return True, "OK"


def is_good_email(email: str, website: str | None = None) -> bool:
    if not email:
        return False

    value = email.lower().strip()
    if value.count("@") != 1:
        return False

    prefix, domain = value.split("@", 1)
    if not prefix or not domain or "." not in domain:
        return False

    bad_prefixes = {
        "noreply",
        "no-reply",
        "mailer-daemon",
        "postmaster",
        "webmaster",
        "abuse",
        "hostmaster",
        "root",
        "help",
        "tickets",
        "donotreply",
        "do-not-reply",
        "bounces",
        "bounce",
        "daemon",
        "test",
        "demo",
        "sample",
        "example",
        "dummy",
    }
    if prefix in bad_prefixes:
        return False

    bad_domains = {
        "example.com",
        "example.org",
        "example.net",
        "domain.com",
        "sentry.io",
        "wixpress.com",
        "wordpress.com",
        "wpmail.com",
        "w3.org",
        "schema.org",
        "googleapis.com",
        "google.com",
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "tiktok.com",
        "squarespace.com",
        "shopify.com",
        "weebly.com",
        "mailchimp.com",
        "amazonaws.com",
        "cloudflare.com",
        "localhost",
        "test.com",
        "invalid.com",
        "email.com",
        "mybusiness.com",
    }
    if domain in bad_domains:
        return False

    bad_extensions = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".php", ".html", ".webp"}
    if any(value.endswith(ext) for ext in bad_extensions):
        return False

    parts = domain.split(".")
    if len(parts) < 2 or len(parts[-1]) < 2:
        return False

    return True


def _get_domain(url: str) -> str:
    if not url:
        return ""
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain
