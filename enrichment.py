"""
Lead Enrichment — Universal scoring for ALL business types.
One simple formula: rating + reviews + website quality + pain points.
"""
import requests
import time
from urllib.parse import urlparse


# ═══════════════════════════════════════════════════════════
# BUSINESS TYPE DETECTION
# ═══════════════════════════════════════════════════════════

TYPE_KEYWORDS = {
    "restaurant": ["restaurant", "ristorante", "pizzeria", "trattoria", "osteria",
                   "bistro", "café", "cafe", "bar", "pub", "taverna", "food",
                   "sushi", "burger", "grill", "bakery", "pasticceria", "gelateria"],
    "hotel": ["hotel", "albergo", "b&b", "bed and breakfast", "hostel",
              "resort", "motel", "agriturismo", "lodge", "inn", "aparthotel"],
    "service": ["salon", "salone", "parrucchiere", "barbiere", "barber",
                "spa", "beauty", "estetica", "gym", "palestra", "fitness",
                "yoga", "pilates", "clinica", "dentist", "veterinario",
                "physiotherapy", "massage", "nail"],
    "ecommerce": ["shop", "store", "negozio", "boutique", "market",
                  "retail", "fashion", "clothing", "jewelry", "gioielleria"],
    "local_service": ["plumber", "idraulico", "electrician", "elettricista",
                      "carpenter", "mechanic", "officina", "cleaning",
                      "contractor", "painter", "locksmith", "mover"],
}

business_types = {
    "restaurant": {"label": "Restaurants & Cafes"},
    "hotel": {"label": "Hotels & Lodging"},
    "service": {"label": "Beauty & Wellness Services"},
    "ecommerce": {"label": "Retail & E-Commerce"},
    "local_service": {"label": "Home & Local Services"},
    "other": {"label": "Other Businesses"}
}


def detect_business_type(category):
    """Detect business type from category/name. Returns type key."""
    if not category:
        return "other"
    text = category.lower()
    for btype, keywords in TYPE_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                return btype
    return "other"


# ═══════════════════════════════════════════════════════════
# WEBSITE CHECK
# ═══════════════════════════════════════════════════════════

def check_website(url):
    """
    Deep website quality check. Returns dict with:
    - status, response_time_ms, has_ssl, has_mobile, cms_detected
    - has_contact_form, has_cta, website_score
    """
    result = {
        "status": "none",
        "response_time_ms": 0,
        "has_ssl": False,
        "has_mobile": False,
        "cms_detected": None,
        "has_contact_form": False,
        "has_cta": False,
        "website_score": 0
    }
    
    if not url:
        return result

    original_url = url
    if not url.startswith(("http://", "https://")):
        url = "http://" + url  # Start with HTTP to test SSL redirect

    try:
        start = time.time()
        resp = requests.get(url, timeout=5, allow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        elapsed = int((time.time() - start) * 1000)
        
        if resp.status_code != 200:
            result["status"] = "error"
            result["response_time_ms"] = elapsed
            return result

        html = resp.text.lower()
        
        # 1. SSL Check
        result["has_ssl"] = resp.url.startswith("https://")
        
        # 2. Mobile Check
        result["has_mobile"] = "meta name=\"viewport\"" in html or "meta name='viewport'" in html
        
        # 3. CMS Detection
        cms = None
        if "wix.com" in html or "x-wix" in resp.headers.get("x-wix-request-id", "").lower():
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
        
        # 4. Contact Form Check
        # Looking for standard form tags that might have email/message fields
        result["has_contact_form"] = "<form" in html and ("email" in html or "message" in html or "contact" in html)
        
        # 5. CTA Check
        cta_keywords = ["book now", "schedule", "appointment", "buy now", "get started", "call us", "contact us"]
        result["has_cta"] = any(cta in html for cta in cta_keywords)
        
        # 6. Final Data Assembly
        result["response_time_ms"] = elapsed
        result["status"] = "ok" if elapsed < 3000 else "slow"
        
        # Compute Score (0-100)
        score = 0
        if result["has_ssl"]: score += 15
        if result["has_mobile"]: score += 20
        if result["has_contact_form"]: score += 15
        if result["has_cta"]: score += 15
        
        # Speed scoring
        if elapsed < 2000: score += 20
        elif elapsed < 4000: score += 10
        
        # CMS Penalty (bonus if custom/pro)
        if result["cms_detected"] not in ["wix", "weebly", "godaddy", "squarespace"]:
            score += 15  # Reward custom builds or pro WP/Shopify setups
            
        result["website_score"] = min(100, score)
        
        return result

    except Exception:
        result["status"] = "error"
        return result


# ═══════════════════════════════════════════════════════════
# UNIVERSAL SCORING — Same for ALL business types
# ═══════════════════════════════════════════════════════════

def score_business(biz):
    """
    Score a business 0-100 based on likelihood of needing our services.
    v5 Enhanced Scoring Formula:
    - No website: +25
    - Free/Basic CMS: +20
    - Broken/slow website: +15
    - Not mobile friendly: +10
    - Social active but no website: +20
    - New business: +15
    - Good Google Rating (4.0+): +10
    - Active Reviews (10-500): +10
    """
    score = 0
    details = []

    ws = biz.get("website_check", {})
    has_website = bool(biz.get("website"))

    # --- Website Quality (0-40) ---
    if not has_website:
        score += 25
        details.append("🚫 No website — top priority")
        
        # Social active, no website
        if biz.get("instagram_url") or biz.get("facebook_url"):
            score += 20
            details.append("📱 Active on social but no website")
    else:
        if ws.get("status") in ["error", "slow"]:
            score += 15
            details.append("🐌 Broken/slow website")
            
        if not ws.get("has_mobile"):
            score += 10
            details.append("🖥️ Not mobile-friendly")
            
        cms = ws.get("cms_detected")
        if cms in ["wix", "weebly", "godaddy", "squarespace"]:
            score += 20
            details.append(f"🛠️ Uses basic CMS ({cms})")

    # --- Google Rating & Reviews (0-20) ---
    rating = float(biz.get("google_rating", 0) or 0)
    if rating >= 4.0:
        score += 10
        details.append(f"⭐ Good reputation ({rating})")

    reviews = int(biz.get("review_count", 0) or 0)
    if 10 <= reviews <= 500:
        score += 10
        details.append(f"📊 Active reviews ({reviews})")

    # --- New Business Bonus (0-15) ---
    if biz.get("is_new_business"):
        score += 15
        details.append("🎉 Newly discovered business")

    pain = detect_pain_points(biz)

    return {
        "score": min(score, 100),
        "details": details,
        "pain_points": pain,
    }

def assign_tier(score):
    """Auto-assign lead to Tier 1 (Hot), Tier 2 (Warm), or Tier 3 (Cold)"""
    if score >= 60:
        return 1
    elif score >= 35:
        return 2
    else:
        return 3

def choose_channel(biz):
    """Intelligently route outreach to the best channel based on available data."""
    # No website but Instagram -> IG DM
    if not biz.get("website") and biz.get("instagram_url"):
        return "instagram_dm"
        
    # No website but WhatsApp -> WA message
    if not biz.get("website") and biz.get("whatsapp"):
        return "whatsapp"
        
    # Bad website score -> perfect for cold email pitch
    ws = biz.get("website_check", {})
    if biz.get("website") and ws.get("website_score", 100) < 50:
        return "email"
        
    # Default fallback
    return "email"


def detect_pain_points(biz):
    """Detect specific pain points to personalize the outreach."""
    pains = []

    ws = biz.get("website_check", {})
    if not biz.get("website"):
        pains.append("No website")
    elif ws.get("status") == "error":
        pains.append("Website has errors")
    elif ws.get("status") == "slow":
        pains.append("Slow website")

    if ws and not ws.get("has_mobile"):
        pains.append("Not mobile-friendly")

    rating = float(biz.get("google_rating", 0) or 0)
    reviews = int(biz.get("review_count", 0) or 0)

    if rating > 0 and rating < 4.0:
        pains.append("Below-average rating")
    if 0 < reviews < 20:
        pains.append("Few reviews")

    return pains


# ═══════════════════════════════════════════════════════════
# QUALITY FILTER — Only send to GOOD businesses
# ═══════════════════════════════════════════════════════════

def is_good_business(biz):
    """
    Check if a business is worth contacting.
    - Valid email
    - Rating 4.0+ (ignore low ratings)
    - Under 500 reviews (filter out super famous ones)
    """
    # Must have email
    if not biz.get("email"):
        return False, "No email"

    # Fame filter: Ignore places with massive review counts (likely corporate/famous)
    reviews = int(biz.get("review_count", 0) or 0)
    if reviews > 2500:
        return False, f"Too famous ({reviews} reviews)"

    # Must have valid email (domain matches, no bad prefix)
    if not is_good_email(biz.get("email", ""), biz.get("website", "")):
        return False, "Bad email"

    score = biz.get("score", biz.get("website_score", 0))
    # We allow lower scores to pass qualification; 
    # campaigns or manual sorting can filter further based on tier.


    return True, "OK"


def is_good_email(email, website=None):
    """
    Fast email quality check — format + blacklist only.
    Filters out bad system prefixes and dummy domains.
    """
    if not email:
        return False

    email = email.lower().strip()

    # Must have exactly one @ with content on both sides
    if email.count("@") != 1:
        return False
    prefix, domain = email.split("@")
    if not prefix or not domain or "." not in domain:
        return False

    # Reject obvious system/role prefixes
    BAD_PREFIXES = {
        "noreply", "no-reply", "mailer-daemon", "postmaster", "webmaster",
        "abuse", "hostmaster", "root", "help", "tickets", "donotreply",
        "do-not-reply", "bounces", "bounce", "daemon", "admin", "user",
        "test", "demo", "sample", "example", "dummy"
    }
    if prefix in BAD_PREFIXES:
        return False

    # Reject known platform/CDN/tracking and dummy domains
    BAD_DOMAINS = {
        "example.com", "example.org", "example.net", "domain.com",
        "sentry.io", "wixpress.com", "sentry-next.wixpress.com",
        "wordpress.com", "wpmail.com", "w3.org", "schema.org", 
        "googleapis.com", "google.com", "facebook.com", "instagram.com", 
        "twitter.com", "tiktok.com", "squarespace.com", "shopify.com", 
        "weebly.com", "mailchimp.com", "amazonaws.com", "cloudflare.com",
        "localhost", "test.com", "invalid.com", "email.com", "mybusiness.com"
    }
    if domain in BAD_DOMAINS:
        return False

    # Reject emails that end with file extensions
    BAD_EXTENSIONS = {".png", ".jpg", ".gif", ".svg", ".css", ".js", ".php", ".html", ".webp"}
    if any(email.endswith(ext) for ext in BAD_EXTENSIONS):
        return False

    # Reject single-character domains or obviously fake TLDs
    parts = domain.split(".")
    if len(parts) < 2 or len(parts[-1]) < 2:
        return False

    # All good — accept the email
    return True



def _get_domain(url):
    """Extract clean domain from URL."""
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        d = parsed.netloc.lower()
        if d.startswith("www."):
            d = d[4:]
        return d
    except Exception:
        return ""
