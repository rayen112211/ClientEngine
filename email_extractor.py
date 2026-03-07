"""
Email Extractor Module.
Hardened multi-step extraction with practical validation and debug metadata.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/contact-us/",
    "/contacts",
    "/about",
    "/about-us",
    "/team",
    "/impressum",
    "/kontakt",
    "/kontakt/",
    "/contatti",
    "/contatto",
    "/chi-siamo",
    "/a-propos",
    "/nous-contacter",
    "/mentions-legales",
    "/privacy",
    "/terms",
]

CONTACT_HINTS = {
    "contact",
    "kontakt",
    "contatt",
    "about",
    "team",
    "impress",
    "mentions",
    "privacy",
    "legal",
    "a-propos",
    "nous-contacter",
    "support",
    "help",
}

COMMON_PREFIXES = {
    "info",
    "contact",
    "hello",
    "office",
    "booking",
    "reservations",
    "sales",
    "admin",
    "biuro",      # Polish
    "kontakt",    # Polish/German
    "prenotazioni",  # Italian
    "commercial",    # French
}

EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,24}", re.IGNORECASE)

# Obfuscated variants like "name [at] domain [dot] com" / "name(at)domain(dot)pl"
OBFUSCATED_AT_REGEX = re.compile(r"\s*(\[at\]|\(at\)|\{at\}|\sat\s|\(at\)|\[\s*@\s*\])\s*", re.IGNORECASE)
OBFUSCATED_DOT_REGEX = re.compile(r"\s*(\[dot\]|\(dot\)|\{dot\}|\sdot\s|\(dot\)|\[\s*\.\s*\])\s*", re.IGNORECASE)

EXCLUDED_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "domain.com",
    "localhost",
    "test.com",
    "invalid.com",
    "email.com",
    "mybusiness.com",
    "google.com",
    "googleapis.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "tiktok.com",
    "wixpress.com",
    "wordpress.com",
    "wpmail.com",
    "squarespace.com",
    "shopify.com",
    "mailchimp.com",
    "cloudflare.com",
    "amazonaws.com",
}

EXCLUDED_PREFIXES = {
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "mailer-daemon",
    "postmaster",
    "hostmaster",
    "abuse",
    "root",
    "webmaster",
    "bounce",
    "bounces",
    "daemon",
    "test",
    "demo",
    "sample",
    "example",
    "dummy",
}

FILE_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".woff",
    ".woff2",
    ".ttf",
    ".php",
    ".html",
    ".xml",
}


@dataclass
class FetchResult:
    ok: bool
    url: str
    final_url: str
    status_code: int
    html: str
    error: str


def _strip_www(domain: str) -> str:
    d = (domain or "").strip().lower()
    if d.startswith("www."):
        return d[4:]
    return d


def normalize_website_url(website_url: str) -> str:
    """
    Normalize user/Places website values into a fetchable absolute URL.
    """
    raw = (website_url or "").strip()
    if not raw:
        return ""

    raw = raw.strip("\"' ")
    parsed = urlparse(raw)

    if not parsed.scheme and not parsed.netloc:
        # Input like "example.com" or "example.com/path"
        parsed = urlparse(f"https://{raw}")
    elif not parsed.scheme and parsed.netloc:
        parsed = parsed._replace(scheme="https")

    scheme = parsed.scheme.lower() if parsed.scheme else "https"
    if scheme not in ("http", "https"):
        scheme = "https"

    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""
    if "/" in netloc and not path:
        first, rest = netloc.split("/", 1)
        netloc = first
        path = f"/{rest}"

    netloc = netloc.strip().strip(".")
    if not netloc:
        return ""

    return urlunparse((scheme, netloc, path or "", "", "", ""))


def _candidate_urls(normalized_url: str) -> List[str]:
    if not normalized_url:
        return []
    parsed = urlparse(normalized_url)
    base = urlunparse((parsed.scheme, parsed.netloc, parsed.path or "", "", "", ""))
    alt_scheme = "http" if parsed.scheme == "https" else "https"
    alt = urlunparse((alt_scheme, parsed.netloc, parsed.path or "", "", "", ""))
    if base == alt:
        return [base]
    return [base, alt]


def validate_email(email: str) -> bool:
    """
    Fast practical validation for extraction.
    Intentionally avoids DNS/SMTP probes because they are slow and unreliable in batch crawling.
    """
    if not email:
        return False

    value = email.lower().strip().strip("'\";,.:")
    if "mailto:" in value:
        value = value.split("mailto:", 1)[1].split("?", 1)[0].strip()

    if not EMAIL_REGEX.fullmatch(value):
        return False

    local, domain = value.split("@", 1)
    if not local or not domain or "." not in domain:
        return False

    if local in EXCLUDED_PREFIXES:
        return False
    if domain in EXCLUDED_DOMAINS:
        return False

    if any(value.endswith(suffix) for suffix in FILE_SUFFIXES):
        return False

    if ".." in value or local.startswith(".") or local.endswith("."):
        return False

    tld = domain.rsplit(".", 1)[-1]
    if len(tld) < 2:
        return False

    return True


def _safe_email(value: str) -> Optional[str]:
    candidate = (value or "").lower().strip().strip("'\";,.:)")
    if validate_email(candidate):
        return candidate
    return None


def _extract_regex_emails(text: str) -> Set[str]:
    out = set()
    if not text:
        return out
    for match in EMAIL_REGEX.findall(text):
        email = _safe_email(match)
        if email:
            out.add(email)
    return out


def _extract_obfuscated_emails(text: str) -> Set[str]:
    out = set()
    if not text:
        return out

    normalized = OBFUSCATED_AT_REGEX.sub("@", text)
    normalized = OBFUSCATED_DOT_REGEX.sub(".", normalized)
    normalized = normalized.replace(" [@] ", "@").replace(" [.] ", ".")

    for match in EMAIL_REGEX.findall(normalized):
        email = _safe_email(match)
        if email:
            out.add(email)
    return out


def _extract_mailto_emails(soup: BeautifulSoup) -> Set[str]:
    out = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if href.lower().startswith("mailto:"):
            parsed = unquote(href[7:]).split("?", 1)[0]
            email = _safe_email(parsed)
            if email:
                out.add(email)
    return out


def _source_confidence(source: str, email: str, site_domain: str) -> float:
    base = 0.72
    src = (source or "").lower()

    if "mailto" in src:
        base = 0.98
    elif "homepage" in src:
        base = 0.90
    elif "contact" in src or "kontakt" in src or "contatt" in src:
        base = 0.92
    elif "footer" in src or "header" in src:
        base = 0.86
    elif "obfuscated" in src:
        base = 0.80

    local, domain = email.split("@", 1)
    if _strip_www(domain) == _strip_www(site_domain):
        base += 0.04
    if local in COMMON_PREFIXES:
        base += 0.02

    return min(base, 0.99)


def _add_candidates(
    out: Dict[str, Dict[str, object]],
    emails: Iterable[str],
    source: str,
    site_domain: str,
) -> None:
    for email in emails:
        confidence = _source_confidence(source, email, site_domain)
        existing = out.get(email)
        if existing is None or confidence > float(existing.get("confidence", 0)):
            out[email] = {"email": email, "source": source, "confidence": confidence}


def _extract_candidates_from_html(html: str, source_prefix: str, page_url: str, site_domain: str) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    if not html:
        return out

    soup = BeautifulSoup(html, "html.parser")

    # Raw HTML scan
    _add_candidates(out, _extract_regex_emails(html), f"{source_prefix}:html", site_domain)

    # Mailto links
    _add_candidates(out, _extract_mailto_emails(soup), f"{source_prefix}:mailto", site_domain)

    # Text scan + obfuscated scan
    text = soup.get_text(" ", strip=True)
    _add_candidates(out, _extract_regex_emails(text), f"{source_prefix}:text", site_domain)
    _add_candidates(out, _extract_obfuscated_emails(text), f"{source_prefix}:obfuscated", site_domain)

    # Header/footer focused scan
    for tag_name in ("header", "footer"):
        for tag in soup.find_all(tag_name):
            tag_text = tag.get_text(" ", strip=True)
            if not tag_text:
                continue
            _add_candidates(out, _extract_regex_emails(tag_text), f"{source_prefix}:{tag_name}", site_domain)
            _add_candidates(out, _extract_obfuscated_emails(tag_text), f"{source_prefix}:{tag_name}_obfuscated", site_domain)

    return out


def _collect_contact_links(html: str, page_url: str, site_domain: str) -> List[str]:
    links: List[str] = []
    if not html:
        return links

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(page_url, href)
        parsed = urlparse(full)
        if parsed.scheme not in ("http", "https"):
            continue
        if _strip_www(parsed.netloc) != _strip_www(site_domain):
            continue

        label = f"{href} {a.get_text(' ', strip=True)}".lower()
        if any(token in label for token in CONTACT_HINTS):
            links.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path or "", "", "", "")))

    return links


def _fetch_page(
    session: requests.Session,
    url: str,
    timeout: Tuple[float, float],
    verify: bool = True,
) -> FetchResult:
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9,pl;q=0.7,it;q=0.6,fr;q=0.5"}
    try:
        response = session.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=verify)
        ok = response.status_code < 400 and bool(response.text)
        return FetchResult(
            ok=ok,
            url=url,
            final_url=response.url,
            status_code=response.status_code,
            html=response.text or "",
            error="" if ok else f"http_{response.status_code}",
        )
    except requests.exceptions.SSLError as exc:
        return FetchResult(False, url, url, 0, "", f"ssl_error:{exc}")
    except requests.exceptions.Timeout:
        return FetchResult(False, url, url, 0, "", "timeout")
    except requests.exceptions.RequestException as exc:
        return FetchResult(False, url, url, 0, "", f"request_error:{exc}")


def extract_email_from_website(
    website_url: str,
    *,
    request_timeout: Tuple[float, float] = (4.0, 7.0),
    max_pages: int = 8,
    max_contact_pages: int = 6,
) -> Dict[str, object]:
    """
    Try to extract contact emails from a website using layered fallbacks.
    """
    normalized = normalize_website_url(website_url)
    if not normalized:
        return {
            "email": None,
            "source": None,
            "confidence": 0.0,
            "all_found": [],
            "checked_urls": [],
            "errors": ["missing_website"],
            "final_url": "",
            "status_code": 0,
        }

    session = requests.Session()
    all_candidates: Dict[str, Dict[str, object]] = {}
    checked_urls: List[str] = []
    errors: List[str] = []
    visited: Set[str] = set()
    site_domain = _strip_www(urlparse(normalized).netloc)
    final_url = normalized
    final_status = 0

    # 1) Homepage with scheme fallback + SSL fallback
    homepage_result = None
    for candidate in _candidate_urls(normalized):
        if candidate in visited:
            continue
        visited.add(candidate)
        checked_urls.append(candidate)

        fetch = _fetch_page(session, candidate, request_timeout, verify=True)
        if not fetch.ok and fetch.error.startswith("ssl_error"):
            insecure = _fetch_page(session, candidate, request_timeout, verify=False)
            if insecure.ok:
                fetch = insecure

        if fetch.ok:
            homepage_result = fetch
            break

        errors.append(f"{candidate}:{fetch.error}")

    if homepage_result is None:
        return {
            "email": None,
            "source": None,
            "confidence": 0.0,
            "all_found": [],
            "checked_urls": checked_urls,
            "errors": errors or ["website_unreachable"],
            "final_url": normalized,
            "status_code": 0,
        }

    final_url = homepage_result.final_url
    final_status = homepage_result.status_code
    homepage_url = homepage_result.final_url

    homepage_candidates = _extract_candidates_from_html(homepage_result.html, "homepage", homepage_url, site_domain)
    all_candidates.update(homepage_candidates)

    # 2) Contact/about/impressum links from homepage + common known paths
    candidate_pages = []
    candidate_pages.extend(_collect_contact_links(homepage_result.html, homepage_url, site_domain))

    parsed_home = urlparse(homepage_url)
    base_url = f"{parsed_home.scheme}://{parsed_home.netloc}"
    for path in CONTACT_PATHS:
        candidate_pages.append(urljoin(base_url, path))

    # Deduplicate while preserving order
    deduped_pages = []
    seen = set()
    for url in candidate_pages:
        p = urlparse(url)
        canonical = urlunparse((p.scheme, p.netloc, p.path or "", "", "", ""))
        if canonical not in seen and _strip_www(p.netloc) == site_domain:
            seen.add(canonical)
            deduped_pages.append(canonical)

    pages_checked = 0
    for page_url in deduped_pages:
        if page_url in visited:
            continue
        if pages_checked >= max_contact_pages or len(visited) >= max_pages:
            break

        visited.add(page_url)
        checked_urls.append(page_url)
        pages_checked += 1

        fetch = _fetch_page(session, page_url, request_timeout, verify=True)
        if not fetch.ok and fetch.error.startswith("ssl_error"):
            insecure = _fetch_page(session, page_url, request_timeout, verify=False)
            if insecure.ok:
                fetch = insecure

        if not fetch.ok:
            errors.append(f"{page_url}:{fetch.error}")
            continue

        page_key = "contact_page" if any(k in page_url.lower() for k in CONTACT_HINTS) else "extra_page"
        page_candidates = _extract_candidates_from_html(fetch.html, page_key, fetch.final_url, site_domain)
        _add_candidates(all_candidates, page_candidates.keys(), page_key, site_domain)

        # Keep richer per-source data from page extraction
        for email, data in page_candidates.items():
            existing = all_candidates.get(email)
            if existing is None or float(data.get("confidence", 0)) > float(existing.get("confidence", 0)):
                all_candidates[email] = data

    if not all_candidates:
        return {
            "email": None,
            "source": None,
            "confidence": 0.0,
            "all_found": [],
            "checked_urls": checked_urls,
            "errors": errors,
            "final_url": final_url,
            "status_code": final_status,
        }

    ranked = sorted(
        all_candidates.values(),
        key=lambda item: (float(item.get("confidence", 0)), item.get("email", "") in COMMON_PREFIXES),
        reverse=True,
    )
    best = ranked[0]

    return {
        "email": best.get("email"),
        "source": best.get("source"),
        "confidence": float(best.get("confidence", 0)),
        "all_found": ranked,
        "checked_urls": checked_urls,
        "errors": errors,
        "final_url": final_url,
        "status_code": final_status,
    }


def guess_email(business_name: str, website_url: str) -> List[str]:
    """
    Generate likely email addresses from domain name.
    Returns list of guessed emails to try manually.
    """
    normalized = normalize_website_url(website_url)
    if not normalized:
        return []

    domain = _strip_www(urlparse(normalized).netloc)
    if not domain:
        return []

    guesses = [f"{prefix}@{domain}" for prefix in ("info", "contact", "hello", "office", "sales")]
    return guesses


def find_email(
    business_name: str,
    website_url: str,
    *,
    request_timeout: Tuple[float, float] = (4.0, 7.0),
    max_pages: int = 8,
    max_contact_pages: int = 6,
) -> Dict[str, object]:
    """
    Main entry for the pipeline.
    """
    result: Dict[str, object] = {
        "email": None,
        "source": None,
        "confidence": 0.0,
        "guesses": [],
        "all_found": [],
        "checked_urls": [],
        "errors": [],
        "final_url": "",
        "status_code": 0,
    }

    if not website_url:
        result["errors"] = ["missing_website"]
        return result

    extracted = extract_email_from_website(
        website_url,
        request_timeout=request_timeout,
        max_pages=max_pages,
        max_contact_pages=max_contact_pages,
    )

    result.update(extracted)
    if extracted.get("email"):
        return result

    # Keep guessed options only for manual review (never auto-send to guesses).
    result["guesses"] = guess_email(business_name, website_url)
    return result
