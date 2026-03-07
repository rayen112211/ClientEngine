"""
Email Extractor Module.
Extracts email addresses from business websites.
"""
import re
import requests
import smtplib
from urllib.parse import urlparse
import dns.resolver


# Common contact page paths to check
CONTACT_PATHS = [
    "/contatti", "/contatto", "/contact", "/contacts",
    "/contact-us", "/about", "/chi-siamo", "/about-us",
    "/info", "/informazioni", "/impressum",
]

# Common email patterns to try when no email found on website
COMMON_PREFIXES = [
    "info", "contact", "contatti", "prenotazioni",
    "booking", "reception", "hello", "mail",
]

# Regex for finding emails in text
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Emails to exclude (generic/system emails)
EXCLUDED_DOMAINS = {
    "example.com", "example.org", "example.net", "domain.com",
    "sentry.io", "wixpress.com", "sentry-next.wixpress.com",
    "wordpress.com", "wpmail.com", "w3.org", "schema.org", 
    "googleapis.com", "google.com", "facebook.com", "instagram.com", 
    "twitter.com", "tiktok.com", "squarespace.com", "shopify.com", 
    "weebly.com", "mailchimp.com", "amazonaws.com", "cloudflare.com",
    "localhost", "test.com", "invalid.com", "email.com", "mybusiness.com"
}

EXCLUDED_PREFIXES = {
    "noreply", "no-reply", "mailer-daemon", "postmaster", "webmaster",
    "abuse", "hostmaster", "root", "help", "tickets", "donotreply",
    "do-not-reply", "bounces", "bounce", "daemon", "admin", "user",
    "test", "demo", "sample", "example", "dummy"
}


def validate_email(email):
    """Check if an email looks real and usable by testing syntax, roles, and MX records."""
    if not email:
        return False
        
    email = email.lower().strip()

    # Basic format check
    if not re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email):
        return False

    prefix = email.split("@")[0]
    domain = email.split("@")[-1]

    # Check excluded domains
    if domain in EXCLUDED_DOMAINS:
        return False

    # Check excluded prefixes (role-based)
    if prefix in EXCLUDED_PREFIXES:
        return False

    # Check for image/file extensions in email (false positives)
    if any(email.endswith(ext) for ext in [".png", ".jpg", ".gif", ".svg", ".css", ".js"]):
        return False

    # Check MX records
    mx_record = None
    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = 2.0
        resolver.lifetime = 2.0
        answers = resolver.resolve(domain, 'MX')
        mx_record = sorted(answers, key=lambda x: x.preference)[0].exchange.to_text()
    except Exception:
        # SMTP standard allows fallback to A records if no MX exists
        try:
            resolver.resolve(domain, 'A')
            mx_record = domain
        except Exception:
            return False

    # Perform deep SMTP mailbox verification to prevent hard bounces
    if mx_record:
        try:
            server = smtplib.SMTP(timeout=3.0)
            server.connect(mx_record, 25)
            server.helo('spacemail.com')  # valid-looking EHLO
            server.mail('hello@rayenlazizi.tech')
            code, message = server.rcpt(email)
            server.quit()
            
            # 550 means the mailbox definitely does not exist (e.g. Google Workspace)
            if code == 550:
                return False
        except Exception:
            # If the SMTP connection times out, graylists us, or fails for any reason,
            # we gracefully ignore it to avoid discarding potentially valid emails.
            pass

    return True


def _fetch_page(url, timeout=4):
    """Fetch a webpage and return its text content."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return ""


def _extract_emails_from_html(html):
    """Find all email addresses in HTML text."""
    if not html:
        return []

    emails = EMAIL_REGEX.findall(html)

    from bs4 import BeautifulSoup
    import urllib.parse
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.lower().startswith('mailto:'):
                email_part = href[7:]
                email_part = email_part.split('?')[0].strip()
                email_part = urllib.parse.unquote(email_part)
                if email_part:
                    emails.append(email_part)
    except Exception:
        pass

    valid = [e.lower().strip() for e in emails if validate_email(e)]

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for e in valid:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    return unique


def extract_email_from_website(website_url):
    """
    Try to extract a contact email from a business website.
    Checks homepage and common contact pages.
    Returns: {"email": "found@email.com", "source": "homepage"} or {"email": None, "source": None}
    """
    if not website_url:
        return {"email": None, "source": None}

    # Ensure URL has protocol
    url = website_url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    all_emails = []

    # Step 1: Check homepage
    html = _fetch_page(url)
    if html:
        emails = _extract_emails_from_html(html)
        if emails:
            all_emails.extend([(e, "homepage") for e in emails])

    # Step 2: Check contact pages
    if not all_emails:
        for path in CONTACT_PATHS:
            contact_url = base_url + path
            html = _fetch_page(contact_url, timeout=5)
            if html:
                emails = _extract_emails_from_html(html)
                if emails:
                    all_emails.extend([(e, f"page:{path}") for e in emails])
                    break  # Found on a contact page, stop

    # Return the best email found
    if all_emails:
        # Prefer emails with common business prefixes
        for email, source in all_emails:
            prefix = email.split("@")[0]
            if prefix in COMMON_PREFIXES:
                return {"email": email, "source": source}
        # Otherwise return the first one
        return {"email": all_emails[0][0], "source": all_emails[0][1]}

    return {"email": None, "source": None}


def guess_email(business_name, website_url):
    """
    Generate likely email addresses from domain name.
    Returns list of guessed emails to try.
    """
    if not website_url:
        return []

    url = website_url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc
    if domain.startswith("www."):
        domain = domain[4:]

    if not domain:
        return []

    guesses = [f"{prefix}@{domain}" for prefix in COMMON_PREFIXES[:5]]
    return guesses


def find_email(business_name, website_url):
    """
    Main entry: try to find an email for a business.
    1. Extract from website
    2. If not found, generate guesses
    Returns dict with email, source, and guesses.
    """
    result = {
        "email": None,
        "source": None,
        "guesses": [],
        "all_found": [],
    }

    # Try extracting from website (real emails only — no guessing)
    if website_url:
        extracted = extract_email_from_website(website_url)
        if extracted["email"]:
            result["email"] = extracted["email"]
            result["source"] = f"website ({extracted['source']})"
            return result

        # Generate potential guesses for reference — but DO NOT send to them.
        # Guessed emails (info@, contact@) are not verified to exist and cause bounces.
        guesses = guess_email(business_name, website_url)
        result["guesses"] = guesses
        # result["email"] intentionally left as None — no guessing fallback

    return result
