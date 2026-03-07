"""
Email Engine — handles SMTP sending, sequence orchestration,
spam checking, bounce handling, and unsubscribe management.
"""
import smtplib
import ssl
import json
import random
import re
import time
import imaplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from config import SEQUENCE_DELAYS, SPAM_TRIGGER_WORDS, APP_BASE_URL
from database import (
    get_db,
    get_campaign,
    get_due_sequences,
    advance_sequence,
    log_email,
    get_settings,
    update_campaign,
    get_unsubscribe_token,
    is_lead_unsubscribed,
    is_lead_bounced,
    mark_bounced,
    already_sent,
)
from templates_data import get_template


# ═══════════════════════════════════════════════════════════
# PERSONALIZATION
# ═══════════════════════════════════════════════════════════

def personalize(text, lead, settings):
    """Replace all {{PLACEHOLDER}} variables and process {a|b} spintax."""
    replacements = {
        "{{BUSINESS_NAME}}": lead.get("business_name", ""),
        "{{CITY}}": lead.get("city", ""),
        "{{CATEGORY}}": lead.get("category", ""),
        "{{FROM_NAME}}": settings.get("from_name", "Rayen"),
        "{{PORTFOLIO_LINK}}": settings.get("portfolio_link", "https://rayenlazizi.tech"),
    }
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, str(value))
    
    # Process Spintax like {Hello|Hi|Hey}
    while True:
        match = re.search(r'\{([^{}]*)\}', text)
        if not match:
            break
        options = match.group(1).split('|')
        choice = random.choice(options)
        text = text[:match.start()] + choice + text[match.end():]
        
    return text


def add_unsubscribe_footer(body, lead_id):
    """Add standard footer and unsubscribe link."""
    token = get_unsubscribe_token(lead_id)
    unsub_url = f"{APP_BASE_URL}/unsubscribe/{token}"
    
    if "[UNSUBSCRIBE_LINK]" in body:
        return body.replace("[UNSUBSCRIBE_LINK]", unsub_url)
        
    footer = (
        f"\n\nBest,\n"
        f"Rayen Lazizi\n"
        f"Web Designer\n"
        f"https://rayenlazizi.tech\n\n"
        f"---\n"
        f"To unsubscribe: {unsub_url}"
    )
    return body + footer


# ═══════════════════════════════════════════════════════════
# SPAM CHECK
# ═══════════════════════════════════════════════════════════

def check_spam_score(subject, body):
    """
    Check email content for spam indicators.
    Returns dict with score (0-100), warnings list, and is_safe bool.
    """
    warnings = []
    score = 0

    combined = (subject + " " + body).lower()

    # Check for spam trigger words
    found_words = []
    for word in SPAM_TRIGGER_WORDS:
        if word.lower() in combined:
            found_words.append(word)
            score += 5

    if found_words:
        warnings.append(f"Contains spam trigger words: {', '.join(found_words[:5])}")

    # Check for excessive capitals in subject
    if subject and len(subject) > 5:
        caps_ratio = sum(1 for c in subject if c.isupper()) / len(subject)
        if caps_ratio > 0.5:
            score += 20
            warnings.append("Subject has too many CAPITALS (over 50%)")

    # Check for excessive exclamation/question marks
    excl_count = subject.count("!") + subject.count("?")
    if excl_count > 2:
        score += 10
        warnings.append(f"Subject has {excl_count} exclamation/question marks")

    # Check body length
    if len(body) < 50:
        score += 10
        warnings.append("Body is very short (under 50 chars)")
    elif len(body) > 2000:
        score += 5
        warnings.append("Body is very long (over 2000 chars)")

    # Check for URLs in body (too many = spam)
    url_count = len(re.findall(r"https?://", body))
    if url_count > 3:
        score += 10
        warnings.append(f"Body contains {url_count} URLs (keep under 3)")

    is_safe = score < 30

    return {
        "score": min(score, 100),
        "warnings": warnings,
        "is_safe": is_safe,
        "verdict": "✅ Looks clean" if is_safe else "⚠️ May trigger spam filters",
    }


# ═══════════════════════════════════════════════════════════
# SENDING
# ═══════════════════════════════════════════════════════════

def send_email(to_email, subject, body, settings):
    """
    Send a single email via SMTP.
    Returns (success: bool, error_msg: str or None, bounce: bool, rate_limited: bool).
    The bounce flag indicates if the error is a permanent bounce (bad email).
    The rate_limited flag indicates the provider is throttling us (should pause & retry).
    """
    smtp_host = settings.get("smtp_host", "smtp.gmail.com")
    smtp_port = int(settings.get("smtp_port", 465))
    smtp_user = settings.get("smtp_user", "")
    smtp_password = settings.get("smtp_password", "")
    use_ssl = settings.get("smtp_use_ssl", "true").lower() == "true"
    from_name = settings.get("from_name", "Rayen")
    from_email = settings.get("from_email", smtp_user)
    reply_to = settings.get("reply_to", from_email)

    if not smtp_user or not smtp_password:
        return False, "SMTP credentials not configured", False, False

    if not to_email:
        return False, "No recipient email", True, False  # Treat missing email as bounce

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{from_name} <{from_email}>"
        msg["To"]      = to_email
        msg["Reply-To"] = reply_to  # Ensures replies go to hello@rayenlazizi.tech

        # Plain text for maximum deliverability
        msg.attach(MIMEText(body, "plain", "utf-8"))

        if use_ssl:
            # SSL mode: port 465
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context, timeout=20) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        else:
            # STARTTLS mode: port 587 (SpaceMail default)
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
                
        # Automatically append to the Sent folder via IMAP
        try:
            imap_host = smtp_host.replace("smtp", "imap")
            if "spacemail" in smtp_host:
                imap_host = "mail.spacemail.com" # SpaceMail uses the same host for IMAP
            
            with imaplib.IMAP4_SSL(imap_host, 993) as imap:
                imap.login(smtp_user, smtp_password)
                # Ensure the string is formatted exactly as standard RFC822
                message_str = msg.as_string().encode('utf-8')
                
                # Check for standard sent folder names
                for sent_folder in ['"Sent Items"', 'Sent', '"Sent Messages"', '"Sent"']:
                    status, _ = imap.select(sent_folder)
                    if status == 'OK':
                        imap.append(sent_folder, '\\Seen', imaplib.Time2Internaldate(time.time()), message_str)
                        break
        except Exception as imap_err:
            print(f"Warning: Could not save to Sent folder via IMAP: {imap_err}")

        return True, None, False, False

    except smtplib.SMTPAuthenticationError:
        return False, "SMTP authentication failed — check username/password", False, False
    except smtplib.SMTPRecipientsRefused as e:
        return False, f"Recipient refused (bounced): {to_email}", True, False
    except smtplib.SMTPDataError as e:
        error_msg = str(e)
        # Rate-limit errors (554 5.7.1 "too many messages") are TEMPORARY — NOT a bounce
        # Do not mark the recipient as bounced for rate limits
        is_rate_limit = any(kw in error_msg.lower() for kw in [
            "too many messages", "rate", "policy", "rejected", "spam"
        ])
        # Genuine hard bounces: user doesn't exist or domain doesn't accept mail
        is_hard_bounce = (
            ("550" in error_msg or "553" in error_msg) and not is_rate_limit
        )
        if is_hard_bounce:
            return False, f"Bounced (hard): {error_msg}", True, False
        if is_rate_limit:
            return False, f"Rate limit hit: {error_msg}", False, True
        # Everything else = soft failure
        return False, f"Soft fail: {error_msg}", False, False

    except smtplib.SMTPSenderRefused:
        return False, "Sender address rejected — check From Email", False, False
    except Exception as e:
        return False, str(e), False, False



# ═══════════════════════════════════════════════════════════
# PREVIEW
# ═══════════════════════════════════════════════════════════

def preview_campaign_emails(campaign_id):
    """
    Generate a preview of all emails that would be sent for a campaign.
    Returns list of preview dicts (without actually sending).
    """
    campaign = get_campaign(campaign_id)
    if not campaign:
        return {"error": "Campaign not found", "previews": []}

    settings = get_settings()
    due_sequences = get_due_sequences(campaign_id)

    previews = []
    spam_warnings = []

    for seq in due_sequences:
        lead_id = seq["lead_id"]
        current_step = seq["current_step"]
        next_step = current_step + 1

        if next_step > 5:
            continue

        # Check unsubscribed / bounced
        if is_lead_unsubscribed(lead_id):
            previews.append({
                "business_name": seq["business_name"],
                "email": seq.get("email", ""),
                "step": next_step,
                "status": "skip",
                "reason": "Unsubscribed",
            })
            continue

        if is_lead_bounced(lead_id):
            previews.append({
                "business_name": seq["business_name"],
                "email": seq.get("email", ""),
                "step": next_step,
                "status": "skip",
                "reason": "Bounced",
            })
            continue

        business_type = seq.get("business_type", "other") or "other"
        template = get_template(business_type, next_step)
        if not template:
            previews.append({
                "business_name": seq["business_name"],
                "email": seq.get("email", ""),
                "step": next_step,
                "status": "skip",
                "reason": f"No template for {business_type} step {next_step}",
            })
            continue

        subject = random.choice(template["subject_variants"])
        lead_data = dict(seq)
        subject = personalize(subject, lead_data, settings)
        body = personalize(template["body"], lead_data, settings)
        body = add_unsubscribe_footer(body, lead_id)

        # Spam check
        spam = check_spam_score(subject, body)
        if not spam["is_safe"]:
            spam_warnings.extend(spam["warnings"])

        previews.append({
            "business_name": seq["business_name"],
            "email": seq.get("email", ""),
            "step": next_step,
            "business_type": business_type,
            "subject": subject,
            "body": body,
            "tier": seq.get("tier"),
            "score": seq.get("qualification_score"),
            "city": seq.get("city"),
            "pain_points": seq.get("pain_points", "[]"),
            "spam_score": spam["score"],
            "spam_safe": spam["is_safe"],
            "spam_warnings": spam["warnings"],
            "status": "ready",
        })

    return {
        "campaign": campaign["name"],
        "total_ready": sum(1 for p in previews if p["status"] == "ready"),
        "total_skip": sum(1 for p in previews if p["status"] == "skip"),
        "spam_warnings": list(set(spam_warnings)),
        "previews": previews,
    }


# ═══════════════════════════════════════════════════════════
# CAMPAIGN PROCESSING
# ═══════════════════════════════════════════════════════════

def process_campaign(campaign_id):
    """
    Process a campaign: find all leads due for their next email and send them.
    Skips unsubscribed and bounced leads. Adds unsubscribe link.
    Returns a summary dict.
    """
    campaign = get_campaign(campaign_id)
    if not campaign:
        return {"error": "Campaign not found"}

    if campaign["status"] not in ("active",):
        return {"error": f"Campaign is {campaign['status']}, not active"}

    settings = get_settings()
    due_sequences = get_due_sequences(campaign_id)

    summary = {
        "campaign": campaign["name"],
        "total_due": len(due_sequences),
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "bounced": 0,
        "details": [],
    }

    delay_min = campaign.get("delay_min_seconds", 60)
    delay_max = campaign.get("delay_max_seconds", 180)

    for seq in due_sequences:
        lead_id = seq["lead_id"]
        current_step = seq["current_step"]
        next_step = current_step + 1

        if next_step > 5:
            summary["skipped"] += 1
            continue

        # ---- Check unsubscribed ----
        if is_lead_unsubscribed(lead_id):
            summary["skipped"] += 1
            summary["details"].append({
                "business": seq["business_name"],
                "status": "skipped",
                "reason": "Unsubscribed",
            })
            continue

        # ---- Check bounced ----
        if is_lead_bounced(lead_id):
            summary["skipped"] += 1
            summary["details"].append({
                "business": seq["business_name"],
                "status": "skipped",
                "reason": "Previously bounced",
            })
            continue

        # ---- Check already sent this sequence step ----
        if already_sent(lead_id, next_step):
            summary["skipped"] += 1
            summary["details"].append({
                "business": seq["business_name"],
                "status": "skipped",
                "reason": f"Step {next_step} already sent",
            })
            
            # Auto-advance to next step since it's already sent
            advance_sequence(seq["id"], 1)
            continue

        business_type = seq.get("business_type", "other")
        if not business_type:
            business_type = "other"

        # Get template
        template = get_template(business_type, next_step)
        if not template:
            summary["skipped"] += 1
            summary["details"].append({
                "business": seq["business_name"],
                "status": "skipped",
                "reason": f"No template for {business_type} step {next_step}",
            })
            continue

        # Select random subject
        subjects = template["subject_variants"]
        subject = random.choice(subjects)

        # Personalize
        lead_data = dict(seq)
        subject = personalize(subject, lead_data, settings)
        body = personalize(template["body"], lead_data, settings)

        # Add unsubscribe footer
        body = add_unsubscribe_footer(body, lead_id)

        # Send
        success, error, is_bounce, _is_rate_limited = send_email(seq["email"], subject, body, settings)

        status = "sent" if success else "failed"

        # ---- Handle bounce ----
        if is_bounce:
            mark_bounced(lead_id)
            summary["bounced"] += 1
            status = "bounced"

        # Log the email
        log_email({
            "lead_id": lead_id,
            "campaign_id": campaign_id,
            "sequence_step": next_step,
            "business_type": business_type,
            "subject": subject,
            "body": body,
            "status": status,
            "error_message": error,
            "tier": seq.get("tier"),
            "qualification_score": seq.get("qualification_score"),
            "city": seq.get("city"),
            "country": seq.get("country"),
        })

        # Advance sequence
        if success:
            next_delay = SEQUENCE_DELAYS[next_step] if next_step < len(SEQUENCE_DELAYS) else 7
            advance_sequence(seq["id"], next_delay)
            summary["sent"] += 1
        else:
            summary["failed"] += 1

        summary["details"].append({
            "business": seq["business_name"],
            "email": seq["email"],
            "step": next_step,
            "subject": subject,
            "status": status,
            "error": error,
        })

        # Random delay between sends (anti-spam)
        if success:
            delay = random.randint(delay_min, delay_max)
            time.sleep(delay)

    return summary


# ═══════════════════════════════════════════════════════════
# TEST EMAIL
# ═══════════════════════════════════════════════════════════

def send_test_email(to_email, settings=None):
    """Send a test email to verify SMTP config."""
    if not settings:
        settings = get_settings()

    subject = "Test Email — ClientEngine"
    body = """This is a test email from ClientEngine.

If you received this, your SMTP configuration is working correctly!

Settings used:
- SMTP Host: {host}
- From: {name} <{email}>

System is ready to send campaigns.
""".format(
        host=settings.get("smtp_host", "N/A"),
        name=settings.get("from_name", "N/A"),
        email=settings.get("from_email", "N/A"),
    )

    success, error, _is_bounce, _is_rate_limited = send_email(to_email, subject, body, settings)
    return success, error

