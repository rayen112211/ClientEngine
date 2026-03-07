"""
Pipeline state and metrics helpers shared by backend routes and workers.
"""
from __future__ import annotations

from typing import Dict, Iterable

STATUS_PENDING = "pending"
STATUS_DISCOVERING = "discovering"
STATUS_EXTRACTING = "extracting"
STATUS_SCORING = "scoring"
STATUS_READY = "ready"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "failed"
STATUS_DONE = "done"
STATUS_NO_EMAILS = "no_emails"
STATUS_NO_WEBSITE = "no_website"
STATUS_TIMEOUT = "timeout"
STATUS_ERROR = "error"
STATUS_SENDING = "sending"
STATUS_PAUSED = "paused"
STATUS_STOPPED = "stopped"

SEARCH_ACTIVE_STATUSES = {
    STATUS_PENDING,
    STATUS_DISCOVERING,
    STATUS_EXTRACTING,
    STATUS_SCORING,
}

SEARCH_TERMINAL_STATUSES = {
    STATUS_READY,
    STATUS_PARTIAL,
    STATUS_FAILED,
    STATUS_DONE,
    STATUS_NO_EMAILS,
    STATUS_NO_WEBSITE,
    STATUS_TIMEOUT,
    STATUS_ERROR,
}


def normalize_status(status: str | None) -> str:
    """Normalize status values for safer comparisons."""
    if not status:
        return ""
    return str(status).strip().lower()


def is_search_terminal(status: str | None) -> bool:
    return normalize_status(status) in SEARCH_TERMINAL_STATUSES


def summarize_businesses(businesses: Iterable[dict]) -> Dict[str, int]:
    """
    Build dashboard-safe counters from pipeline result rows.
    """
    items = list(businesses or [])
    found = len(items)
    with_website = 0
    with_email = 0
    qualified = 0
    failed_leads = 0
    skipped_leads = 0
    no_website = 0

    for biz in items:
        website = (biz.get("website") or "").strip()
        email = (biz.get("email") or "").strip()
        qualified_flag = bool(biz.get("qualified"))
        lead_status = normalize_status(biz.get("lead_status"))

        if website:
            with_website += 1
        else:
            no_website += 1
        if email:
            with_email += 1
        if qualified_flag:
            qualified += 1

        if lead_status in {"failed", "error", "timeout"}:
            failed_leads += 1
        elif not qualified_flag:
            skipped_leads += 1

    return {
        "found": found,
        "with_website": with_website,
        "with_email": with_email,
        "qualified": qualified,
        "failed_leads": failed_leads,
        "skipped_leads": skipped_leads,
        "no_website": no_website,
    }


def determine_search_status(
    *,
    found: int,
    with_website: int,
    with_email: int,
    qualified: int,
    processed: int,
    total: int,
    timed_out: bool,
    had_errors: bool,
) -> str:
    """
    Decide final search status from aggregate counters.
    """
    if total <= 0 or found <= 0:
        return STATUS_NO_WEBSITE if with_website <= 0 else STATUS_NO_EMAILS

    if timed_out and processed <= 0:
        return STATUS_TIMEOUT

    if with_website <= 0:
        return STATUS_NO_WEBSITE

    if with_email <= 0:
        if timed_out or had_errors:
            return STATUS_PARTIAL
        return STATUS_NO_EMAILS

    if timed_out or had_errors or processed < total:
        return STATUS_PARTIAL

    if qualified < 0:
        return STATUS_FAILED

    return STATUS_READY
