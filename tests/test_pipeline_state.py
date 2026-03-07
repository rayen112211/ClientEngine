import unittest

from pipeline_state import (
    STATUS_NO_EMAILS,
    STATUS_NO_WEBSITE,
    STATUS_PARTIAL,
    STATUS_READY,
    STATUS_TIMEOUT,
    determine_search_status,
    summarize_businesses,
)


class PipelineStateTests(unittest.TestCase):
    def test_summarize_businesses_counts(self):
        businesses = [
            {"website": "https://a.com", "email": "a@a.com", "qualified": True, "lead_status": "processed"},
            {"website": "", "email": "", "qualified": False, "lead_status": "no_website"},
            {"website": "https://b.com", "email": "", "qualified": False, "lead_status": "failed"},
        ]
        metrics = summarize_businesses(businesses)
        self.assertEqual(metrics["found"], 3)
        self.assertEqual(metrics["with_website"], 2)
        self.assertEqual(metrics["with_email"], 1)
        self.assertEqual(metrics["qualified"], 1)
        self.assertEqual(metrics["failed_leads"], 1)
        self.assertEqual(metrics["skipped_leads"], 1)

    def test_determine_status_ready(self):
        status = determine_search_status(
            found=10,
            with_website=8,
            with_email=5,
            qualified=3,
            processed=10,
            total=10,
            timed_out=False,
            had_errors=False,
        )
        self.assertEqual(status, STATUS_READY)

    def test_determine_status_no_website(self):
        status = determine_search_status(
            found=6,
            with_website=0,
            with_email=0,
            qualified=0,
            processed=6,
            total=6,
            timed_out=False,
            had_errors=False,
        )
        self.assertEqual(status, STATUS_NO_WEBSITE)

    def test_determine_status_no_emails(self):
        status = determine_search_status(
            found=6,
            with_website=4,
            with_email=0,
            qualified=0,
            processed=6,
            total=6,
            timed_out=False,
            had_errors=False,
        )
        self.assertEqual(status, STATUS_NO_EMAILS)

    def test_determine_status_timeout_vs_partial(self):
        timeout_status = determine_search_status(
            found=6,
            with_website=4,
            with_email=1,
            qualified=1,
            processed=0,
            total=6,
            timed_out=True,
            had_errors=True,
        )
        partial_status = determine_search_status(
            found=6,
            with_website=4,
            with_email=1,
            qualified=1,
            processed=3,
            total=6,
            timed_out=True,
            had_errors=True,
        )
        self.assertEqual(timeout_status, STATUS_TIMEOUT)
        self.assertEqual(partial_status, STATUS_PARTIAL)


if __name__ == "__main__":
    unittest.main()
