import unittest

from email_extractor import (
    normalize_website_url,
    validate_email,
    _extract_candidates_from_html,
    _extract_obfuscated_emails,
    find_email,
)


class EmailExtractorTests(unittest.TestCase):
    def test_normalize_website_url_adds_https(self):
        self.assertEqual(normalize_website_url("acme-atelier.pl"), "https://acme-atelier.pl")

    def test_normalize_website_url_keeps_path(self):
        self.assertEqual(
            normalize_website_url("http://example-business.fr/contact"),
            "http://example-business.fr/contact",
        )

    def test_validate_email_filters_asset_like_strings(self):
        self.assertFalse(validate_email("logo.png"))
        self.assertFalse(validate_email("noreply@acme-atelier.pl"))
        self.assertTrue(validate_email("kontakt@acme-atelier.pl"))

    def test_extract_candidates_from_html_finds_mailto_and_regex(self):
        html = """
        <html>
          <body>
            Reach us at kontakt@acme-atelier.pl
            <a href="mailto:sales@acme-atelier.pl?subject=hello">Email sales</a>
          </body>
        </html>
        """
        candidates = _extract_candidates_from_html(
            html,
            source_prefix="homepage",
            page_url="https://acme-atelier.pl",
            site_domain="acme-atelier.pl",
        )
        self.assertIn("kontakt@acme-atelier.pl", candidates)
        self.assertIn("sales@acme-atelier.pl", candidates)

    def test_extract_obfuscated_emails(self):
        text = "For offers: handlowy [at] acme-atelier [dot] pl"
        emails = _extract_obfuscated_emails(text)
        self.assertIn("handlowy@acme-atelier.pl", emails)

    def test_find_email_missing_website(self):
        result = find_email("Acme", "")
        self.assertIsNone(result["email"])
        self.assertIn("missing_website", result.get("errors", []))


if __name__ == "__main__":
    unittest.main()
