import unittest

from enrichment import is_good_business, is_good_email


class QualificationTests(unittest.TestCase):
    def test_is_good_email(self):
        self.assertTrue(is_good_email("kontakt@acme-atelier.pl"))
        self.assertFalse(is_good_email("noreply@acme-atelier.pl"))
        self.assertFalse(is_good_email("logo.png"))

    def test_is_good_business_requires_email(self):
        ok, reason = is_good_business({"business_name": "A", "email": ""})
        self.assertFalse(ok)
        self.assertEqual(reason, "No email")

    def test_is_good_business_fame_filter(self):
        ok, reason = is_good_business(
            {
                "business_name": "A",
                "email": "kontakt@acme-atelier.pl",
                "review_count": 5000,
            }
        )
        self.assertFalse(ok)
        self.assertIn("Too famous", reason)

    def test_is_good_business_passes_valid_lead(self):
        ok, reason = is_good_business(
            {
                "business_name": "A",
                "email": "kontakt@acme-atelier.pl",
                "review_count": 120,
                "website": "https://acme-atelier.pl",
            }
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "OK")


if __name__ == "__main__":
    unittest.main()
