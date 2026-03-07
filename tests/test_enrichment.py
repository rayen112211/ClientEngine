import unittest
from unittest.mock import patch

import requests

from enrichment import _url_variants, check_website, is_good_business, is_good_email


class DummyResponse:
    def __init__(self, status_code=200, url='https://example.com', text='<html><meta name="viewport"></html>', headers=None):
        self.status_code = status_code
        self.url = url
        self.text = text
        self.headers = headers or {}


class EnrichmentTests(unittest.TestCase):
    def test_url_variants_adds_scheme_alternatives(self):
        variants = _url_variants('acme-shop.pl')
        self.assertIn('https://acme-shop.pl', variants)
        self.assertIn('http://acme-shop.pl', variants)

    def test_check_website_missing_url(self):
        result = check_website('')
        self.assertEqual(result.get('status'), 'none')
        self.assertEqual(result.get('fetch_error'), 'missing_website')

    def test_check_website_ssl_fallback(self):
        calls = []

        def fake_get(url, timeout=None, allow_redirects=True, headers=None, verify=True):
            calls.append({'url': url, 'verify': verify})
            if verify:
                raise requests.exceptions.SSLError('cert')
            return DummyResponse(status_code=200, url='https://acme-shop.pl', text='<html><meta name="viewport"><form>contact</form></html>')

        with patch('enrichment.requests.get', side_effect=fake_get):
            result = check_website('acme-shop.pl')

        self.assertEqual(result.get('status'), 'ok')
        self.assertEqual(result.get('status_code'), 200)
        self.assertTrue(any(call['verify'] is False for call in calls))

    def test_is_good_business_and_email_filters(self):
        ok, reason = is_good_business({'business_name': 'A', 'email': 'info@acme.pl', 'review_count': 30})
        self.assertTrue(ok)
        self.assertEqual(reason, 'OK')

        too_big, reason_big = is_good_business({'business_name': 'B', 'email': 'info@acme.pl', 'review_count': 9999})
        self.assertFalse(too_big)
        self.assertIn('Too famous', reason_big)

        self.assertFalse(is_good_email('noreply@acme.pl'))
        self.assertFalse(is_good_email('logo.png'))
        self.assertTrue(is_good_email('contact@acme.pl'))


if __name__ == '__main__':
    unittest.main()
