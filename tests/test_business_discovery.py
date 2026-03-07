import unittest
from unittest.mock import patch

import business_discovery as bd


class BusinessDiscoveryTests(unittest.TestCase):
    def test_search_google_places_requires_api_key(self):
        with patch('business_discovery.config.GOOGLE_PLACES_API_KEY', ''):
            res = bd.search_google_places('restaurant', location='florence', max_results=5, api_key='')
        self.assertTrue(res.get('error'))
        self.assertEqual(res.get('results'), [])

    def test_search_google_places_paginates_after_invalid_request_warmup(self):
        first_page = {
            'status': 'OK',
            'results': [
                {
                    'name': 'Alpha',
                    'formatted_address': 'Via Roma 1, Florence, Italy',
                    'rating': 4.6,
                    'user_ratings_total': 53,
                    'place_id': 'p1',
                    'types': ['restaurant'],
                }
            ],
            'next_page_token': 'TOKEN123',
        }
        invalid = {'status': 'INVALID_REQUEST', 'results': []}
        second_page = {
            'status': 'OK',
            'results': [
                {
                    'name': 'Beta',
                    'formatted_address': 'Via Verdi 8, Florence, Italy',
                    'rating': 4.2,
                    'user_ratings_total': 31,
                    'place_id': 'p2',
                    'types': ['restaurant'],
                }
            ],
        }

        sequence = [first_page, invalid, second_page]

        def fake_request_json(url, *, params, timeout, retries=2):
            return sequence.pop(0)

        with patch('business_discovery._request_json', side_effect=fake_request_json), patch('business_discovery.time.sleep', return_value=None):
            res = bd.search_google_places('restaurant', location='florence', max_results=10, api_key='k')

        self.assertIsNone(res.get('error'))
        self.assertEqual(res.get('total'), 2)
        self.assertEqual(len(res.get('results', [])), 2)

    def test_search_businesses_dedup_merges_fields(self):
        gm_rows = {
            'error': None,
            'results': [
                {
                    'business_name': 'Acme Bistro',
                    'city': 'florence',
                    'source': 'google_maps',
                    'website': '',
                    'phone': '',
                    'email': '',
                },
                {
                    'business_name': 'Acme Bistro',
                    'city': 'florence',
                    'source': 'google_maps',
                    'website': 'https://acme.example',
                    'phone': '+39000',
                    'email': 'info@acme.example',
                },
            ],
        }

        with patch('business_discovery.search_google_places', return_value=gm_rows), patch('business_discovery.enrich_with_details', side_effect=lambda rows, **_: rows):
            res = bd.search_businesses('restaurant', 'florence', source_choice='google_maps', max_results=20)

        self.assertIsNone(res.get('error'))
        self.assertEqual(res.get('total'), 1)
        row = res['results'][0]
        self.assertEqual(row.get('website'), 'https://acme.example')
        self.assertEqual(row.get('phone'), '+39000')
        self.assertEqual(row.get('email'), 'info@acme.example')


if __name__ == '__main__':
    unittest.main()
