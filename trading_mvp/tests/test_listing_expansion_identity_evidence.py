import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))


class IdentityEvidenceTests(unittest.TestCase):
    def setUp(self):
        import listing_expansion_identity_evidence as module
        self.module = module
        self.job = {'exchange': 'bitget', 'symbol': 'ABCUSDT', 'base': 'ABC'}
        self.raw = {'code': '00000', 'data': [{'symbol': 'ABCUSDT', 'baseCoin': 'ABC',
            'quoteCoin': 'USDT', 'category': 'SPOT', 'symbolType': 'crypto',
            'isReality': 'no', 'launchTime': '1700000000000'}]}
        self.source = {'source_url': 'https://api.bitget.com/api/v3/market/instruments?category=SPOT',
            'source_path': str(Path(__file__).resolve()), 'source_sha256': 'a'*64, 'received_ts': 1800000000}

    def build(self, raw=None, source=None):
        return self.module.bitget_identity_overlay({'id1': {'job': self.job}},
            self.raw if raw is None else raw, self.source if source is None else source)

    def test_api_exists(self):
        self.assertTrue(callable(getattr(self.module, 'bitget_identity_overlay', None)))

    def test_positive_identity_retains_metadata_not_official_t0(self):
        overlay = self.build()
        self.assertEqual(overlay.get('id1', {}).get('asset_class'), 'crypto_token')
        self.assertEqual(overlay.get('id1', {}).get('received_ts'), 1800000000)
        self.assertEqual(overlay.get('id1', {}).get('metadata_launch_ts'), 1700000000)
        self.assertNotIn('official_spot_event', overlay['id1'])

    def test_stock_requires_positive_stock_metadata_not_prefix(self):
        self.raw['data'][0].update(symbolType='stock', isReality='yes')
        self.assertEqual(self.build().get('id1', {}).get('asset_class'), 'tokenized_equity')

    def test_no_is_reality_alone_never_implies_crypto(self):
        del self.raw['data'][0]['symbolType']
        self.assertEqual(self.build(), {})

    def test_conflicting_fields_and_duplicate_symbol_fail_closed(self):
        self.raw['data'][0]['isReality'] = 'yes'
        self.assertEqual(self.build(), {})
        self.raw['data'][0]['isReality'] = 'no'
        self.raw['data'].append(copy.deepcopy(self.raw['data'][0]))
        self.assertEqual(self.build(), {})

    def test_exact_market_identity_required(self):
        for field, value in [('baseCoin', 'OTHER'), ('quoteCoin', 'BTC'), ('category', 'USDT-FUTURES')]:
            raw = copy.deepcopy(self.raw)
            raw['data'][0][field] = value
            self.assertEqual(self.build(raw), {})

    def test_response_error_not_silent_empty_success(self):
        self.raw['code'] = '40000'
        with self.assertRaises(ValueError):
            self.build()

    def test_exact_allowlist_and_source_hash_required(self):
        for field, value in [('source_url', 'https://evil.invalid/api/v3/market/instruments?category=SPOT'),
                             ('source_sha256', 'bad'), ('received_ts', 0), ('source_path', 'relative.json')]:
            source = dict(self.source, **{field: value})
            with self.assertRaises(ValueError):
                self.build(source=source)

    def test_no_mutation(self):
        before = copy.deepcopy((self.job, self.raw, self.source))
        self.build()
        self.assertEqual(before, (self.job, self.raw, self.source))

    def test_storage_grade_is_raw_not_an_article_excerpt_hash(self):
        self.assertEqual(self.build()['id1'].get('source_storage'), 'raw_http_response')


if __name__ == '__main__':
    unittest.main()
