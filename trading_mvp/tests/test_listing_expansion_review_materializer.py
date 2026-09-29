import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import listing_expansion_review_materializer as module


class MaterializerTests(unittest.TestCase):
    def setUp(self):
        self.entries = {'a'*64: {'job': {'exchange': 'binance', 'symbol': 'ABCUSDT', 'base': 'ABC'}}}
        self.review = {'rows': [{'venue': 'binance', 'symbol': 'ABCUSDT', 'base': 'ABC',
            'reviewed_asset_class': 'crypto', 'official_spot_t0': '2026-09-24T11:00:00Z',
            'source_url': 'https://www.binance.com/en/support/announcement/detail/example',
            'review_status': 'VERIFIED_OFFICIAL_WEB_SOURCE_NOT_RUNTIME_BOUND'}]}
        self.source = {'source_path': str(Path(__file__).resolve()), 'source_sha256': 'b'*64}

    def test_proxy_review_keeps_explicit_weak_storage(self):
        overlay = module.reviewed_proxy_overlay(self.entries, self.review, self.source, 1790673000)
        evidence = overlay.get('a'*64, {})
        self.assertEqual(evidence.get('asset_class'), 'crypto_token')
        self.assertEqual(evidence.get('source_storage'), 'reviewed_web_extract')
        self.assertEqual(evidence.get('received_ts'), 1790673000)
        self.assertEqual(evidence.get('official_spot_event', {}).get('source_storage'), 'reviewed_web_extract')

    def test_duplicate_or_unmatched_review_fails_closed(self):
        self.review['rows'].append(copy.deepcopy(self.review['rows'][0]))
        with self.assertRaises(ValueError):
            module.reviewed_proxy_overlay(self.entries, self.review, self.source, 1790673000)

    def test_source_job_timestamp_not_overwritten(self):
        self.entries['a'*64]['job']['proxy_ts'] = 123
        old = copy.deepcopy(self.entries)
        module.reviewed_proxy_overlay(self.entries, self.review, self.source, 1790673000)
        self.assertEqual(self.entries, old)

    def test_unverified_review_not_promoted(self):
        self.review['rows'][0]['review_status'] = 'GUESS'
        with self.assertRaises(ValueError):
            module.reviewed_proxy_overlay(self.entries, self.review, self.source, 1790673000)

    def test_selected_export_is_never_runtime_authorization(self):
        bundle = module.selection_summary({'identity_review': [], 'crypto_descriptive': [], 'equity_descriptive': []})
        self.assertIs(bundle.get('collector_allowed'), False)
        self.assertIs(bundle.get('selection_not_execution'), True)


if __name__ == '__main__':
    unittest.main()
