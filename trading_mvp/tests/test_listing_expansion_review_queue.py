"""Offline proposal policy tests; no collector, writer claim, or networking."""
from __future__ import annotations

import copy
import importlib.util
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
if importlib.util.find_spec('listing_expansion_review_queue'):
    import listing_expansion_review_queue as review
else:
    review = None

NOW = 1_800_000_000
PLAN = 'a' * 64
LEDGER = 'b' * 64


def entry(i, *, venue='bitget', asset='unclassified', declared=False,
          status='PENDING', first_seen=NOW - 100, anchor=NOW - 100,
          timestamp_source='openTime_ms_deprecated', due=0):
    event_id = f'{i:064x}'
    return dict(event_id=event_id, status=status, first_seen_ts=first_seen,
                last_seen_ts=NOW, admitted_seq=1, last_attempt_seq=0,
                attempt_count=int(status == 'RETRY'), next_attempt_after_ts=due,
                revisions=[], discovery_evidence={'path': 'C:/source.json', 'sha256': 'c'*64},
                job=dict(exchange=venue, symbol=f'T{i}USDT', base=f'T{i}',
                         asset_class=asset,
                         asset_class_source=('declared_spot_asset_registry_v1'
                                             if declared else 'unclassified'),
                         asset_class_acceptance_eligible=asset == 'crypto_token',
                         proxy_ts=anchor, window_end_ts=anchor + 72*3600,
                         timestamp_source=timestamp_source))


def proof(e, *, asset='crypto_token', received=NOW - 1):
    venue = e['job']['exchange']
    return dict(status='VERIFIED', evidence_class='OFFICIAL_VENUE_IDENTITY',
                exchange=venue, symbol=e['job']['symbol'], base=e['job']['base'],
                asset_class=asset, source_url=f'https://www.{venue}.com/support/item',
                source_path='C:/evidence/raw-response.json', source_sha256='d'*64,
                received_ts=received, source_storage='raw_http_response')


def official(e, *, t0=NOW + 100, received=NOW - 1):
    p = proof(e, received=received)
    p.pop('asset_class')
    p.update(evidence_class='OFFICIAL_ANNOUNCEMENT', official_spot_t0=t0)
    return p


class ReviewQueueTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(review, 'offline queue proposal policy is missing')

    def build(self, rows, overlay=None, **kwargs):
        return review.build_review_queues(
            {e['event_id']: e for e in rows}, overlay or {}, as_of_ts=NOW,
            source_plan_hash=PLAN, source_ledger_sha256=LEDGER, **kwargs)

    def all_rows(self, report):
        return [row for rows in report['queues'].values() for row in rows]

    def test_three_lanes_retain_every_event_exactly_once(self):
        rows = [entry(1), entry(2, asset='crypto_token', declared=True),
                entry(3, asset='tokenized_equity', declared=True),
                entry(4, asset='preipo_equity', declared=True)]
        report = self.build(rows)
        self.assertEqual({'identity_review': 1, 'crypto_descriptive': 1,
                          'equity_descriptive': 2}, report['counts'])
        self.assertCountEqual([e['event_id'] for e in rows],
                              [e['event_id'] for e in self.all_rows(report)])

    def test_source_lineage_is_retained_and_inputs_are_not_mutated(self):
        e = entry(1)
        evidence = {e['event_id']: proof(e)}
        before = copy.deepcopy((e, evidence))
        row = self.build([e], evidence)['queues']['crypto_descriptive'][0]
        self.assertEqual(e, row['source_entry'])
        self.assertEqual(PLAN, row['source_plan_hash'])
        self.assertEqual(LEDGER, row['source_ledger_sha256'])
        self.assertFalse(row['acceptance_eligible'])
        row['source_entry']['job']['base'] = 'MUTATION'
        row['identity_evidence']['base'] = 'MUTATION'
        self.assertEqual(before, (e, evidence))

    def test_unknown_ticker_is_not_identity_evidence(self):
        e = entry(1)
        e['job'].update(symbol='BTCUSDT', base='BTC')
        row = self.build([e])['queues']['identity_review'][0]
        self.assertEqual('unclassified', row['asset_class'])
        self.assertEqual('UNKNOWN_IDENTITY', row['identity_status'])

    def test_undeclared_legacy_class_is_not_trusted(self):
        row = self.build([entry(1, asset='crypto_token')])['queues']['identity_review'][0]
        self.assertEqual('unclassified', row['asset_class'])

    def test_declared_legacy_class_is_marked_legacy_not_verified(self):
        row = self.build([entry(1, asset='crypto_token', declared=True)])['queues']['crypto_descriptive'][0]
        self.assertEqual('LEGACY_DECLARED_REGISTRY', row['identity_status'])
        self.assertFalse(row['identity_verified'])

    def test_verified_overlay_classifies_without_promoting_metadata_time(self):
        e = entry(1)
        row = self.build([e], {e['event_id']: proof(e)})['queues']['crypto_descriptive'][0]
        self.assertTrue(row['identity_verified'])
        self.assertEqual('VERIFIED_OFFICIAL_IDENTITY', row['identity_status'])
        self.assertIsNone(row['official_spot_t0'])
        self.assertNotEqual('OFFICIAL_EXACT_UPCOMING_OR_ACTIVE', row['priority_class'])
        self.assertEqual('METADATA', row['timestamp_class'])

    def test_missing_or_malformed_provenance_fails_closed(self):
        e = entry(1)
        for key in ('status', 'evidence_class', 'source_path', 'source_url', 'source_storage',
                    'source_sha256', 'received_ts', 'exchange', 'symbol', 'base'):
            with self.subTest(key=key):
                p = proof(e)
                p.pop(key)
                self.assertEqual(1, self.build([e], {e['event_id']: p})['counts']['identity_review'])
        for key, value in [('status', 'UNVERIFIED'), ('source_path', 'relative.json'),
                           ('source_sha256', 'not-a-hash'),
                           ('source_storage', 'pretend_raw'),
                           ('source_url', 'https://bitget.com.evil.example/item'),
                           ('source_url', 'https://user:secret@www.bitget.com/item'),
                           ('symbol', 'OTHERUSDT'), ('base', 'OTHER'),
                           ('exchange', 'bybit'), ('asset_class', 'maybe_crypto')]:
            with self.subTest(key=key, value=value):
                p = proof(e)
                p[key] = value
                self.assertEqual(1, self.build([e], {e['event_id']: p})['counts']['identity_review'])

    def test_future_received_evidence_cannot_change_causal_classification(self):
        e = entry(1)
        p = proof(e, received=NOW + 1)
        row = self.build([e], {e['event_id']: p})['queues']['identity_review'][0]
        self.assertIn('identity_evidence_received_after_as_of', row['diagnostics'])

    def test_conflicting_verified_identity_is_reviewed_not_overwritten(self):
        e = entry(1, asset='crypto_token', declared=True)
        row = self.build([e], {e['event_id']: proof(e, asset='tokenized_equity')})['queues']['identity_review'][0]
        self.assertEqual('CONFLICTING_IDENTITY', row['identity_status'])
        self.assertEqual('crypto_token', row['source_entry']['job']['asset_class'])

    def test_official_t0_needs_independent_bound_announcement_proof(self):
        e = entry(1, timestamp_source='snapshot_diff_detection_time_proxy')
        p = proof(e)
        p['official_spot_t0'] = NOW + 100
        row = self.build([e], {e['event_id']: p})['queues']['crypto_descriptive'][0]
        self.assertIsNone(row['official_spot_t0'])
        p['official_spot_event'] = official(e)
        row = self.build([e], {e['event_id']: p})['queues']['crypto_descriptive'][0]
        self.assertEqual(NOW + 100, row['official_spot_t0'])
        self.assertEqual('OFFICIAL_EXACT_UPCOMING_OR_ACTIVE', row['priority_class'])
        self.assertFalse(row['acceptance_eligible'])

    def test_invalid_or_future_official_evidence_cannot_raise_priority(self):
        e = entry(1, timestamp_source='snapshot_diff_detection_time_proxy')
        p = proof(e)
        for bad in [dict(official(e), evidence_class='OFFICIAL_METADATA'),
                    official(e, received=NOW + 1),
                    dict(official(e), symbol='OTHER'),
                    dict(official(e), official_spot_t0=0)]:
            with self.subTest(bad=bad):
                p['official_spot_event'] = bad
                row = self.build([e], {e['event_id']: p})['queues']['crypto_descriptive'][0]
                self.assertIsNone(row['official_spot_t0'])
                self.assertNotEqual('OFFICIAL_EXACT_UPCOMING_OR_ACTIVE', row['priority_class'])

    def test_expired_official_t0_is_retained_but_not_urgent(self):
        e = entry(1)
        p = dict(proof(e), official_spot_event=official(e, t0=NOW - 61))
        row = self.build([e], {e['event_id']: p})['queues']['crypto_descriptive'][0]
        self.assertEqual(NOW - 61, row['official_spot_t0'])
        self.assertNotEqual('OFFICIAL_EXACT_UPCOMING_OR_ACTIVE', row['priority_class'])

    def test_reviewed_web_identity_is_retained_but_not_selectable_or_armed(self):
        e = entry(1)
        p = dict(proof(e), source_storage='reviewed_web_extract')
        p['official_spot_event'] = official(e)
        report = self.build([e], {e['event_id']: p})
        row = report['queues']['crypto_descriptive'][0]
        self.assertEqual('crypto_token', row['asset_class'])
        self.assertEqual('REVIEWED_WEB_IDENTITY', row['identity_status'])
        self.assertFalse(row['identity_verified'])
        self.assertEqual(p, row['identity_evidence'])
        self.assertIn('RAW_SOURCE_ARCHIVE_MISSING', row['diagnostics'])
        self.assertFalse(row['selectable'])
        self.assertNotEqual('OFFICIAL_EXACT_UPCOMING_OR_ACTIVE', row['priority_class'])
        self.assertEqual([], review.select_proposals(report, lane='crypto_descriptive'))

    def test_raw_identity_and_web_only_t0_stays_unselectable_and_not_urgent(self):
        e = entry(1)
        p = dict(proof(e), official_spot_event=dict(official(e),
                                                  source_storage='reviewed_web_extract'))
        row = self.build([e], {e['event_id']: p})['queues']['crypto_descriptive'][0]
        self.assertTrue(row['identity_verified'])
        self.assertEqual(NOW + 100, row['official_spot_t0'])
        self.assertFalse(row['selectable'])
        self.assertIn('RAW_SOURCE_ARCHIVE_MISSING', row['diagnostics'])
        self.assertNotEqual('OFFICIAL_EXACT_UPCOMING_OR_ACTIVE', row['priority_class'])

    def test_recent_metadata_precedes_detection_proxy_with_identical_age(self):
        rows = [entry(1, timestamp_source='snapshot_diff_detection_time_proxy'), entry(2)]
        proposed = review.select_proposals(self.build(rows), lane='identity_review',
                                           limit=2, aging_slots_per_venue=0)
        self.assertEqual([rows[1]['event_id'], rows[0]['event_id']],
                         [e['event_id'] for e in proposed])

    def test_terminal_quarantined_and_not_due_rows_are_retained_not_selected(self):
        rows = [entry(1, status='COLLECTED_DESCRIPTIVE'), entry(2, status='QUARANTINED'),
                entry(3, status='RETRY', due=NOW + 1)]
        report = self.build(rows)
        self.assertEqual(3, sum(report['counts'].values()))
        self.assertTrue(all(not row['selectable'] for row in self.all_rows(report)))
        self.assertEqual([], review.select_proposals(report, lane='identity_review'))

    def test_retry_budget_is_reserved_and_capped(self):
        rows = [entry(i, status='PENDING') for i in range(100)]
        rows += [entry(i, status='RETRY') for i in range(100, 150)]
        selected = review.select_proposals(self.build(rows), lane='identity_review')
        self.assertEqual(50, len(selected))
        self.assertEqual(10, sum(e['queue_status'] == 'RETRY' for e in selected))
        only_retries = review.select_proposals(self.build(rows[100:]), lane='identity_review')
        self.assertEqual(10, len(only_retries))

    def test_four_venues_receive_round_robin_slots(self):
        rows = [entry(i) for i in range(100)]
        rows += [entry(100 + i, venue=v) for i, v in enumerate(['binance', 'bybit', 'okx'])]
        result = review.select_proposals(self.build(rows), lane='identity_review', limit=4)
        self.assertCountEqual(['binance', 'bitget', 'bybit', 'okx'], [e['exchange'] for e in result])

    def test_oldest_slot_prevents_priority_starvation(self):
        old = entry(1, first_seen=NOW-1000000, anchor=NOW-1000000)
        new = entry(2, first_seen=NOW-1)
        report = self.build([new, old])
        result = review.select_proposals(report, lane='identity_review', limit=1)
        self.assertEqual(old['event_id'], result[0]['event_id'])

    def test_venue_cursor_rotates_small_batches(self):
        rows = [entry(i, venue=v) for i, v in enumerate(['binance', 'bitget', 'bybit', 'okx'])]
        result = review.select_proposals(self.build(rows), lane='identity_review', limit=1,
                                         venue_cursor='bybit')
        self.assertEqual('bybit', result[0]['exchange'])

    def test_selection_respects_lane_and_explicit_limit(self):
        rows = [entry(1), entry(2, asset='crypto_token', declared=True)]
        report = self.build(rows)
        self.assertEqual([rows[1]['event_id']], [e['event_id'] for e in
                         review.select_proposals(report, lane='crypto_descriptive', limit=1)])
        for limit in (0, -1, 51, True):
            with self.assertRaises(ValueError):
                review.select_proposals(report, lane='identity_review', limit=limit)
        with self.assertRaises(ValueError):
            review.select_proposals(report, lane='all')

    def test_permutation_invariant_and_output_detached(self):
        rows = [entry(i, venue=['bitget', 'okx'][i % 2]) for i in range(60)]
        report = self.build(rows)
        reordered = self.build(list(reversed(rows)))
        self.assertEqual(report, reordered)
        chosen = review.select_proposals(report, lane='identity_review')
        before = copy.deepcopy(report)
        chosen[0]['source_entry']['job']['base'] = 'MUTATED'
        self.assertEqual(before, report)

    def test_bad_lineage_duplicate_identity_or_unknown_overlay_is_rejected(self):
        e = entry(1)
        with self.assertRaises(ValueError):
            review.build_review_queues({e['event_id']: e}, {}, as_of_ts=NOW,
                                       source_plan_hash='bad', source_ledger_sha256=LEDGER)
        duplicate = copy.deepcopy(e)
        duplicate['event_id'] = f'{2:064x}'
        with self.assertRaises(ValueError):
            self.build([e, duplicate])
        with self.assertRaises(ValueError):
            self.build([e], {f'{99:064x}': proof(e)})


if __name__ == '__main__':
    unittest.main()
