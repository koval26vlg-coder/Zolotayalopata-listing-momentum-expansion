from __future__ import annotations

import hashlib
import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
if importlib.util.find_spec('listing_expansion_backlog'):
    import listing_expansion_backlog as backlog
else:
    backlog = None

PLAN = 'a' * 64
NOW = 1_800_000_000


def job(i=0, *, source='detection_time_proxy', ts=NOW):
    return dict(exchange='binance', symbol=f'T{i:04}USDT', base=f'T{i:04}',
                proxy_ts=ts, timestamp_source=source, probe_start_ts=ts-3600,
                window_end_ts=ts+72*3600, category='new_listing_in_progress',
                asset_class='unknown', asset_class_source='unclassified',
                asset_class_acceptance_eligible=False)


class BacklogTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(backlog, 'durable discovery backlog implementation is missing')
        self.temp = tempfile.TemporaryDirectory(prefix='expansion-backlog-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / 'events.jsonl'
        self.q = backlog.Backlog(self.path, PLAN)

    def admit(self, jobs, tick='t1', now=NOW):
        p = self.root / f'{tick}.discovery.json'
        p.write_text(json.dumps({'tick_id': tick, 'plan_hash': PLAN, 'now_ts': now,
                                'jobs': jobs, 'current_rows': [], 'skipped': []}), encoding='utf-8')
        self.q.admit(p)

    def outcome_receipt(self, rows, tick='t1', *, raw_evidence=True):
        raw_path = self.root / f'{tick}.ohlcv.jsonl'
        raw_path.write_bytes(b'{"fixture": "durable raw bytes"}\n')
        receipt = {'tick_id': tick, 'plan_hash': PLAN, 'jobs': rows}
        if raw_evidence:
            receipt['raw_evidence'] = {
                'path': str(raw_path.resolve()),
                'sha256': hashlib.sha256(raw_path.read_bytes()).hexdigest(),
            }
        path = self.root / f'{tick}.acquisition-outcomes.json'
        path.write_text(json.dumps(receipt), encoding='utf-8')
        return path, raw_path

    def start_one(self, *, source='detection_time_proxy'):
        self.admit([job(source=source)])
        event_id = self.q.select(NOW, 50)[0]['event_id']
        self.q.start(event_id, 't1', NOW)
        return event_id

    def test_persist_all_before_cap_and_fair_crash_retry(self):
        self.admit([job(i) for i in range(151)])
        self.assertEqual(151, len(backlog.Backlog(self.path, PLAN).entries))
        first = self.q.select(NOW, 50)
        self.assertEqual(50, len(first))
        for item in first:
            self.q.start(item['event_id'], 't1', NOW)
        restarted = backlog.Backlog(self.path, PLAN)
        second = restarted.select(NOW+600, 50)
        self.assertTrue({j['event_id'] for j in first}.isdisjoint(j['event_id'] for j in second))
        self.assertEqual(151, len(restarted.pending(NOW+600)))

    def test_proxy_anchor_is_first_observed_even_after_missing_snapshot(self):
        self.admit([job()])
        self.admit([], 't2', NOW+3600)
        self.admit([job(ts=NOW+86400)], 't3', NOW+86400)
        self.assertEqual(1, len(self.q.entries))
        saved = self.q.select(NOW+86400, 50)[0]
        self.assertEqual(NOW, saved['proxy_ts'])
        self.assertEqual(NOW, saved['first_seen_ts'])

    def test_metadata_revision_quarantines_without_losing_original(self):
        self.admit([job(source='openTime')])
        self.admit([job(source='openTime', ts=NOW+3600)], 't2', NOW+100)
        self.assertEqual([], self.q.select(NOW+600, 50))
        entry = next(iter(self.q.entries.values()))
        self.assertEqual('QUARANTINED', entry['status'])
        self.assertEqual(NOW, entry['job']['proxy_ts'])
        self.assertEqual(1, len(entry['revisions']))
        self.assertEqual(1, len(self.q.pending(NOW+600)))

    def test_corrupt_tail_and_wrong_binding_fail_closed(self):
        self.admit([job()])
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'binding'):
            backlog.Backlog(self.path, 'b'*64)
        with self.path.open('ab') as f:
            f.write(b'{')
        with self.assertRaisesRegex(ValueError, 'tail'):
            backlog.Backlog(self.path, PLAN)
        self.assertEqual(before+b'{', self.path.read_bytes())

    def test_changed_snapshot_rejected_by_lineage(self):
        self.admit([job()])
        (self.root/'t1.discovery.json').write_text('{}', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'evidence'):
            backlog.Backlog(self.path, PLAN)

    def test_stale_second_writer_cannot_append(self):
        stale = backlog.Backlog(self.path, PLAN)
        self.admit([job()])
        with self.assertRaisesRegex(ValueError, 'changed'):
            stale.admit(self.root/'t1.discovery.json')
        self.assertEqual(1, len(backlog.Backlog(self.path, PLAN).entries))

    def test_completed_tombstone_and_partial_outcome(self):
        self.admit([job(0), job(1)])
        selected = self.q.select(NOW, 50)
        for j in selected:
            self.q.start(j['event_id'], 't1', NOW)
        terminal, _ = self.outcome_receipt([
            {'event_id': selected[0]['event_id'], 'coverage_complete': True},
            {'event_id': selected[1]['event_id'], 'coverage_complete': False},
        ])
        self.q.finish(terminal, NOW+73*3600)
        self.admit([job(0,ts=NOW+4*86400),job(1,ts=NOW+4*86400)], 't2', NOW+4*86400)
        self.assertEqual(1, len(self.q.select(NOW+4*86400, 50)))
        self.assertEqual(2, len(self.q.entries))

    def test_duplicate_tick_refused(self):
        self.admit([job()])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            self.q.admit(self.root/'t1.discovery.json')

    def test_selection_cap_is_not_silently_relaxed(self):
        self.admit([job()])
        with self.assertRaisesRegex(ValueError, 'cap'):
            self.q.select(NOW, 51)

    def test_invalid_transition_does_not_append_or_mutate_memory(self):
        self.admit([job(source='openTime')])
        event_id = self.q.select(NOW, 50)[0]['event_id']
        self.admit([job(source='openTime', ts=NOW+3600)], 't2', NOW+100)
        before = self.path.read_bytes()
        entries = copy.deepcopy(self.q.entries)
        with self.assertRaisesRegex(ValueError, 'quarantined'):
            self.q.start(event_id, 't2', NOW+600)
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual(entries, self.q.entries)
        self.assertEqual(entries, backlog.Backlog(self.path, PLAN).entries)

    def test_attempt_requires_discovery_tick_before_append(self):
        self.admit([job()])
        event_id = self.q.select(NOW, 50)[0]['event_id']
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'discovery'):
            self.q.start(event_id, 'unknown-tick', NOW)
        self.assertEqual(before, self.path.read_bytes())

    def test_late_outcome_cannot_erase_quarantine(self):
        event_id = self.start_one(source='openTime')
        self.admit([job(source='openTime', ts=NOW+3600)], 't2', NOW+100)
        terminal, _ = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
        ])
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'quarantined'):
            self.q.finish(terminal, NOW+73*3600)
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual('QUARANTINED', self.q.entries[event_id]['status'])
        self.assertEqual(1, len(self.q.pending(NOW+73*3600)))

    def test_duplicate_outcome_event_ids_refused_without_append(self):
        event_id = self.start_one()
        terminal, _ = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
            {'event_id': event_id, 'coverage_complete': False},
        ])
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'duplicate outcome event'):
            self.q.finish(terminal, NOW+73*3600)
        self.assertEqual(before, self.path.read_bytes())

    def test_outcome_requires_raw_evidence(self):
        event_id = self.start_one()
        terminal, _ = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
        ], raw_evidence=False)
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'raw evidence'):
            self.q.finish(terminal, NOW+73*3600)
        self.assertEqual(before, self.path.read_bytes())

    def test_outcome_rejects_changed_raw_bytes_before_append(self):
        event_id = self.start_one()
        terminal, raw_path = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
        ])
        raw_path.write_bytes(b'changed')
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'raw evidence'):
            self.q.finish(terminal, NOW+73*3600)
        self.assertEqual(before, self.path.read_bytes())

    def test_reload_rejects_changed_raw_behind_completed_tombstone(self):
        event_id = self.start_one()
        terminal, raw_path = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
        ])
        self.q.finish(terminal, NOW+73*3600)
        self.assertEqual('COLLECTED_DESCRIPTIVE',
                         backlog.Backlog(self.path, PLAN).entries[event_id]['status'])
        raw_path.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'raw evidence'):
            backlog.Backlog(self.path, PLAN)

    def test_reload_rejects_missing_raw_behind_completed_tombstone(self):
        event_id = self.start_one()
        terminal, raw_path = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
        ])
        self.q.finish(terminal, NOW+73*3600)
        raw_path.unlink()
        with self.assertRaisesRegex(ValueError, 'raw evidence'):
            backlog.Backlog(self.path, PLAN)

    def test_raw_evidence_path_must_be_absolute(self):
        event_id = self.start_one()
        terminal, _ = self.outcome_receipt([
            {'event_id': event_id, 'coverage_complete': True},
        ])
        receipt = json.loads(terminal.read_text(encoding='utf-8'))
        receipt['raw_evidence']['path'] = 'ohlcv.jsonl'
        terminal.write_text(json.dumps(receipt), encoding='utf-8')
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'raw evidence'):
            self.q.finish(terminal, NOW+73*3600)
        self.assertEqual(before, self.path.read_bytes())


if __name__ == '__main__':
    unittest.main()
