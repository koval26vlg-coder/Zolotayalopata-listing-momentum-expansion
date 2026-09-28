"""Append-only, generation-bound discovery queue. Caller holds the global writer claim.

COLLECTED_DESCRIPTIVE is acquisition deduplication, never acceptance. No legacy
state is imported. A damaged tail or changed evidence requires explicit recovery.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path


def _bytes(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _require(ok, reason):
    if not ok:
        raise ValueError('expansion backlog: ' + reason)


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('expansion backlog: invalid numeric constant')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def _evidence_bytes(evidence, label):
    _require(isinstance(evidence, dict), label + ' missing or malformed')
    path_text = evidence.get('path')
    digest = evidence.get('sha256')
    _require(isinstance(path_text, str) and Path(path_text).is_absolute(),
             label + ' path must be absolute')
    _require(isinstance(digest, str) and len(digest) == 64
             and all(c in '0123456789abcdef' for c in digest),
             label + ' checksum malformed')
    try:
        raw = Path(path_text).read_bytes()
    except OSError as exc:
        raise ValueError('expansion backlog: ' + label + ' unavailable') from exc
    _require(_sha(raw) == digest, label + ' changed')
    return raw


class Backlog:
    def __init__(self, path: Path, plan_hash: str):
        self.path = path
        self.plan_hash = plan_hash
        self.entries = {}
        self.ticks = set()
        self.outcome_ticks = set()
        self.seq = 0
        self.head = None
        self.raw = path.read_bytes() if path.exists() else b''
        _require(not self.raw or self.raw.endswith(b'\n'), 'incomplete ledger tail')
        verified = set()
        for line in self.raw.splitlines():
            record = _decode(line)
            expected = record.pop('record_hash', None)
            _require(expected == _sha(_bytes(record)), 'record checksum mismatch')
            _require(record.get('schema') == 'expansion_backlog_event_v1', 'schema mismatch')
            _require(record.get('plan_hash') == plan_hash, 'plan binding mismatch')
            _require(record.get('seq') == self.seq+1 and record.get('previous_hash') == self.head,
                     'broken ledger sequence')
            evidence = record.get('evidence')
            if record['kind'] in ('DISCOVERY', 'OUTCOMES'):
                _require(isinstance(evidence, dict), 'source evidence missing')
                evidence_key = (evidence.get('path'), evidence.get('sha256'))
                if evidence_key not in verified or record['kind'] == 'OUTCOMES':
                    source_raw = _evidence_bytes(evidence, 'source evidence')
                    verified.add(evidence_key)
                    if record['kind'] == 'OUTCOMES':
                        receipt = _decode(source_raw)
                        _require(receipt.get('plan_hash') == self.plan_hash
                                 and receipt.get('tick_id') == record['payload']['tick_id'],
                                 'outcome evidence binding mismatch')
                        _evidence_bytes(receipt.get('raw_evidence'), 'raw evidence')
            self._apply(record)
            self.seq = record['seq']
            self.head = expected

    def _append(self, kind, payload, evidence=None):
        record = dict(schema='expansion_backlog_event_v1', plan_hash=self.plan_hash,
                      seq=self.seq+1, previous_hash=self.head, kind=kind,
                      payload=payload, evidence=evidence)
        # Validate the whole transition against detached state before creating or
        # appending any bytes. A later rejected row cannot poison the durable chain.
        preview = copy.copy(self)
        preview.entries = copy.deepcopy(self.entries)
        preview.ticks = set(self.ticks)
        preview.outcome_ticks = set(self.outcome_ticks)
        preview._apply(record)
        record_hash = _sha(_bytes(record))
        encoded = _bytes(dict(record, record_hash=record_hash))+b'\n'
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('a+b') as handle:
            handle.seek(0)
            _require(handle.read() == self.raw, 'ledger changed since load')
            handle.seek(0, os.SEEK_END)
            # No truncate/retry after ambiguous append. Preserve a torn tail.
            _require(handle.write(encoded) == len(encoded), 'ledger short write')
            handle.flush()
            os.fsync(handle.fileno())
        self.entries = preview.entries
        self.ticks = preview.ticks
        self.outcome_ticks = preview.outcome_ticks
        self.raw += encoded
        self.seq += 1
        self.head = record_hash

    def _apply(self, record):
        p = record['payload']
        if record['kind'] == 'DISCOVERY':
            _require(p['tick_id'] not in self.ticks, 'duplicate discovery tick')
            self.ticks.add(p['tick_id'])
            for job in p['jobs']:
                # One instrument per immutable baseline generation. A changed
                # base/source/metadata anchor is quarantined, not silently rebased.
                event_id = _sha(_bytes([self.plan_hash, job['exchange'], job['symbol']]))
                if event_id not in self.entries:
                    self.entries[event_id] = dict(job=job, event_id=event_id,
                        first_seen_ts=p['now_ts'], last_seen_ts=p['now_ts'],
                        admitted_seq=record['seq'], last_attempt_seq=0,
                        attempt_count=0, next_attempt_after_ts=0, status='PENDING',
                        revisions=[], discovery_evidence=record['evidence'])
                else:
                    entry = self.entries[event_id]
                    old = entry['job']
                    conflict = any(old.get(k) != job.get(k) for k in
                        ('base', 'timestamp_source', 'asset_class', 'asset_class_source',
                         'asset_class_acceptance_eligible'))
                    if not str(old['timestamp_source']).endswith('proxy'):
                        conflict = conflict or old['proxy_ts'] != job['proxy_ts']
                    if conflict:
                        entry['status'] = 'QUARANTINED'
                        revision = dict(job=job, evidence=record['evidence'])
                        entry['revisions'].append(revision)
                    entry['last_seen_ts'] = p['now_ts']
        elif record['kind'] == 'ATTEMPT':
            _require(p['tick_id'] in self.ticks, 'attempt lacks discovery tick')
            _require(p['tick_id'] not in self.outcome_ticks, 'attempt after terminal outcome tick')
            entry = self.entries[p['event_id']]
            _require(entry['status'] not in ('QUARANTINED','COLLECTED_DESCRIPTIVE'),
                     'attempt on terminal/quarantined record')
            entry.update(status='RETRY', last_attempt_seq=record['seq'],
                         last_attempt_tick=p['tick_id'], next_attempt_after_ts=p['now_ts']+300,
                         attempt_count=entry['attempt_count']+1)
        elif record['kind'] == 'OUTCOMES':
            _require(p['tick_id'] in self.ticks, 'outcome lacks discovery tick')
            _require(p['tick_id'] not in self.outcome_ticks, 'duplicate outcome tick')
            self.outcome_ticks.add(p['tick_id'])
            seen = set()
            for outcome in p['outcomes']:
                _require(outcome['event_id'] not in seen, 'duplicate outcome event')
                seen.add(outcome['event_id'])
                entry = self.entries[outcome['event_id']]
                _require(entry.get('last_attempt_tick') == p['tick_id'], 'outcome attempt mismatch')
                _require(entry['status'] == 'RETRY', 'outcome on terminal/quarantined record')
                _require(type(outcome['complete']) is bool, 'outcome complete flag malformed')
                if outcome['complete']:
                    entry['status'] = 'COLLECTED_DESCRIPTIVE'
                entry['last_outcome'] = dict(outcome, evidence=record['evidence'])
        else:
            raise ValueError('expansion backlog: unknown event kind')

    def admit(self, snapshot_path: Path):
        raw = snapshot_path.read_bytes()
        snapshot = _decode(raw)
        _require(snapshot.get('plan_hash') == self.plan_hash, 'snapshot binding mismatch')
        _require(snapshot['tick_id'] not in self.ticks, 'duplicate discovery tick')
        _require(isinstance(snapshot['jobs'], list), 'snapshot jobs malformed')
        seen = set()
        for job in snapshot['jobs']:
            key = (job['exchange'], job['symbol'])
            _require(key not in seen, 'duplicate instrument in snapshot')
            seen.add(key)
            _require(type(job['proxy_ts']) is int and type(job['window_end_ts']) is int
                     and 0 < job['proxy_ts'] < job['window_end_ts'], 'invalid window')
        self._append('DISCOVERY', dict(tick_id=snapshot['tick_id'], now_ts=snapshot['now_ts'],
                     jobs=snapshot['jobs']), dict(path=str(snapshot_path.resolve()), sha256=_sha(raw)))

    def _job(self, entry, now_ts):
        job = copy.deepcopy(entry['job'])
        job.update(event_id=entry['event_id'], first_seen_ts=entry['first_seen_ts'],
                   queue_status=entry['status'], category=('new_listing_window_complete'
                   if now_ts >= job['window_end_ts'] else 'new_listing_in_progress'))
        return job

    def select(self, now_ts: int, limit: int):
        _require(type(limit) is int and 0 < limit <= 50, 'job cap must be 1..50')
        entries = [e for e in self.entries.values() if e['status'] in ('PENDING','RETRY')
                   and e['next_attempt_after_ts'] <= now_ts]
        entries.sort(key=lambda e:(e['last_attempt_seq'], e['admitted_seq'], e['event_id']))
        return [self._job(e, now_ts) for e in entries[:limit]]

    def pending(self, now_ts: int):
        return [dict(self._job(e, now_ts), failure_reason=('identity_or_timestamp_revision'
                     if e['status']=='QUARANTINED' else 'queued_next_interval'),
                     retry_disposition='RETRY_NEXT_INTERVAL')
                for e in self.entries.values() if e['status'] != 'COLLECTED_DESCRIPTIVE']

    def start(self, event_id: str, tick_id: str, now_ts: int):
        entry = self.entries[event_id]
        _require(entry['next_attempt_after_ts'] <= now_ts, 'attempt not due')
        self._append('ATTEMPT', dict(event_id=event_id, tick_id=tick_id, now_ts=now_ts))

    def finish(self, manifest_path: Path, now_ts: int):
        """Bind an immutable acquisition receipt, not a rewritable tick manifest.

        The caller has already fsynced/closed raw data and durably recorded its
        terminal manifest, state and attempt ledger before invoking this barrier.
        """
        raw = manifest_path.read_bytes()
        manifest = _decode(raw)
        _require(manifest['plan_hash'] == self.plan_hash, 'manifest binding mismatch')
        _require(manifest['tick_id'] not in self.outcome_ticks, 'duplicate outcome tick')
        _evidence_bytes(manifest.get('raw_evidence'), 'raw evidence')
        outcomes = []
        for row in manifest['jobs']:
            entry = self.entries[row['event_id']]
            _require(entry.get('last_attempt_tick') == manifest['tick_id'], 'outcome attempt mismatch')
            outcomes.append(dict(event_id=row['event_id'], complete=(
                row.get('coverage_complete') is True and now_ts >= entry['job']['window_end_ts'])))
        self._append('OUTCOMES', dict(tick_id=manifest['tick_id'], outcomes=outcomes),
                     dict(path=str(manifest_path.resolve()), sha256=_sha(raw)))
