"""Reviewed successor queue: immutable seed, isolated ledger, crypto-only jobs.

Construction and inspection are read-only. The caller must hold the global writer
claim before admit/start/finish. The monitor separately verifies the seed against
the complete bound review plan, source backlog and materialized bundle. A reviewed
identity permits descriptive acquisition only, never acceptance or execution.
"""
from __future__ import annotations

import copy
from pathlib import Path

from listing_expansion_backlog import Backlog, _bytes, _sha
from listing_expansion_review_contract import _read_stable, _strict_json
from listing_expansion_review_queue import build_review_queues, select_proposals, _proof_error


SCHEMA = 'expansion_reviewed_lineage_seed_v1'
POLICY = dict(retry_slots=10, max_proposals=50, aging_slots_per_venue=1)


def _require(ok, reason):
    if not ok:
        raise ValueError('expansion generation: ' + reason)


def _hash(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _binding(binding, label):
    _require(isinstance(binding, dict) and _hash(binding.get('sha256'))
             and isinstance(binding.get('path'), str)
             and Path(binding['path']).is_absolute(), label + ' binding malformed')


def validate_seed(seed_path, seed_sha256, source_plan_hash, expected_source_events=2202):
    """Read-only structural/hash/proof check; caller validates review lineage.

The seed is explicitly authorized by the successor plan hash. This check does
not grant authority to arbitrary objects carrying review-plan-looking metadata.
"""
    _require(_hash(seed_sha256) and _hash(source_plan_hash), 'seed/source hash malformed')
    _require(type(expected_source_events) is int and expected_source_events > 0,
             'source event count malformed')
    raw = _read_stable(Path(seed_path))
    _require(_sha(raw) == seed_sha256, 'seed hash mismatch')
    seed = _strict_json(raw)
    _require(isinstance(seed, dict) and seed.get('schema') == SCHEMA, 'seed schema mismatch')
    _require(seed.get('source_plan_hash') == source_plan_hash, 'source plan mismatch')
    _require(_hash(seed.get('source_ledger_sha256')), 'source ledger hash malformed')
    source_seq = seed.get('source_seq')
    _require(type(source_seq) is int and source_seq > 0, 'source sequence malformed')
    _binding(seed.get('review_plan'), 'review plan')
    _require(_hash(seed['review_plan'].get('plan_hash')), 'review plan hash malformed')
    entries, overlay = seed.get('entries'), seed.get('identity_overlay')
    _require(isinstance(entries, dict) and len(entries) == expected_source_events,
             'source event count mismatch')
    _require(isinstance(overlay, dict) and set(overlay) <= set(entries), 'unknown overlay identity')
    instruments = set()
    proof_files = {}
    for event_id, entry in entries.items():
        _require(isinstance(entry, dict) and entry.get('event_id') == event_id, 'source event ID mismatch')
        job = entry.get('job')
        _require(isinstance(job, dict) and job.get('exchange') in ('binance','bitget','bybit','okx')
                 and isinstance(job.get('symbol'), str) and bool(job['symbol'])
                 and isinstance(job.get('base'), str) and bool(job['base']), 'source instrument malformed')
        instrument = (job['exchange'], job['symbol'])
        _require(instrument not in instruments, 'duplicate source instrument')
        instruments.add(instrument)
        _require(event_id == _sha(_bytes([source_plan_hash, *instrument])), 'source event ID formula mismatch')
        _require(entry.get('status') in ('PENDING','RETRY','QUARANTINED','COLLECTED_DESCRIPTIVE'),
                 'source status malformed')
        for field in ('first_seen_ts','last_seen_ts','next_attempt_after_ts','attempt_count'):
            _require(type(entry.get(field)) is int and entry[field] >= 0, 'source ' + field + ' malformed')
        _require(entry['first_seen_ts'] > 0, 'source first_seen_ts malformed')
        for field in ('admitted_seq','last_attempt_seq'):
            _require(type(entry.get(field)) is int and 0 <= entry[field] <= source_seq,
                     'source sequence bound malformed')
        _require(isinstance(entry.get('revisions'), list), 'source revisions malformed')
        _require(type(job.get('proxy_ts')) is int and type(job.get('window_end_ts')) is int
                 and 0 < job['proxy_ts'] < job['window_end_ts'], 'source window malformed')
        proof = overlay.get(event_id)
        if proof is not None:
            _require(isinstance(proof, dict), 'identity proof malformed')
            proofs = [(proof, 'OFFICIAL_VENUE_IDENTITY', 'identity')]
            if proof.get('official_spot_event') is not None:
                proofs.append((proof['official_spot_event'], 'OFFICIAL_ANNOUNCEMENT', 'official t0'))
            for item, evidence_class, label in proofs:
                _require(isinstance(item,dict), label + ' proof malformed')
                received = item.get('received_ts')
                _require(type(received) is int and received > 0, label + ' source reception malformed')
                _require(_proof_error(item, job, received, evidence_class, label) is None,
                         label + ' source proof mismatch')
                key = (item['source_path'], item['source_sha256'])
                if key not in proof_files:
                    proof_files[key] = _sha(_read_stable(Path(key[0])))
                _require(proof_files[key] == key[1], label + ' source evidence changed')
    return seed


class GenerationBacklog(Backlog):
    def __init__(self, path, plan_hash, *, seed_path, seed_sha256, source_plan_hash,
                 expected_source_events=2202):
        _require(_hash(plan_hash) and plan_hash != source_plan_hash, 'new generation plan hash required')
        self.seed_path = Path(seed_path).absolute()
        self.seed_sha256 = seed_sha256
        self.seed = validate_seed(self.seed_path, seed_sha256, source_plan_hash, expected_source_events)
        self.source_plan_hash = source_plan_hash
        self.projected_entries = {}
        self.identity_overlay = {}
        offset = self.seed['source_seq'] + 1
        for source_id, source in self.seed['entries'].items():
            event_id = _sha(_bytes([plan_hash, source['job']['exchange'], source['job']['symbol']]))
            entry = copy.deepcopy(source)
            entry.update(event_id=event_id, source_event_id=source_id,
                         source_plan_hash=source_plan_hash, source_entry=copy.deepcopy(source),
                         generation_plan_hash=plan_hash,
                         admitted_seq=source['admitted_seq']-offset,
                         last_attempt_seq=source['last_attempt_seq']-offset)
            self.projected_entries[event_id] = entry
            if source_id in self.seed['identity_overlay']:
                self.identity_overlay[event_id] = copy.deepcopy(self.seed['identity_overlay'][source_id])
        super().__init__(Path(path), plan_hash)
        if self.seq == 0:
            self.entries = copy.deepcopy(self.projected_entries)

    def _apply(self, record):
        if record['kind'] == 'LINEAGE_IMPORT':
            _require(record['seq'] == 1 and record.get('previous_hash') is None,
                     'lineage import must be first and unique')
            _require(record.get('payload') == dict(seed_sha256=self.seed_sha256,
                     source_plan_hash=self.source_plan_hash, source_events=len(self.projected_entries)),
                     'lineage import binding mismatch')
            _require(record.get('evidence') == dict(path=str(self.seed_path),sha256=self.seed_sha256),
                     'lineage import evidence mismatch')
            self.entries = copy.deepcopy(self.projected_entries)
            return
        _require(record['seq'] > 1, 'lineage import required before generation writes')
        applied = record
        if record['kind'] == 'DISCOVERY':
            applied = copy.deepcopy(record)
            for observed in applied['payload']['jobs']:
                event_id = _sha(_bytes([self.plan_hash, observed['exchange'], observed['symbol']]))
                if (event_id in self.entries
                        and observed.get('asset_class') == 'unclassified'
                        and observed.get('asset_class_source') == 'pending_positive_venue_identity_review'
                        and observed.get('asset_class_acceptance_eligible') is False):
                    # Absence of a new identity review is not a contradictory
                    # declaration. Preserve frozen classification for comparison
                    # while the durable DISCOVERY record retains the observation.
                    for field in ('asset_class','asset_class_source','asset_class_acceptance_eligible'):
                        observed[field] = self.entries[event_id]['job'].get(field)
        super()._apply(applied)

    def admit(self, snapshot_path):
        # Validate snapshot BEFORE even writing the import receipt.
        snapshot = _strict_json(_read_stable(Path(snapshot_path)))
        _require(snapshot.get('plan_hash') == self.plan_hash, 'snapshot binding mismatch')
        if self.seq == 0:
            self._append('LINEAGE_IMPORT', dict(seed_sha256=self.seed_sha256,
                source_plan_hash=self.source_plan_hash,source_events=len(self.projected_entries)),
                dict(path=str(self.seed_path),sha256=self.seed_sha256))
        super().admit(snapshot_path)

    def review(self, now_ts):
        entries = copy.deepcopy(self.entries)
        # New discoveries cannot inherit a positive class from ticker spelling.
        for event_id, entry in entries.items():
            if event_id not in self.projected_entries:
                entry['job'].update(asset_class='unclassified',asset_class_source='unclassified',
                                    asset_class_acceptance_eligible=False)
        report = build_review_queues(entries, self.identity_overlay, as_of_ts=now_ts,
            source_plan_hash=self.source_plan_hash,source_ledger_sha256=self.seed['source_ledger_sha256'])
        # The generic offline review selector permits legacy classes. Runtime
        # acquisition additionally requires a raw-backed identity with no defect.
        for rows in report['queues'].values():
            for row in rows:
                row['selectable'] = row['selectable'] and self._identity_allowed(row)
        return report

    @staticmethod
    def _identity_allowed(row):
        return (row['lane'] == 'crypto_descriptive' and row['identity_verified']
                and not row['diagnostics'] and row['identity_status']=='VERIFIED_OFFICIAL_IDENTITY')

    def _enrich_job(self, entry, row, now_ts):
        job = super()._job(entry, now_ts)
        job.update(asset_class='crypto_token',asset_class_source='reviewed_official_identity_seed_v1',
            asset_class_acceptance_eligible=False,source_event_id=entry.get('source_event_id'),
            source_plan_hash=entry.get('source_plan_hash'),generation_plan_hash=self.plan_hash,
            identity_evidence=copy.deepcopy(row['identity_evidence']),
            official_spot_t0=row['official_spot_t0'],official_t0_evidence=copy.deepcopy(row['official_t0_evidence']),
            identity_seed_sha256=self.seed_sha256,queue_lane='crypto_descriptive',
            priority_class=row['priority_class'],source_entry=copy.deepcopy(entry.get('source_entry')))
        job.update(original_asset_class=entry['job'].get('asset_class'),
                   source_ledger_sha256=self.seed['source_ledger_sha256'],
                   identity_review_status=row['identity_status'])
        return job

    def select(self, now_ts, limit):
        report = self.review(now_ts)
        rows = select_proposals(report,lane='crypto_descriptive',limit=limit,
            retry_slots=POLICY['retry_slots'],aging_slots_per_venue=POLICY['aging_slots_per_venue'])
        return [self._enrich_job(self.entries[row['event_id']],row,now_ts) for row in rows]

    def pending(self, now_ts):
        rows = self.review(now_ts)['queues']['crypto_descriptive']
        return [dict(self._enrich_job(self.entries[row['event_id']],row,now_ts),
                     failure_reason='queued_next_interval',retry_disposition='RETRY_NEXT_INTERVAL')
                for row in rows if self._identity_allowed(row)
                and row['queue_status'] in ('PENDING','RETRY') and row['first_seen_ts']<=now_ts]

    def start(self, event_id, tick_id, now_ts):
        report = self.review(now_ts)
        _require(any(row['event_id']==event_id and row['selectable']
                     for row in report['queues']['crypto_descriptive']),
                 'acquisition requires due raw-backed crypto review')
        super().start(event_id,tick_id,now_ts)

    def review_status(self, now_ts, *, completed_ids=()):
        report = self.review(now_ts)
        rows = [row for lane in report['queues'].values() for row in lane]
        completed_ids = set(completed_ids)
        eligible_ids = {row['event_id'] for row in rows if self._identity_allowed(row)
                        and row['queue_status'] in ('PENDING','RETRY')}
        _require(completed_ids <= eligible_ids, 'completed projection contains ineligible event')
        for row in rows:
            if row['event_id'] in completed_ids:
                row['queue_status'] = 'COLLECTED_DESCRIPTIVE'
                row['selectable'] = False
        actionable = [row for row in rows if self._identity_allowed(row)
                      and row['queue_status'] in ('PENDING','RETRY') and row['first_seen_ts']<=now_ts]
        active_ids = {row['event_id'] for row in actionable}
        terminal = [row for row in rows if row['queue_status']=='COLLECTED_DESCRIPTIVE']
        held = [row for row in rows if row['event_id'] not in active_ids
                and row['queue_status']!='COLLECTED_DESCRIPTIVE']
        reasons = {}
        for row in held:
            reason = ('quarantined' if row['queue_status']=='QUARANTINED' else
                      'raw_source_archive_missing' if 'RAW_SOURCE_ARCHIVE_MISSING' in row['diagnostics'] else
                      'equity_descriptive_only' if row['lane']=='equity_descriptive' else
                      'identity_review_required')
            reasons[reason] = reasons.get(reason,0)+1
        return dict(lane_counts=report['counts'],held_count=len(held),
            held_counts={lane:sum(row['lane']==lane for row in held) for lane in report['queues']},
            held_reasons=reasons,terminal_count=len(terminal),non_actionable_count=len(rows)-len(actionable),
            actionable_pending_count=len(actionable),due_crypto_count=sum(row['selectable'] for row in actionable),
            projected_after_outcome_commit=bool(completed_ids),
            completion_commit_pending_count=len(completed_ids),
            lineage=dict(source_plan_hash=self.source_plan_hash,source_ledger_sha256=self.seed['source_ledger_sha256'],
                         seed_sha256=self.seed_sha256,generation_plan_hash=self.plan_hash,
                         source_events=len(self.projected_entries)),
            acceptance_eligible=False,collection_authorized=False)
