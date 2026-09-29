"""Deterministic, OFFLINE review proposals for a verified Expansion backlog.

This module never reads files, starts collectors, changes the source backlog, or
grants acceptance. The caller must verify the ledger and every evidence file's
bytes before supplying VERIFIED proof objects. Here we enforce their metadata,
instrument binding, and causal availability. Identity proof cannot establish t0.
An immutable offline plan must bind the inputs and all policy parameters.
"""
from __future__ import annotations

import copy
from pathlib import PurePosixPath, PureWindowsPath
from urllib.parse import urlsplit


LANES = ('identity_review', 'crypto_descriptive', 'equity_descriptive')
CLASSES = {'crypto_token', 'tokenized_equity', 'preipo_equity', 'equity'}
VENUE_DOMAINS = {'binance': 'binance.com', 'bybit': 'bybit.com',
                 'okx': 'okx.com', 'bitget': 'bitget.com'}
METADATA_SOURCES = {'listTime_ms', 'openTime_ms_deprecated', 'launchTime_ms'}
LEGACY_DECLARATION = 'declared_spot_asset_registry_v1'


def _require(ok, reason):
    if not ok:
        raise ValueError('expansion review queue: ' + reason)


def _hash(value):
    return (isinstance(value, str) and len(value) == 64
            and all(c in '0123456789abcdef' for c in value))


def _positive_int(value):
    return type(value) is int and value > 0


def _proof_error(proof, job, as_of_ts, evidence_class, prefix):
    if not isinstance(proof, dict):
        return prefix + '_missing'
    if proof.get('status') != 'VERIFIED' or proof.get('evidence_class') != evidence_class:
        return prefix + '_unverified_or_wrong_class'
    if proof.get('source_storage') not in {'raw_http_response', 'reviewed_web_extract'}:
        return prefix + '_source_storage_invalid'
    for key in ('exchange', 'symbol', 'base'):
        if job.get(key) and proof.get(key) != job[key]:
            return prefix + '_instrument_mismatch'
    received = proof.get('received_ts')
    if not _positive_int(received):
        return prefix + '_received_ts_invalid'
    if received > as_of_ts:
        return prefix + '_received_after_as_of'
    path = proof.get('source_path')
    if not isinstance(path, str) or not (
            PureWindowsPath(path).is_absolute() or PurePosixPath(path).is_absolute()):
        return prefix + '_source_path_invalid'
    if not _hash(proof.get('source_sha256')):
        return prefix + '_source_sha256_invalid'
    try:
        url = urlsplit(proof.get('source_url', ''))
        domain = VENUE_DOMAINS[job['exchange']]
        hostname = url.hostname or ''
        if (url.scheme != 'https' or url.username is not None or url.password is not None
                or url.port not in (None, 443)
                or not (hostname == domain or hostname.endswith('.' + domain))):
            return prefix + '_source_url_invalid'
    except (KeyError, TypeError, ValueError):
        return prefix + '_source_url_invalid'
    return None


def build_review_queues(entries, identity_overlay, *, as_of_ts, source_plan_hash,
                        source_ledger_sha256, official_future_horizon_sec=86400,
                        official_active_after_sec=60, recent_metadata_horizon_sec=259200):
    """Return three disjoint descriptive lanes; retain terminal rows and lineage.

    `entries` is a verified Backlog.entries mapping. `identity_overlay` is keyed
    by event_id. A proof has status VERIFIED, evidence_class
    OFFICIAL_VENUE_IDENTITY, exchange/symbol/base, asset_class, source_url,
    source_path, source_sha256, received_ts, source_storage. Storage is either
    raw_http_response or reviewed_web_extract. Extracts retain reviewed identity
    but never become selectable or obtain official urgent priority.
    Optional `official_spot_event` is a
    separate proof with class OFFICIAL_ANNOUNCEMENT and official_spot_t0.
    Future/unverified evidence is retained as input evidence but not applied.
    """
    _require(isinstance(entries, dict) and isinstance(identity_overlay, dict),
             'entries and overlay must be mappings')
    _require(_positive_int(as_of_ts), 'as_of_ts must be positive integer seconds')
    _require(_hash(source_plan_hash) and _hash(source_ledger_sha256), 'invalid source lineage')
    _require(all(type(v) is int and v >= 0 for v in (
        official_future_horizon_sec, official_active_after_sec, recent_metadata_horizon_sec)),
        'invalid priority horizons')
    _require(set(identity_overlay) <= set(entries), 'overlay contains unknown event')
    queues = {lane: [] for lane in LANES}
    seen_instruments = set()
    for event_id, source in sorted(entries.items()):
        _require(_hash(event_id) and source.get('event_id') == event_id, 'event identity mismatch')
        job = source.get('job', {})
        venue, symbol = job.get('exchange'), job.get('symbol')
        _require(venue in VENUE_DOMAINS and isinstance(symbol, str) and bool(symbol),
                 'invalid instrument')
        _require((venue, symbol) not in seen_instruments, 'duplicate instrument identity')
        seen_instruments.add((venue, symbol))
        _require(source.get('status') in {'PENDING', 'RETRY', 'QUARANTINED', 'COLLECTED_DESCRIPTIVE'},
                 'unknown source status')
        _require(_positive_int(source.get('first_seen_ts')), 'invalid first_seen_ts')
        _require(type(source.get('next_attempt_after_ts')) is int
                 and source['next_attempt_after_ts'] >= 0, 'invalid next_attempt_after_ts')
        diagnostics = []
        old_class = job.get('asset_class')
        legacy = old_class in CLASSES and job.get('asset_class_source') == LEGACY_DECLARATION
        asset = old_class if legacy else 'unclassified'
        identity_status = 'LEGACY_DECLARED_REGISTRY' if legacy else 'UNKNOWN_IDENTITY'
        identity_verified = False
        raw_archive_missing = False
        evidence = identity_overlay.get(event_id)
        identity_evidence = None
        if evidence is not None:
            error = _proof_error(evidence, job, as_of_ts, 'OFFICIAL_VENUE_IDENTITY', 'identity_evidence')
            if error is None and evidence.get('asset_class') not in CLASSES:
                error = 'identity_evidence_asset_class_invalid'
            if error:
                diagnostics.append(error)
            elif legacy and old_class != evidence['asset_class']:
                asset, identity_status = 'unclassified', 'CONFLICTING_IDENTITY'
                diagnostics.append('identity_evidence_conflicts_with_legacy_declaration')
            else:
                asset = evidence['asset_class']
                raw_archive_missing = evidence['source_storage'] == 'reviewed_web_extract'
                identity_status = ('REVIEWED_WEB_IDENTITY' if raw_archive_missing
                                   else 'VERIFIED_OFFICIAL_IDENTITY')
                identity_verified = not raw_archive_missing
                identity_evidence = copy.deepcopy(evidence)
        event_evidence = evidence.get('official_spot_event') if isinstance(evidence, dict) else None
        official_t0 = None
        if event_evidence is not None:
            error = _proof_error(event_evidence, job, as_of_ts, 'OFFICIAL_ANNOUNCEMENT', 'official_t0_evidence')
            if error is None and not _positive_int(event_evidence.get('official_spot_t0')):
                error = 'official_t0_evidence_timestamp_invalid'
            if error:
                diagnostics.append(error)
            else:
                official_t0 = event_evidence['official_spot_t0']
                raw_archive_missing = (raw_archive_missing or
                                       event_evidence['source_storage'] == 'reviewed_web_extract')
        if raw_archive_missing:
            diagnostics.append('RAW_SOURCE_ARCHIVE_MISSING')
        timestamp_source = job.get('timestamp_source', '')
        if timestamp_source in METADATA_SOURCES:
            timestamp_class = 'METADATA'
        elif isinstance(timestamp_source, str) and timestamp_source.endswith('proxy'):
            timestamp_class = 'DETECTION_PROXY'
        else:
            timestamp_class = 'UNKNOWN_TIMESTAMP'
        anchor = job.get('proxy_ts')
        if (official_t0 is not None and not raw_archive_missing and
                as_of_ts - official_active_after_sec <= official_t0 <= as_of_ts + official_future_horizon_sec):
            priority_rank, priority_class = 0, 'OFFICIAL_EXACT_UPCOMING_OR_ACTIVE'
        elif (timestamp_class == 'METADATA' and _positive_int(anchor)
              and abs(as_of_ts - anchor) <= recent_metadata_horizon_sec):
            priority_rank, priority_class = 1, 'RECENT_METADATA_REVIEW'
        else:
            priority_rank, priority_class = 2, 'AGED_OR_PROXY_DESCRIPTIVE'
        lane = ('crypto_descriptive' if asset == 'crypto_token' else
                'equity_descriptive' if asset in CLASSES else 'identity_review')
        selectable = (source['status'] in {'PENDING', 'RETRY'}
                      and source['next_attempt_after_ts'] <= as_of_ts
                      and source['first_seen_ts'] <= as_of_ts and not raw_archive_missing)
        queues[lane].append(dict(
            event_id=event_id, exchange=venue, symbol=symbol, base=job.get('base'),
            lane=lane, asset_class=asset, identity_status=identity_status,
            identity_verified=identity_verified, identity_evidence=identity_evidence,
            provided_identity_overlay=copy.deepcopy(evidence), diagnostics=diagnostics,
            official_spot_t0=official_t0,
            official_t0_evidence=copy.deepcopy(event_evidence) if official_t0 is not None else None,
            timestamp_class=timestamp_class, original_timestamp_source=timestamp_source,
            original_metadata_or_proxy_ts=anchor, priority_rank=priority_rank,
            priority_class=priority_class, first_seen_ts=source['first_seen_ts'],
            last_attempt_seq=source.get('last_attempt_seq', 0),
            queue_status=source['status'], selectable=selectable,
            acceptance_eligible=False, proposal_only=True,
            source_plan_hash=source_plan_hash, source_ledger_sha256=source_ledger_sha256,
            source_entry=copy.deepcopy(source)))
    return dict(schema='listing_expansion_offline_review_queues_v1', as_of_ts=as_of_ts,
                source_plan_hash=source_plan_hash, source_ledger_sha256=source_ledger_sha256,
                policy=dict(official_future_horizon_sec=official_future_horizon_sec,
                            official_active_after_sec=official_active_after_sec,
                            recent_metadata_horizon_sec=recent_metadata_horizon_sec),
                proposal_only=True, collection_authorized=False, queues=queues,
                counts={lane: len(queues[lane]) for lane in LANES})


def _fair_order(rows, venue_cursor, aging_slots_per_venue):
    """Oldest round(s), then priority rounds; each round grants one per venue."""
    grouped = {}
    for row in rows:
        grouped.setdefault(row['exchange'], []).append(row)
    venues = sorted(grouped)
    if venue_cursor is not None and venue_cursor in venues:
        start = venues.index(venue_cursor)
        venues = venues[start:] + venues[:start]
    age_key = lambda row: (row['last_attempt_seq'], row['first_seen_ts'], row['event_id'])
    priority_key = lambda row: (row['priority_rank'], *age_key(row))
    ordered = []
    for _ in range(aging_slots_per_venue):
        for venue in venues:
            if grouped[venue]:
                oldest = min(grouped[venue], key=age_key)
                grouped[venue].remove(oldest)
                ordered.append(oldest)
    for venue in venues:
        grouped[venue].sort(key=priority_key, reverse=True)
    while any(grouped.values()):
        for venue in venues:
            if grouped[venue]:
                ordered.append(grouped[venue].pop())
    return ordered


def select_proposals(report, *, lane, limit=50, retry_slots=10, venue_cursor=None,
                     aging_slots_per_venue=1):
    """Propose <=50 rows from exactly one lane, without mutating any state.

    Retry slots are both reserved and capped, not borrowed by extra retries.
    Remaining capacity goes to PENDING. One oldest-per-venue round prevents
    priority starvation within each pool; subsequent rounds use priority. For
    small batches the caller can record and rotate an explicit venue_cursor.
    This is a review receipt, not an invocation of the OHLCV runtime selector.
    """
    _require(report.get('schema') == 'listing_expansion_offline_review_queues_v1'
             and report.get('proposal_only') is True
             and report.get('collection_authorized') is False, 'not an offline proposal report')
    _require(lane in LANES, 'select exactly one known lane')
    _require(type(limit) is int and 1 <= limit <= 50, 'proposal cap must be 1..50')
    _require(type(retry_slots) is int and 0 <= retry_slots <= 50, 'invalid retry cap')
    _require(type(aging_slots_per_venue) is int and 0 <= aging_slots_per_venue <= 50,
             'invalid aging reservation')
    _require(venue_cursor is None or venue_cursor in VENUE_DOMAINS, 'invalid venue cursor')
    rows = [row for row in report['queues'][lane] if row['selectable']]
    retries = _fair_order([row for row in rows if row['queue_status'] == 'RETRY'],
                          venue_cursor, aging_slots_per_venue)[:min(limit, retry_slots)]
    pending = _fair_order([row for row in rows if row['queue_status'] == 'PENDING'],
                          venue_cursor, aging_slots_per_venue)[:limit - len(retries)]
    return copy.deepcopy(retries + pending)
