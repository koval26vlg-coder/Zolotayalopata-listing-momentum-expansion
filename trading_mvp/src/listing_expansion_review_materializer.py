"""Hash-bound offline proposal materialization, never a market-data launcher."""
import argparse
import csv
from datetime import datetime
import hashlib
import io
import json
import math
from pathlib import Path


def utc_ts(value):
    if not isinstance(value, str) or not value.endswith('Z'):
        raise ValueError('Explicit UTC source time required')
    return math.ceil(datetime.fromisoformat(value[:-1] + '+00:00').timestamp())


def reviewed_proxy_overlay(entries, review, source, received_ts):
    index = {(e['job']['exchange'], e['job']['symbol']): (key, e['job']) for key, e in entries.items()}
    overlay = {}
    for row in review['rows']:
        key = (row['venue'], row['symbol'])
        if key not in index or row.get('review_status') != 'VERIFIED_OFFICIAL_WEB_SOURCE_NOT_RUNTIME_BOUND':
            raise ValueError('Unmatched or unverified proxy review')
        event_id, job = index[key]
        if event_id in overlay or row.get('base') != job['base']:
            raise ValueError('Duplicate or mismatching reviewed instrument')
        asset = {'crypto': 'crypto_token', 'tokenized_equity': 'tokenized_equity'}.get(row['reviewed_asset_class'])
        if asset is None:
            raise ValueError('Unsupported reviewed class')
        proof = dict(status='VERIFIED', evidence_class='OFFICIAL_VENUE_IDENTITY',
            exchange=key[0], symbol=key[1], base=job['base'], asset_class=asset,
            source_url=row['source_url'], source_storage='reviewed_web_extract',
            source_path=source['source_path'], source_sha256=source['source_sha256'],
            received_ts=received_ts, source_hash_semantics='reviewed_extract_artifact_not_original_article')
        proof['official_spot_event'] = dict(proof, evidence_class='OFFICIAL_ANNOUNCEMENT',
            official_spot_t0=utc_ts(row['official_spot_t0']))
        overlay[event_id] = proof
    return overlay


def selection_summary(lanes):
    return dict(schema='listing_expansion_offline_selection_v1', selection_not_execution=True,
        collector_allowed=False, scheduler_allowed=False,
        proposal_ids={lane: [row['event_id'] for row in rows] for lane, rows in lanes.items()},
        counts={lane: len(rows) for lane, rows in lanes.items()},
        note='Independent review lists, never a combined 150-job collector allocation')


def materialize(plan_path, output_dir):
    from listing_expansion_backlog import Backlog
    from listing_expansion_identity_evidence import bitget_identity_overlay
    from listing_expansion_review_contract import validate_plan, write_new_bundle
    from listing_expansion_review_queue import build_review_queues, select_proposals, LANES

    plan_path, output_dir = Path(plan_path).resolve(), Path(output_dir).absolute()
    plan = json.loads(plan_path.read_text(encoding='utf-8'))
    verified = validate_plan(plan, expected_plan_path=plan_path)
    inputs = {b['role']: b for b in plan['input_files']}
    # The new output is disjoint from source directories, and cannot rewrite a bound file.
    for binding in plan['input_files'] + plan['implementation_files']:
        source_path = Path(binding['path']).resolve()
        if output_dir.resolve() == source_path.parent or source_path.is_relative_to(output_dir.resolve()):
            raise ValueError('Output overlaps a bound input')
    source_plan = json.loads(verified['source_plan'])
    if source_plan['plan_hash'] != plan['source_plan_hash']:
        raise ValueError('Source plan identity mismatch')
    backlog = Backlog(Path(inputs['source_backlog']['path']), plan['source_plan_hash'])
    entries = backlog.entries
    if len(entries) != plan['expected_input_events']:
        raise ValueError('Source event count drift')
    bitget_receipt = json.loads(verified['bitget_receipt'])['sources'][0]
    if bitget_receipt['sha256'] != inputs['bitget_raw']['sha256'] or bitget_receipt['http_status'] != 200:
        raise ValueError('Bitget response receipt mismatch')
    overlay = bitget_identity_overlay(entries, json.loads(verified['bitget_raw']), dict(
        source_url=bitget_receipt['url'], source_path=inputs['bitget_raw']['path'],
        source_sha256=inputs['bitget_raw']['sha256'], received_ts=utc_ts(bitget_receipt['received_at_utc'])))

    xdp = json.loads(verified['okx_review'])
    event_id = xdp['backlog_event_id']
    job = entries[event_id]['job']
    metadata = json.loads(verified['okx_instrument'])
    if metadata.get('code') != '0' or len(metadata.get('data', [])) != 1:
        raise ValueError('OKX instrument response not a singleton success')
    instrument = metadata['data'][0]
    if (job['exchange'] != 'okx' or job['symbol'] != 'XDP-USDT' or job['base'] != 'XDP'
            or instrument['instId'] != job['symbol'] or instrument['instCategory'] != '1'
            or instrument['instType'] != 'SPOT' or instrument['baseCcy'] != job['base']
            or instrument['quoteCcy'] != 'USDT'):
        raise ValueError('OKX identity mismatch')
    sources = {s['id']: s for s in xdp['sources']}
    for source_id, role in [('instrument_metadata', 'okx_instrument'), ('spot_announcement', 'okx_announcement')]:
        if sources[source_id]['sha256'] != inputs[role]['sha256']:
            raise ValueError('OKX source hash mismatch')
    timestamps = xdp['timestamps']
    t0 = utc_ts(timestamps['official_spot_t0'])
    if (int(instrument['contTdSwTime']) // 1000 != t0
            or int(instrument['listTime']) // 1000 != utc_ts(timestamps['auction_start_ts'])):
        raise ValueError('OKX timing disagreement')
    proof = dict(status='VERIFIED', evidence_class='OFFICIAL_VENUE_IDENTITY', asset_class='crypto_token',
        exchange='okx', symbol=job['symbol'], base=job['base'], source_storage='raw_http_response',
        source_url=sources['instrument_metadata']['url'], source_path=inputs['okx_instrument']['path'],
        source_sha256=inputs['okx_instrument']['sha256'],
        received_ts=utc_ts(timestamps['instrument_metadata_received_at_utc']))
    proof['official_spot_event'] = dict(proof, evidence_class='OFFICIAL_ANNOUNCEMENT',
        source_url=sources['spot_announcement']['url'], source_path=inputs['okx_announcement']['path'],
        source_sha256=inputs['okx_announcement']['sha256'],
        received_ts=utc_ts(timestamps['spot_announcement_received_at_utc']), official_spot_t0=t0,
        auction_start_ts=utc_ts(timestamps['auction_start_ts']),
        announcement_published_ts=utc_ts(timestamps['spot_announcement_published_at_utc']),
        execution_caveats=xdp['execution_caveats'], first_trade_ts=None,
        premarket_contract_launch_ts=None, transition_ts=None)
    overlay[event_id] = proof
    proxy = reviewed_proxy_overlay(entries, json.loads(verified['proxy_review']), dict(
        source_path=inputs['proxy_review']['path'], source_sha256=inputs['proxy_review']['sha256']),
        plan['manual_review_received_ts'])
    if set(proxy) & set(overlay):
        raise ValueError('Conflicting evidence overlay producers')
    overlay.update(proxy)
    report = build_review_queues(entries, overlay, as_of_ts=plan['as_of_ts'],
        source_plan_hash=plan['source_plan_hash'], source_ledger_sha256=inputs['source_backlog']['sha256'],
        **{k: plan['policy'][k] for k in ('official_future_horizon_sec', 'official_active_after_sec', 'recent_metadata_horizon_sec')})
    selected = {lane: select_proposals(report, lane=lane, limit=plan['policy']['max_proposals'],
        retry_slots=plan['policy']['retry_slots'], aging_slots_per_venue=plan['policy']['aging_slots_per_venue']) for lane in LANES}
    summary = selection_summary(selected)
    summary.update(plan_id=plan['plan_id'], plan_hash=plan['plan_hash'],
        events=sum(report['counts'].values()), lane_counts=report['counts'],
        source_backlog_hash=inputs['source_backlog']['sha256'], source_ledger_records=backlog.seq,
        manual_review_blocked=sum('RAW_SOURCE_ARCHIVE_MISSING' in r['diagnostics'] for lane in LANES for r in report['queues'][lane]),
        official_urgent_count=sum(r['priority_class']=='OFFICIAL_EXACT_UPCOMING_OR_ACTIVE' for lane in LANES for r in report['queues'][lane]),
        input_files=plan['input_files'])
    def encoded(value):
        return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)+'\n').encode('utf-8')
    files = {'queues.json': encoded(report), 'identity_overlay.json': encoded(overlay), 'selection.json': encoded(summary)}
    fields = ['event_id', 'exchange', 'symbol', 'asset_class', 'identity_status', 'queue_status', 'selectable',
              'priority_class', 'original_timestamp_source', 'original_metadata_or_proxy_ts', 'official_spot_t0',
              'acceptance_eligible']
    for lane in LANES:
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(report['queues'][lane])
        files[lane+'.csv'] = stream.getvalue().encode('utf-8-sig')
    files['manifest.json'] = encoded(dict(plan_id=plan['plan_id'], plan_hash=plan['plan_hash'],
        status='OFFLINE_MATERIALIZED_NOT_ACTIVATED', output_sha256={n: hashlib.sha256(v).hexdigest() for n,v in files.items()}))
    # Recheck all evidence and implementation after transformations and before any output.
    if validate_plan(plan, expected_plan_path=plan_path) != verified:
        raise ValueError('Input changed during materialization')
    write_new_bundle(output_dir, files)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    summary = materialize(args.plan, args.out)
    print(json.dumps({k: summary[k] for k in ('plan_id', 'events', 'lane_counts', 'counts', 'manual_review_blocked', 'official_urgent_count', 'collector_allowed')}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
