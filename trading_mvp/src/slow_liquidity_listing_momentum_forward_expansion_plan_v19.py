"""Immutable reviewed-lineage successor; building/checking never collects data."""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from listing_expansion_backlog import Backlog
from listing_expansion_review_contract import validate_plan as validate_review, _read_stable, _strict_json
from slow_liquidity_spot_v2_official_page_discovery import canonical_hash

ROOT = Path(__file__).resolve().parents[2]
PREVIOUS = ROOT / 'docs/plans/slow-liquidity-listing-momentum-forward-expansion-planonly-20260925-v18.json'
PREVIOUS_HASH = '5f32fed7297b46c5b657623157ad816e72e79ed6307f309ead3efd6d4e3ebb31'
PREVIOUS_FILE_SHA = '5a214eb887ec43178b934c14fdbb7aedbdb0d48524177bcff5552ba6cb4ea64d'
REVIEW = ROOT / 'docs/plans/listing-expansion-identity-priority-planonly-20260929-v2.json'
REVIEW_HASH = '8e11bcb2a03688f20539ee0797f35ba7a4f1143c2a30bbd038d4b58738725706'
REVIEW_BUNDLE = Path(r'C:\Users\koval\Documents\ChatGPT\Listing\reports\expansion-identity-priority-20260929\materialized-v2')
REVIEW_MANIFEST_SHA = '1d94159a17f573c28fd702077fc61d3840896faf37b92574da669691d1f8128b'
SEED = ROOT / 'docs/evidence/listing-expansion-reviewed-lineage-seed-20260929-v1.json'
PLAN = ROOT / 'docs/plans/slow-liquidity-listing-momentum-forward-expansion-planonly-20260929-v19.json'


def _require(ok, reason):
    if not ok:
        raise ValueError('expansion successor: ' + reason)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _binding(path):
    path = Path(path).absolute()
    return dict(path=str(path), sha256=_sha(_read_stable(path)))


def _bound(binding):
    _require(isinstance(binding, dict), 'missing file binding')
    raw = _read_stable(binding.get('path'))
    _require(_sha(raw) == binding.get('sha256'), 'bound file changed')
    return raw


def _review_inputs(review_path):
    raw = _read_stable(review_path)
    review = _strict_json(raw)
    _require(review.get('plan_hash') == REVIEW_HASH, 'unreviewed review generation')
    verified = validate_review(review, expected_plan_path=review_path)
    inputs = {row['role']: row for row in review['input_files']}
    _require(review['source_plan_hash'] == PREVIOUS_HASH, 'wrong predecessor')
    source = Backlog(Path(inputs['source_backlog']['path']), PREVIOUS_HASH)
    _require(len(source.entries) == review['expected_input_events'] == 2202, 'source count drift')
    _require(_sha(source.raw) == inputs['source_backlog']['sha256'], 'source ledger changed')
    return review, source, inputs, verified


def _review_bundle(bindings, review, source):
    _require(set(bindings) == {'manifest', 'identity_overlay', 'queues'}, 'review bundle roles')
    # A self-published manifest cannot authorize a different classification merely
    # by repeating a valid review plan hash. Pin the independently reviewed output.
    for role in bindings:
        _require(bindings[role].get('path') == str(REVIEW_BUNDLE / (role + '.json')),
                 'reviewed bundle path mismatch')
    _require(bindings['manifest'].get('sha256') == REVIEW_MANIFEST_SHA,
             'reviewed bundle manifest hash mismatch')
    manifest = _strict_json(_bound(bindings['manifest']))
    _require(manifest.get('plan_hash') == review['plan_hash'], 'review bundle plan')
    for role in ('identity_overlay', 'queues'):
        _require(manifest['output_sha256'][role + '.json'] == bindings[role]['sha256'], 'bundle file binding')
    overlay = _strict_json(_bound(bindings['identity_overlay']))
    queues = _strict_json(_bound(bindings['queues']))
    rows = [row for lane in queues['queues'].values() for row in lane]
    by_id = {row['event_id']: row for row in rows}
    _require(len(rows) == len(by_id) == len(source.entries), 'duplicate or missing review event')
    _require(set(by_id) == set(source.entries), 'review/source identity mismatch')
    _require(all(row['source_entry'] == source.entries[key] for key, row in by_id.items()), 'review source entry drift')
    return overlay


def build_seed(bundle_dir):
    review, source, inputs, _ = _review_inputs(REVIEW)
    bundle_dir = Path(bundle_dir).absolute()
    bindings = {role: _binding(bundle_dir / (role + '.json')) for role in ('manifest', 'identity_overlay', 'queues')}
    overlay = _review_bundle(bindings, review, source)
    return dict(schema='expansion_reviewed_lineage_seed_v1', source_plan_hash=PREVIOUS_HASH,
        source_ledger_sha256=inputs['source_backlog']['sha256'], source_seq=source.seq,
        entries=source.entries, identity_overlay=overlay,
        review_plan=dict(_binding(REVIEW), plan_hash=review['plan_hash']), review_bundle=bindings,
        acceptance_eligible=False, purpose='linked_classification_only_no_collection_or_state_migration',
        generated_at_utc=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))


def validate_queue_generation(plan):
    from listing_expansion_generation import validate_seed
    generation = plan.get('queue_generation') or {}
    _require(isinstance(generation, dict), 'queue generation missing')
    _require(generation.get('seed_path') == str(SEED), 'seed path mismatch')
    _require(generation.get('source_plan_hash') == PREVIOUS_HASH, 'source plan hash mismatch')
    _require(generation.get('expected_source_events') == 2202, 'source count contract mismatch')
    seed = validate_seed(generation['seed_path'], generation.get('seed_sha256'), PREVIOUS_HASH, 2202)
    _require(seed.get('schema') == 'expansion_reviewed_lineage_seed_v1', 'seed schema')
    _require(seed.get('source_plan_hash') == PREVIOUS_HASH, 'seed predecessor')
    _require(seed['review_plan']['path'] == str(REVIEW), 'review path mismatch')
    _bound(seed['review_plan'])
    review, source, inputs, _ = _review_inputs(REVIEW)
    _require(seed['review_plan']['plan_hash'] == review['plan_hash'], 'seed review binding')
    _require(seed['entries'] == source.entries and seed['source_seq'] == source.seq, 'seed lineage drift')
    _require(seed['source_ledger_sha256'] == inputs['source_backlog']['sha256'], 'seed source ledger hash')
    _require(seed['identity_overlay'] == _review_bundle(seed['review_bundle'], review, source), 'seed identity drift')
    return seed


def build_plan(generated_at_utc=None):
    import slow_liquidity_listing_momentum_forward_expansion_monitor_v19 as monitor
    previous_raw = _read_stable(PREVIOUS)
    _require(_sha(previous_raw) == PREVIOUS_FILE_SHA, 'immutable predecessor bytes changed')
    previous = _strict_json(previous_raw)
    _require(previous['plan_hash'] == PREVIOUS_HASH == canonical_hash(previous), 'predecessor identity')
    plan = copy.deepcopy(previous)
    plan.update(plan_id=monitor.PLAN_ID, generated_at_utc=generated_at_utc or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))
    plan['queue_generation'] = dict(seed_path=str(SEED), seed_sha256=_sha(_read_stable(SEED)),
        source_plan_hash=PREVIOUS_HASH, expected_source_events=2202)
    validate_queue_generation(plan)
    plan['backlog_contract'] = copy.deepcopy(monitor.BACKLOG_CONTRACT)
    plan['tick'].update(tick_output_root=str(monitor.TICKS_DIR), state_path=str(monitor.STATE_PATH),
        backlog_path=str(monitor.BACKLOG_PATH), terminal_attempts_ledger_path=str(monitor.TERMINAL_ATTEMPTS_PATH),
        acquisition_lane='crypto_descriptive', held_lanes=['identity_review', 'equity_descriptive'])
    plan['source_bindings']['technical_rebind'] = dict(kind='reviewed_lineage_generation_successor',
        supersedes_plan_id=previous['plan_id'], supersedes_plan_hash=PREVIOUS_HASH,
        supersedes_plan_file_sha256=PREVIOUS_FILE_SHA, supersedes_plan_path=str(PREVIOUS),
        research_scope_changed=True,
        research_scope_change=dict(changed_fields=['job_selection', 'classification_provenance', 'generation_lineage', 'output_namespace'],
            reason='User-approved isolated crypto acquisition lane with reviewed identities; preserve all prior events/outcomes as linked source evidence.',
            autopilot_authority='WITHDRAWN_UNTIL_REVIEWED'),
        installation_authorized=True, collection_or_schedule_activation_authorized=False)
    old_roles = {row['role']: row for row in previous['implementation']['files']}
    plan['implementation']['files'] = [dict(role=role, **_binding(path), provenance=dict(
        superseded_plan_hash=PREVIOUS_HASH, superseded_sha256=old_roles.get(role, {}).get('sha256'),
        kind='immutable_versioned_successor_or_unchanged_dependency')) for role, path in monitor.implementation_roles().items()]
    plan['asset_class_contract'].update(classifier_source='hash_bound_reviewed_lineage_seed_v1',
        acquisition_class='crypto_token', unknown_is_acquisition_eligible=False,
        tokenized_equity_is_acquisition_eligible=False, new_discoveries_without_review='HELD_IDENTITY_REVIEW')
    plan['commands'] = dict(plan_check='python trading_mvp/src/slow_liquidity_listing_momentum_forward_expansion_monitor_v19.py --plan-check',
        status='python trading_mvp/src/slow_liquidity_listing_momentum_forward_expansion_monitor_v19.py --status',
        visible_tick='pwsh -NoProfile -ExecutionPolicy Bypass -File tools/start_listing_momentum_forward_expansion_tick_v19_visible.ps1 -ScheduledTick -Json')
    plan['installation_contract'] = dict(scheduler_enabled=False, execute_during_install=False,
        lineage_import='first_separately_authorized_writer_only', no_new_market_requests_during_install=True)
    plan['plan_hash'] = canonical_hash(plan)
    monitor._validate_plan(plan, PLAN)
    return plan


def publish_new(path, payload):
    path = Path(path)
    raw = (json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + '\n').encode('utf-8')
    _require(not path.exists(), 'immutable output already exists')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        _require(handle.write(raw) == len(raw), 'short immutable write')
        handle.flush()
        os.fsync(handle.fileno())
    _require(_read_stable(path) == raw, 'immutable readback mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--build-seed', type=Path)
    action.add_argument('--build', action='store_true')
    action.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.build_seed:
        seed = build_seed(args.build_seed)
        publish_new(SEED, seed)
        print(json.dumps(dict(status='SEED_FROZEN', events=len(seed['entries']), sha256=_sha(_read_stable(SEED)), execution_performed=False)))
    elif args.build:
        plan = build_plan()
        publish_new(PLAN, plan)
        print(json.dumps(dict(status='PLAN_OK', plan_hash=plan['plan_hash'], execution_performed=False)))
    else:
        import slow_liquidity_listing_momentum_forward_expansion_monitor_v19 as monitor
        plan = monitor.load_plan(PLAN)
        print(json.dumps(dict(status='PLAN_OK', plan_hash=plan['plan_hash'], execution_performed=False)))


if __name__ == '__main__':
    main()
