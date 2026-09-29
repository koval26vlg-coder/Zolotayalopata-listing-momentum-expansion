"""Offline v19 integration contract. No live endpoints or installed state."""
import importlib.util
import hashlib
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
NAME = 'slow_liquidity_listing_momentum_forward_expansion_monitor_v19'
monitor = __import__(NAME) if importlib.util.find_spec(NAME) else None


class MonitorV19Contracts(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(monitor, 'versioned v19 monitor is missing')

    def test_versioned_generation_does_not_share_v18_state_or_ledger(self):
        self.assertEqual('slow_liquidity_listing_momentum_forward_expansion_20260929_v19', monitor.PLAN_ID)
        self.assertEqual('ticks-v19', monitor.TICKS_DIR.name)
        self.assertEqual('backlog-v19', monitor.BACKLOG_PATH.parent.name)
        self.assertIn('20260929_v19', monitor.STATE_PATH.name)
        self.assertIn('v19_terminal', monitor.TERMINAL_ATTEMPTS_PATH.name)

    def test_new_discovery_never_uses_legacy_ticker_declaration_as_identity(self):
        rows = [dict(exchange='bitget', base='DGAI', symbol='DGAIUSDT', quote='USDT', is_delisted=False)]
        results = monitor.diff_new_listings(set(), rows, baseline_as_of_ts=1, now_ts=1800000000)
        self.assertEqual(1, len(results))
        self.assertEqual('unclassified', results[0]['asset_class'])
        self.assertFalse(results[0]['asset_class_acceptance_eligible'])
        self.assertEqual('pending_positive_venue_identity_review', results[0]['asset_class_source'])

    def test_raw_rows_copy_lineage_and_never_grant_acceptance(self):
        job = dict(exchange='okx', base='XDP', symbol='XDP-USDT', proxy_ts=100,
                   timestamp_source='listTime_ms', asset_class='crypto_token',
                   asset_class_source='reviewed_official_identity', asset_class_acceptance_eligible=False,
                   event_id='a'*64, source_event_id='b'*64, generation_plan_hash='c'*64,
                   source_plan_hash='d'*64, official_spot_t0=3700,
                   identity_evidence={'source_sha256':'e'*64}, original_asset_class='unclassified')
        bar = SimpleNamespace(ts=100,open=1.,high=2.,low=.5,close=1.,volume=1.)
        row = monitor._forward_row(job, bar)
        for key in ('source_event_id','generation_plan_hash','source_plan_hash','official_spot_t0',
                    'identity_evidence','original_asset_class'):
            self.assertEqual(job[key], row[key])
        self.assertFalse(row['asset_class_acceptance_eligible'])
        self.assertEqual(100, row['proxy_event_ts'])

    def test_run_tick_validates_before_claim_or_fetch(self):
        with mock.patch.object(monitor, '_validate_plan', side_effect=ValueError('bound seed mismatch')):
            with mock.patch.object(monitor, 'claim_global_market_writer') as claim:
                fetcher=mock.Mock()
                with self.assertRaisesRegex(ValueError, 'bound seed mismatch'):
                    monitor.run_tick({}, tick_id='offline', clients={}, fetcher=fetcher)
                claim.assert_not_called()
                fetcher.assert_not_called()

    def test_even_legacy_eligible_rows_never_grant_v19_acceptance(self):
        provenance = monitor._window_asset_provenance([
            dict(asset_class='crypto_token', asset_class_source=monitor.DECLARATION_SOURCE,
                 asset_class_acceptance_eligible=True)], None)
        self.assertFalse(provenance['asset_class_acceptance_eligible'])

    def test_held_only_manifest_does_not_escalate_cadence(self):
        with tempfile.TemporaryDirectory(prefix='v19-held-state-') as temp:
            root=Path(temp)
            plan_path=root/'plan.json'
            plan_path.write_text(json.dumps({'plan_hash':'a'*64}),encoding='utf-8')
            tick=root/'ticks'/'t1'; tick.mkdir(parents=True)
            (tick/'manifest.json').write_text(json.dumps(dict(tick_id='t1',plan_hash='a'*64,
                jobs=[],backlog_pending_count=0,queue_review=dict(held_count=2190),
                rows_written=0,status='COMPLETED')),encoding='utf-8')
            with mock.patch.multiple(monitor, TICKS_DIR=root/'ticks', STATE_PATH=root/'state.json',
                                     PLAN_PATH=plan_path):
                state=monitor.rebuild_forward_state()
            self.assertFalse(state['cadence_observation']['candidate'])
            self.assertEqual(0,state['current_plan_crypto_acceptance_window_count'])
            self.assertEqual(2190,(state.get('queue_review') or {}).get('held_count'))

    def plan_fixture(self):
        preflight=json.loads(monitor.DEFAULT_PREFLIGHT_PATH.read_text(encoding='utf-8'))
        plan=dict(schema=monitor.SCHEMA,plan_id=monitor.PLAN_ID,mode='PlanOnly',research_only=True,
            public_data_only=True,private_api=False,live_orders=False,real_capital=False,
            leverage_or_margin=False,replay_allowed=False,evaluator_or_oos_allowed=False,
            venues=list(monitor.SUPPORTED_VENUES),backlog_contract=monitor.BACKLOG_CONTRACT,
            tick=dict(backlog_path=str(monitor.BACKLOG_PATH),max_new_listings_per_tick=50,
                tick_output_root=str(monitor.TICKS_DIR),state_path=str(monitor.STATE_PATH),
                terminal_attempts_ledger_path=str(monitor.TERMINAL_ATTEMPTS_PATH),claim_path=str(monitor.CLAIM_PATH)),
            guard_contract=dict(combined_launcher_scheduler_routable=False),
            source_bindings=dict(preflight=dict(path=str(monitor.DEFAULT_PREFLIGHT_PATH),
                file_sha256=monitor._sha256_file(monitor.DEFAULT_PREFLIGHT_PATH),receipt_hash=preflight['receipt_hash'])))
        return plan

    def test_implementation_duplicate_role_rejected_before_seed(self):
        plan=self.plan_fixture()
        path=Path(monitor.__file__)
        binding=dict(role='expansion_monitor',path=str(path),sha256=monitor._sha256_file(path))
        plan['implementation']=dict(files=[binding,binding])
        plan['plan_hash']=monitor.canonical_hash(plan)
        with mock.patch.object(monitor,'implementation_roles',return_value={'expansion_monitor':path}):
            with self.assertRaisesRegex(ValueError,'duplicate'):
                monitor._validate_plan(plan,monitor.PLAN_PATH)

    def test_identical_byte_copy_is_not_an_implementation_path(self):
        plan=self.plan_fixture()
        path=Path(monitor.__file__)
        with tempfile.TemporaryDirectory(prefix='v19-path-copy-') as temp:
            copied=Path(temp)/path.name; copied.write_bytes(path.read_bytes())
            plan['implementation']=dict(files=[dict(role='expansion_monitor',path=str(copied),
                sha256=monitor._sha256_file(path))])
            plan['plan_hash']=monitor.canonical_hash(plan)
            with mock.patch.object(monitor,'implementation_roles',return_value={'expansion_monitor':path}):
                with self.assertRaisesRegex(ValueError,'path mismatch'):
                    monitor._validate_plan(plan,monitor.PLAN_PATH)

    def test_loaded_module_must_match_bound_implementation_location(self):
        plan=self.plan_fixture()
        import listing_expansion_generation as generation
        path=Path(generation.__file__)
        plan['implementation']=dict(files=[dict(role='expansion_generation',path=str(path),
            sha256=monitor._sha256_file(path))])
        plan['plan_hash']=monitor.canonical_hash(plan)
        with mock.patch.object(monitor,'implementation_roles',return_value={'expansion_generation':path}):
            with mock.patch.object(generation,'__file__',str(path.with_name('unbound-copy.py'))):
                with self.assertRaisesRegex(ValueError,'loaded implementation path mismatch'):
                    monitor._validate_plan(plan,monitor.PLAN_PATH)


if __name__ == '__main__':
    unittest.main()


class MonitorV19TickIntegration(unittest.TestCase):
    """Actual generation ledger + materialized fixture proofs; public HTTP mocked."""
    NOW=1800000000
    PLAN='a'*64
    SOURCE='b'*64

    def setUp(self):
        self.assertIsNotNone(monitor, 'versioned monitor missing')
        from listing_expansion_backlog import Backlog
        self.temp=tempfile.TemporaryDirectory(prefix='v19-offline-tick-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.ticks=self.root/'ticks'; self.qpath=self.root/'new-ledger.jsonl'
        self.plan_path=self.root/'plan.json'
        self.plan_path.write_text(json.dumps({'plan_hash':self.PLAN}),encoding='utf-8')
        self.source_path=self.root/'source-ledger.jsonl'
        source=Backlog(self.source_path,self.SOURCE)
        anchor=self.NOW-4*86400
        jobs=[dict(exchange='okx',symbol=base+'-USDT',base=base,proxy_ts=anchor,
            timestamp_source='listTime_ms',probe_start_ts=anchor-3600,
            window_end_ts=anchor+72*3600,category='new_listing_window_complete',
            asset_class='unclassified',asset_class_source='unclassified',
            asset_class_acceptance_eligible=False) for base in ('COIN','STOCK','UNKNOWN')]
        self.snapshot=self.root/'source-discovery.json'
        self.snapshot.write_text(json.dumps(dict(plan_hash=self.SOURCE,tick_id='old',
            now_ts=anchor,jobs=jobs,current_rows=[],skipped=[])),encoding='utf-8')
        source.admit(self.snapshot)
        self.source_bytes=self.source_path.read_bytes()
        raw=self.root/'raw-public-evidence.json'; raw.write_bytes(b'{"fixture":"public venue metadata"}')
        overlay={}
        for eid,entry in source.entries.items():
            job=entry['job']
            if job['base']=='UNKNOWN': continue
            overlay[eid]=dict(status='VERIFIED',evidence_class='OFFICIAL_VENUE_IDENTITY',
                exchange='okx',symbol=job['symbol'],base=job['base'],
                asset_class='crypto_token' if job['base']=='COIN' else 'tokenized_equity',
                source_storage='raw_http_response',source_path=str(raw),
                source_url='https://www.okx.com/api/v5/public/instruments',
                source_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),received_ts=anchor)
        self.seed=dict(schema='expansion_reviewed_lineage_seed_v1',source_plan_hash=self.SOURCE,
            source_ledger_sha256=hashlib.sha256(self.source_bytes).hexdigest(),source_seq=source.seq,
            review_plan=dict(path=str(self.plan_path),sha256='c'*64,plan_hash='d'*64),
            entries=copy.deepcopy(source.entries),identity_overlay=overlay)
        self.seed_path=self.root/'seed.json'
        self.seed_path.write_text(json.dumps(self.seed),encoding='utf-8')
        self.plan=dict(plan_hash=self.PLAN,source_bindings=dict(preflight=dict(path=str(self.plan_path),baseline_as_of_ts=1)),
            queue_generation=dict(seed_path=str(self.seed_path),seed_sha256=hashlib.sha256(self.seed_path.read_bytes()).hexdigest(),
                                  source_plan_hash=self.SOURCE,expected_source_events=3))
        for name,value in dict(TICKS_DIR=self.ticks,BACKLOG_PATH=self.qpath,STATE_PATH=self.root/'state.json',
            PLAN_PATH=self.plan_path,CLAIM_PATH=self.root/'claim.json',LEGACY_CLAIM_PATH=self.root/'legacy.json',
            TERMINAL_ATTEMPTS_PATH=self.root/'attempts.jsonl').items():
            patch=mock.patch.object(monitor,name,value);patch.start();self.addCleanup(patch.stop)
        def claim(path,**kw):
            return dict(run_id=kw['run_id'],plan_hash=self.PLAN,owner_pid=123,
                owner_process_started_at_utc='2026-09-29T00:00:00Z',ownership_token='e'*32,
                output_namespace=str(kw['output_namespace']))
        for name,side in (('_validate_plan',lambda *a:None),('claim_global_market_writer',claim),
                          ('release_global_market_writer',lambda *a,**k:None),('load_baseline_keys',lambda *a:set())):
            patch=mock.patch.object(monitor,name,side_effect=side);patch.start();self.addCleanup(patch.stop)

    def run_tick(self,tick,rows=(),now=None,collector=None):
        def full(client,job,**kwargs):
            bars=[SimpleNamespace(ts=ts,open=1.,high=1.1,low=.9,close=1.,volume=10.)
                  for ts in range(((job['proxy_ts']+3599)//3600)*3600,job['window_end_ts'],3600)]
            return bars,1
        with mock.patch.object(monitor,'collect_window_bars',side_effect=collector or full) as collect:
            result=monitor.run_tick(self.plan,tick_id=tick,clients={'okx':object()},
                fetcher=lambda:(list(rows),1),now_ts=self.NOW if now is None else now)
        return result,collect

    def test_tick_collects_only_reviewed_crypto_and_keeps_source_bytes(self):
        result,collect=self.run_tick('t1')
        self.assertEqual(1,collect.call_count)
        self.assertEqual('COIN-USDT',collect.call_args.args[1]['symbol'])
        self.assertEqual(2,result['queue_review']['held_count'])
        self.assertEqual(0,result['jobs_pending_retry'])
        self.assertEqual('COMPLETED',result['status'])
        self.assertEqual(self.source_bytes,self.source_path.read_bytes())
        raw=json.loads((self.ticks/'t1'/'ohlcv.jsonl').read_text(encoding='utf-8').splitlines()[0])
        self.assertEqual('crypto_token',raw['asset_class'])
        self.assertEqual(self.PLAN,raw['generation_plan_hash'])
        self.assertEqual(self.SOURCE,raw['source_plan_hash'])
        self.assertIsNotNone(raw['source_event_id'])
        self.assertFalse(raw['asset_class_acceptance_eligible'])
        again,collect2=self.run_tick('t2',now=self.NOW+600)
        collect2.assert_not_called()
        self.assertEqual(2,again['queue_review']['held_count'])
        self.assertFalse(again['pending_retry'])

    def test_new_unknown_is_admitted_but_never_collected(self):
        new=dict(exchange='okx',base='NEW',symbol='NEW-USDT',quote='USDT',is_delisted=False)
        result,collect=self.run_tick('t1',rows=[new])
        self.assertEqual(1,collect.call_count)
        self.assertEqual(4,result['backlog_total_count'])
        self.assertEqual(3,result['queue_review']['held_count'])
        self.assertEqual(0,result['backlog_pending_count'])

    def test_request_failure_retries_crypto_without_equity_or_unknown(self):
        result,collect=self.run_tick('t1',collector=lambda *a,**k:(_ for _ in ()).throw(OSError('fixture')))
        self.assertEqual(1,result['jobs_pending_retry'])
        self.assertEqual(1,result['jobs_failed'])
        self.assertEqual('STOPPED_INCOMPLETE',result['status'])
        self.assertEqual(['COIN-USDT'],[j['symbol'] for j in result['retry_queue']])
        early,collect2=self.run_tick('t2',now=self.NOW+1)
        collect2.assert_not_called()
        self.assertEqual(1,early['jobs_pending_retry'])
        complete,collect3=self.run_tick('t3',now=self.NOW+600)
        self.assertEqual(1,collect3.call_count)
        self.assertFalse(complete['pending_retry'])

    def test_manifest_projects_completion_before_durable_outcome_commit(self):
        result,_=self.run_tick('t1')
        self.assertEqual(0,result['queue_review']['actionable_pending_count'])
        self.assertEqual(1,result['queue_review']['terminal_count'])
