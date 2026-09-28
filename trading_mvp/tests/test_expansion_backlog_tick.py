import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import slow_liquidity_listing_momentum_forward_expansion_monitor as monitor

NOW = 1_800_000_000
PLAN = 'a'*64


def row(i):
    return dict(exchange='binance', base=f'T{i:04}', symbol=f'T{i:04}USDT',
                quote='USDT', is_delisted=False)


def bars_for(job):
    return [SimpleNamespace(ts=ts, open=1., high=1.1, low=.9, close=1., volume=10.)
            for ts in range(((job['proxy_ts']+3599)//3600)*3600, job['window_end_ts'], 3600)]


class TickBacklogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='expansion-queue-tick-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ticks = self.root/'ticks'
        self.qpath = self.root/'backlog.jsonl'
        self.plan_path = self.root/'plan.json'
        self.plan_path.write_text(json.dumps({'plan_hash':PLAN}), encoding='utf-8')
        for name,value in dict(TICKS_DIR=self.ticks, BACKLOG_PATH=self.qpath,
             STATE_PATH=self.root/'state.json', PLAN_PATH=self.plan_path,
             CLAIM_PATH=self.root/'claim.json', LEGACY_CLAIM_PATH=self.root/'legacy.json',
             TERMINAL_ATTEMPTS_PATH=self.root/'attempts.jsonl').items():
            patch = mock.patch.object(monitor,name,value,create=True)
            patch.start(); self.addCleanup(patch.stop)
        def claim(path, **kw):
            return dict(run_id=kw['run_id'], plan_hash=PLAN, owner_pid=123,
                        owner_process_started_at_utc='2026-09-25T00:00:00Z',
                        ownership_token='b'*32, output_namespace=str(kw['output_namespace']))
        for name,side in [('claim_global_market_writer',claim),
                          ('release_global_market_writer',lambda *a,**k:None),
                          ('load_baseline_keys',lambda *a:set())]:
            patch = mock.patch.object(monitor,name,side_effect=side)
            patch.start(); self.addCleanup(patch.stop)
        self.plan = {'plan_hash':PLAN,'source_bindings':{'preflight':{'path':str(self.plan_path),'baseline_as_of_ts':1}}}

    def run_tick(self, tick, rows, now=NOW, collector=None):
        with mock.patch.object(monitor,'collect_window_bars',side_effect=collector or (lambda c,j,**kw:(bars_for(j),1))):
            return monitor.run_tick(self.plan,tick_id=tick,clients={'binance':object()},
                                    fetcher=lambda:(rows,1), now_ts=now)

    def test_151_candidates_are_durable_before_first_request_and_fairly_capped(self):
        attempted = []
        def collect(c,j,**kw):
            self.assertTrue(self.qpath.exists(), 'queue must exist before first OHLCV request')
            snap = json.loads((self.ticks/'t1'/'discovery.json').read_text(encoding='utf-8'))
            self.assertEqual(151,len(snap['jobs']))
            attempted.append(j['symbol'])
            return bars_for(j),1
        first = self.run_tick('t1',[row(i) for i in range(151)],collector=collect)
        self.assertEqual(50,first['jobs_attempted'])
        self.assertEqual(151,first['backlog_pending_count'])
        self.assertEqual('PARTIAL_RETRY_NEXT_INTERVAL',first['status'])
        self.assertEqual(50,len(set(attempted)))
        seen2=[]
        second=self.run_tick('t2',[],NOW+600,lambda c,j,**kw:(seen2.append(j['symbol']) or [],1))
        self.assertEqual(50,second['jobs_attempted'])
        self.assertTrue(set(attempted).isdisjoint(seen2))

    def test_failed_first_50_do_not_starve_rest(self):
        first=self.run_tick('t1',[row(i) for i in range(51)],collector=lambda *a,**k:(_ for _ in ()).throw(OSError('offline fixture')))
        self.assertEqual(50,first['jobs_failed'])
        first_ids={j['symbol'] for j in first['jobs']}
        second=self.run_tick('t2',[],NOW+600)
        self.assertNotIn(second['jobs'][0]['symbol'],first_ids)

    def test_no_data_and_incomplete_grid_remain_pending_until_complete(self):
        first=self.run_tick('t1',[row(0)])
        initial=first['jobs'][0]['proxy_ts']
        second=self.run_tick('t2',[row(0)],NOW+4*86400,lambda c,j,**k:(bars_for(j)[:-1],1))
        self.assertEqual(initial,second['jobs'][0]['proxy_ts'])
        self.assertFalse(second['jobs'][0]['coverage_complete'])
        third=self.run_tick('t3',[],NOW+4*86400+600)
        self.assertTrue(third['jobs'][0]['coverage_complete'])
        self.assertEqual(0,third['jobs_pending_retry'])
        state=json.loads((self.root/'state.json').read_text(encoding='utf-8'))
        self.assertEqual(0,state['backlog_pending_count'])
        self.assertFalse(state['cadence_observation']['candidate'])
        fourth=self.run_tick('t4',[row(0)],NOW+5*86400)
        self.assertEqual(0,fourth['jobs_attempted'])

    def test_queue_persistence_failure_stops_before_requests(self):
        self.assertTrue(hasattr(monitor,'Backlog'), 'monitor needs durable backlog integration')
        with mock.patch.object(monitor.Backlog,'admit',side_effect=OSError('disk fixture')):
            with mock.patch.object(monitor,'collect_window_bars') as collect:
                with self.assertRaisesRegex(OSError,'disk fixture'):
                    monitor.run_tick(self.plan,tick_id='t1',clients={},fetcher=lambda:([row(0)],1),now_ts=NOW)
                collect.assert_not_called()

    def test_discovery_error_preserves_and_services_existing_queue(self):
        self.run_tick('t1',[row(0)])
        with mock.patch.object(monitor,'collect_window_bars',side_effect=lambda c,j,**k:(bars_for(j),1)):
            result=monitor.run_tick(self.plan,tick_id='t2',clients={'binance':object()},
                fetcher=lambda:(_ for _ in ()).throw(OSError('discovery fixture')),now_ts=NOW+4*86400)
        self.assertEqual(1,result['jobs_attempted'])
        self.assertTrue(result['pending_retry'])
        self.assertIn('discovery fixture',result['discovery_error'])

    def test_hourly_coverage_rejects_gap_duplicate_and_future(self):
        self.assertTrue(hasattr(monitor,'_coverage_complete'), 'explicit coverage checker missing')
        j=dict(proxy_ts=NOW,window_end_ts=NOW+72*3600)
        full=bars_for(j)
        self.assertTrue(monitor._coverage_complete(j,full,NOW+73*3600))
        self.assertFalse(monitor._coverage_complete(j,full,NOW+3600))
        self.assertFalse(monitor._coverage_complete(j,full[:-1],NOW+73*3600))
        self.assertFalse(monitor._coverage_complete(j,full+full[:1],NOW+73*3600))
        self.assertFalse(monitor._coverage_complete(j,full[:10]+full[11:],NOW+73*3600))

    def test_state_keeps_distinct_instruments_and_uses_coverage_not_http_success(self):
        rows=[dict(row(i),base='SHARED') for i in range(2)]
        self.run_tick('t1',rows)
        result=self.run_tick('t2',rows,NOW+4*86400,lambda c,j,**k:(bars_for(j)[:-1],1))
        state=json.loads((self.root/'state.json').read_text(encoding='utf-8'))
        self.assertEqual(2,state['current_plan_window_count'])
        self.assertEqual(0,state['current_plan_complete_window_count'])
        self.assertEqual(2,state['backlog_pending_count'])
        self.assertTrue(state['cadence_observation']['candidate'])
        self.assertFalse(state['cadence_observation']['official_confirmed'])

    def test_finish_failure_keeps_claim_and_retry_evidence(self):
        self.run_tick('t1',[row(0)])
        with mock.patch.object(monitor.Backlog,'finish',side_effect=OSError('queue outcome fixture')):
            with mock.patch.object(monitor,'release_global_market_writer') as release:
                with self.assertRaisesRegex(OSError,'queue outcome fixture'):
                    self.run_tick('t2',[],NOW+4*86400)
                release.assert_not_called()
        from listing_expansion_backlog import Backlog
        self.assertEqual(1,len(Backlog(self.qpath,PLAN).pending(NOW+4*86400)))
        failed=json.loads((self.ticks/'t2'/'manifest.json').read_text(encoding='utf-8'))
        self.assertEqual('STOPPED_INCOMPLETE',failed['status'])
        self.assertEqual('backlog_outcome_persistence_exception',failed['stop_reason'])
        self.assertEqual(1,failed['backlog_pending_count'])

    def test_known_revision_crossing_baseline_is_quarantined_before_requests(self):
        self.plan['source_bindings']['preflight']['baseline_as_of_ts']=NOW-10*86400
        with mock.patch.object(monitor,'resolve_proxy_timestamp',return_value=(NOW-86400,'metadata_launch_time')):
            self.run_tick('t1',[row(0)])
        with mock.patch.object(monitor,'resolve_proxy_timestamp',return_value=(NOW-20*86400,'metadata_launch_time')):
            with mock.patch.object(monitor,'collect_window_bars') as collect:
                result=monitor.run_tick(self.plan,tick_id='t2',clients={},fetcher=lambda:([row(0),row(1)],1),now_ts=NOW+600)
                collect.assert_not_called()
        from listing_expansion_backlog import Backlog
        q=Backlog(self.qpath,PLAN)
        self.assertEqual(1,len(q.entries))
        self.assertEqual('QUARANTINED',next(iter(q.entries.values()))['status'])
        self.assertEqual(1,result['jobs_pending_retry'])

    def test_budget_deadline_retains_all_unattempted_and_1510_overflow_is_durable(self):
        with mock.patch.object(monitor.time,'monotonic',side_effect=[0.,601.]):
            result=self.run_tick('t1',[row(i) for i in range(1510)])
        self.assertEqual(0,result['jobs_attempted'])
        self.assertEqual(50,result['jobs_selected'])
        self.assertEqual(1510,result['jobs_pending_retry'])
        self.assertEqual('STOPPED_INCOMPLETE',result['status'])
        self.assertEqual(1510,len(json.loads((self.ticks/'t1'/'discovery.json').read_text(encoding='utf-8'))['jobs']))


if __name__ == '__main__':
    unittest.main()
