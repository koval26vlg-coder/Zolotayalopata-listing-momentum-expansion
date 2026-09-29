"""Successor generation tests: all writes are isolated temporary fixtures."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import listing_expansion_backlog as old
if importlib.util.find_spec('listing_expansion_generation'):
    import listing_expansion_generation as generation
else:
    generation = None

NOW = 1800000000
SOURCE_PLAN = 'a' * 64
PLAN = 'b' * 64


class GenerationBacklogTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(generation, 'reviewed generation backlog is missing')
        self.temp = tempfile.TemporaryDirectory(prefix='expansion-generation-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = old.Backlog(self.root / 'source.jsonl', SOURCE_PLAN)
        self.raw = self.root / 'identity.raw.json'
        self.raw.write_bytes(b'{"official_fixture": true}')
        self.path = self.root / 'new' / 'events.jsonl'

    def job(self, index, venue='bitget'):
        return dict(exchange=venue, symbol=f'T{index}USDT', base=f'T{index}',
                    proxy_ts=NOW-100, timestamp_source='launchTime_ms',
                    probe_start_ts=NOW-3700, window_end_ts=NOW+72*3600,
                    category='new_listing_in_progress', asset_class='unknown',
                    asset_class_source='unclassified', asset_class_acceptance_eligible=False)

    def seed(self, count=3, classes=None, statuses=None):
        snapshot = self.root / 'old-discovery.json'
        snapshot.write_text(json.dumps(dict(plan_hash=SOURCE_PLAN, tick_id='old',
            now_ts=NOW-100, jobs=[self.job(i) for i in range(count)])), encoding='utf-8')
        self.source.admit(snapshot)
        entries = copy.deepcopy(self.source.entries)
        overlay = {}
        for index, entry in enumerate(sorted(entries.values(), key=lambda e:e['job']['symbol'])):
            if statuses and index in statuses:
                entry.update(status=statuses[index], attempt_count=2,
                             last_attempt_seq=index+1, next_attempt_after_ts=NOW-1)
            asset = (classes or {}).get(index, 'crypto_token')
            if asset is not None:
                overlay[entry['event_id']] = dict(status='VERIFIED',
                    evidence_class='OFFICIAL_VENUE_IDENTITY', source_storage='raw_http_response',
                    exchange=entry['job']['exchange'], symbol=entry['job']['symbol'], base=entry['job']['base'],
                    asset_class=asset, source_path=str(self.raw.resolve()),
                    source_sha256=hashlib.sha256(self.raw.read_bytes()).hexdigest(),
                    source_url='https://api.bitget.com/api/v3/market/instruments?category=SPOT',
                    received_ts=NOW-10)
        self.seed_payload = dict(schema='expansion_reviewed_lineage_seed_v1',
            source_plan_hash=SOURCE_PLAN, source_ledger_sha256=hashlib.sha256(self.source.raw).hexdigest(),
            source_seq=max(count+1, self.source.seq), entries=entries, identity_overlay=overlay,
            review_plan=dict(path=str((self.root/'review.json').resolve()),sha256='c'*64,plan_hash='d'*64))
        return self.reseal()

    def reseal(self):
        self.seed_path = self.root / 'seed.json'
        self.seed_path.write_text(json.dumps(self.seed_payload), encoding='utf-8')
        self.kwargs = dict(seed_path=self.seed_path,
            seed_sha256=hashlib.sha256(self.seed_path.read_bytes()).hexdigest(),
            source_plan_hash=SOURCE_PLAN, expected_source_events=len(self.seed_payload['entries']))
        return generation.GenerationBacklog(self.path, PLAN, **self.kwargs)

    def admit(self, q, jobs=None, tick='new', now=NOW):
        snapshot = self.root / f'{tick}.discovery.json'
        snapshot.write_text(json.dumps(dict(plan_hash=PLAN,tick_id=tick,now_ts=now,
            jobs=jobs or [])),encoding='utf-8')
        q.admit(snapshot)

    def test_constructor_projects_without_writing_and_preserves_full_lineage(self):
        q = self.seed()
        self.assertFalse(self.path.exists())
        self.assertEqual(0,q.seq)
        self.assertEqual(3,len(q.entries))
        for entry in q.entries.values():
            source = self.seed_payload['entries'][entry['source_event_id']]
            self.assertEqual(source, entry['source_entry'])
            self.assertEqual(source['job'], entry['job'])
            self.assertNotEqual(source['event_id'],entry['event_id'])
            self.assertEqual(SOURCE_PLAN,entry['source_plan_hash'])
            self.assertEqual(source['status'],entry['status'])

    def test_first_admit_records_single_import_and_reload_is_deterministic(self):
        q = self.seed()
        original = self.source.path.read_bytes()
        self.admit(q, [self.job(0)])
        self.assertEqual(['LINEAGE_IMPORT','DISCOVERY'],[json.loads(line)['kind'] for line in self.path.read_text().splitlines()])
        restored = generation.GenerationBacklog(self.path,PLAN,**self.kwargs)
        self.assertEqual(q.entries,restored.entries)
        self.admit(restored,[self.job(1)],tick='next')
        self.assertEqual(1,sum(json.loads(line)['kind']=='LINEAGE_IMPORT' for line in self.path.read_text().splitlines()))
        self.assertEqual(original,self.source.path.read_bytes())

    def test_only_raw_verified_crypto_acquired_others_separate(self):
        q=self.seed(5,classes={1:'tokenized_equity',2:None})
        rows=sorted(self.seed_payload['identity_overlay'].values(),key=lambda e:e['symbol'])
        rows[-1]['source_storage']='reviewed_web_extract'
        q=self.reseal()
        status=q.review_status(NOW)
        self.assertEqual({'identity_review':1,'crypto_descriptive':3,'equity_descriptive':1},status['lane_counts'])
        self.assertEqual(2,len(q.select(NOW,50)))
        self.assertEqual(2,len(q.pending(NOW)))
        self.assertEqual(3,status['held_count'])

    def test_new_unreviewed_discovery_is_held_even_with_legacy_crypto_label(self):
        q=self.seed(1)
        newjob=self.job(9)
        newjob.update(asset_class='crypto_token',asset_class_source='declared_spot_asset_registry_v1',asset_class_acceptance_eligible=True)
        self.admit(q,[newjob])
        self.assertEqual(1,len(q.select(NOW,50)))
        self.assertEqual(2,len(q.entries))
        self.assertEqual(1,q.review_status(NOW)['held_count'])

    def test_source_terminal_outcome_and_due_are_retained(self):
        q=self.seed(3,statuses={0:'COLLECTED_DESCRIPTIVE',1:'QUARANTINED',2:'RETRY'})
        entry=next(e for e in self.seed_payload['entries'].values() if e['job']['base']=='T2')
        entry['next_attempt_after_ts']=NOW+600
        entry['last_outcome']={'complete':False,'evidence':{'sha256':'f'*64,'path':'fixture'}}
        q=self.reseal()
        self.assertEqual([],q.select(NOW,50))
        self.assertEqual(1,len(q.pending(NOW)))
        self.assertEqual(1,len(q.select(NOW+600,50)))
        migrated=next(e for e in q.entries.values() if e['job']['base']=='T2')
        self.assertEqual(entry['last_outcome'],migrated['last_outcome'])
        self.assertEqual(2,migrated['attempt_count'])

    def test_retry_cap_and_new_attempt_fairness_survive_generation_boundary(self):
        q=self.seed(25,statuses={i:'RETRY' for i in range(25)})
        self.admit(q)
        first=q.select(NOW,50)
        self.assertEqual(10,len(first))
        for job in first:q.start(job['event_id'],'new',NOW)
        second=q.select(NOW+600,50)
        self.assertEqual(10,len(second))
        self.assertTrue({j['event_id'] for j in first}.isdisjoint(j['event_id'] for j in second))

    def test_job_enrichment_cannot_rebase_or_grant_acceptance(self):
        q=self.seed(1)
        proof=next(iter(self.seed_payload['identity_overlay'].values()))
        proof['official_spot_event']=dict(proof,evidence_class='OFFICIAL_ANNOUNCEMENT',official_spot_t0=NOW+3600)
        q=self.reseal()
        job=q.select(NOW,50)[0]
        self.assertEqual(NOW-100,job['proxy_ts'])
        self.assertEqual(NOW+3600,job['official_spot_t0'])
        self.assertFalse(job['asset_class_acceptance_eligible'])
        self.assertIn('identity_evidence',job)
        self.assertIn(job['source_event_id'],self.seed_payload['entries'])
        self.assertEqual('unknown',next(iter(q.entries.values()))['job']['asset_class'])

    def test_changed_seed_or_raw_proof_fails_before_new_generation_write(self):
        q=self.seed()
        self.seed_path.write_bytes(self.seed_path.read_bytes()+b' ')
        with self.assertRaisesRegex(ValueError,'seed.*hash'):
            generation.GenerationBacklog(self.path,PLAN,**self.kwargs)
        self.assertFalse(self.path.exists())
        q=self.reseal()
        self.raw.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'evidence|source'):
            generation.GenerationBacklog(self.path,PLAN,**self.kwargs)
        self.assertFalse(self.path.exists())

    def test_malformed_source_identity_overlay_or_duplicate_instrument_rejected(self):
        self.seed()
        baseline=copy.deepcopy(self.seed_payload)
        for defect in ('wrong_source_plan','wrong_id','unknown_overlay','duplicate_instrument'):
            with self.subTest(defect=defect):
                self.seed_payload=copy.deepcopy(baseline)
                first,second=list(self.seed_payload['entries'])[:2]
                if defect=='wrong_source_plan':self.seed_payload['source_plan_hash']='f'*64
                if defect=='wrong_id':self.seed_payload['entries'][first]['event_id']='f'*64
                if defect=='unknown_overlay':self.seed_payload['identity_overlay']['f'*64]={}
                if defect=='duplicate_instrument':self.seed_payload['entries'][second]['job']=copy.deepcopy(self.seed_payload['entries'][first]['job'])
                with self.assertRaises(ValueError):self.reseal()
        self.assertFalse(self.path.exists())

    def test_metadata_revision_quarantines_and_never_changes_source_copy(self):
        q=self.seed(1)
        changed=self.job(0)
        changed['proxy_ts']+=3600
        self.admit(q,[changed])
        self.assertEqual([],q.select(NOW,50))
        self.assertEqual([],q.pending(NOW))
        entry=next(iter(q.entries.values()))
        self.assertEqual('QUARANTINED',entry['status'])
        self.assertEqual(NOW-100,entry['source_entry']['job']['proxy_ts'])

    def test_unclassified_rediscovery_is_not_false_identity_revision(self):
        q=self.seed(1)
        observed=self.job(0)
        observed.update(asset_class='unclassified',
                        asset_class_source='pending_positive_venue_identity_review',
                        asset_class_acceptance_eligible=False)
        self.admit(q,[observed])
        self.assertEqual(1,len(q.select(NOW,50)))
        entry=next(iter(q.entries.values()))
        self.assertEqual('PENDING',entry['status'])
        self.assertEqual('unknown',entry['job']['asset_class'])
        stored=json.loads(self.path.read_text().splitlines()[1])
        self.assertEqual('unclassified',stored['payload']['jobs'][0]['asset_class'])

    def test_direct_start_cannot_bypass_crypto_review_gate(self):
        q=self.seed(2,classes={0:'tokenized_equity',1:None})
        self.admit(q)
        before=self.path.read_bytes()
        for event_id in q.entries:
            with self.assertRaisesRegex(ValueError,'review|acquisition'):
                q.start(event_id,'new',NOW)
        self.assertEqual(before,self.path.read_bytes())

    def test_import_receipt_tampering_and_stale_second_writer_fail_closed(self):
        q=self.seed(1)
        stale=generation.GenerationBacklog(self.path,PLAN,**self.kwargs)
        self.admit(q)
        with self.assertRaisesRegex(ValueError,'changed'):
            self.admit(stale,tick='stale')
        rows=self.path.read_text().splitlines()
        receipt=json.loads(rows[0])
        receipt['payload']['source_events']=999
        receipt.pop('record_hash')
        receipt['record_hash']=old._sha(old._bytes(receipt))
        self.path.write_text(json.dumps(receipt)+'\n',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'import binding'):
            generation.GenerationBacklog(self.path,PLAN,**self.kwargs)

    def test_status_projects_outcomes_explicitly_without_mutating_queue(self):
        q=self.seed(2)
        completed=q.select(NOW,50)[0]['event_id']
        before=copy.deepcopy(q.entries)
        status=q.review_status(NOW,completed_ids=[completed])
        self.assertEqual(1,status['actionable_pending_count'])
        self.assertEqual(1,status['terminal_count'])
        self.assertTrue(status['projected_after_outcome_commit'])
        self.assertEqual(1,status['completion_commit_pending_count'])
        self.assertEqual(before,q.entries)
        self.assertEqual(2,q.review_status(NOW)['actionable_pending_count'])
        with self.assertRaisesRegex(ValueError,'completed'):
            q.review_status(NOW,completed_ids=['f'*64])

    def test_new_outcome_is_durable_without_altering_original_source_outcome(self):
        q=self.seed(1,statuses={0:'RETRY'})
        source_copy=copy.deepcopy(next(iter(self.seed_payload['entries'].values())))
        self.admit(q)
        selected=q.select(NOW,50)[0]
        q.start(selected['event_id'],'new',NOW)
        raw=self.root/'ohlcv.jsonl'
        raw.write_bytes(b'{"test":true}\n')
        receipt=self.root/'outcomes.json'
        receipt.write_text(json.dumps(dict(plan_hash=PLAN,tick_id='new',
            raw_evidence=dict(path=str(raw.resolve()),sha256=hashlib.sha256(raw.read_bytes()).hexdigest()),
            jobs=[dict(event_id=selected['event_id'],coverage_complete=True)])),encoding='utf-8')
        q.finish(receipt,NOW+73*3600)
        reopened=generation.GenerationBacklog(self.path,PLAN,**self.kwargs)
        self.assertEqual([],reopened.pending(NOW+73*3600))
        entry=reopened.entries[selected['event_id']]
        self.assertEqual('COLLECTED_DESCRIPTIVE',entry['status'])
        self.assertTrue(entry['last_outcome']['complete'])
        self.assertEqual(source_copy,entry['source_entry'])
        self.assertEqual('RETRY',entry['source_entry']['status'])

    def test_generation_tail_and_source_binding_mismatch_never_repaired(self):
        q=self.seed(1)
        self.admit(q)
        original=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'binding'):
            generation.GenerationBacklog(self.path,'e'*64,**self.kwargs)
        with self.path.open('ab') as handle:handle.write(b'{')
        with self.assertRaisesRegex(ValueError,'tail'):
            generation.GenerationBacklog(self.path,PLAN,**self.kwargs)
        self.assertEqual(original+b'{',self.path.read_bytes())


if __name__=='__main__':unittest.main()
