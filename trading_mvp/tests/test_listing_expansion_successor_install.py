"""Versioned launch bindings must not redirect or rewrite their predecessors."""
import importlib.util
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'trading_mvp/src'))


class SuccessorInstallTests(unittest.TestCase):
    def test_versioned_wrapper_has_separate_identity_and_predecessor(self):
        self.assertIsNotNone(importlib.util.find_spec('listing_expansion_automation_contract_v14'))
        import listing_expansion_automation_contract_v14 as new
        import listing_expansion_automation_contract as old
        self.assertEqual('listing_momentum_expansion_automation_20260929_v14', new.PLAN_ID)
        self.assertEqual(old.PLAN_RELATIVE_PATH, new.PREVIOUS_PLAN_RELATIVE_PATH)
        self.assertEqual('490cbd087c51b2b3c88bc9d235dc6465cefbb9155b86d3df9ad2f827cfbc6ad9', new.PREVIOUS_PLAN_HASH)
        self.assertEqual('trading_mvp/src/listing_expansion_automation_v14.py', new.WRAPPER_ARTIFACTS['automation_runner'])
        self.assertEqual('listing_momentum_expansion_automation_20260925_v13', old.PLAN_ID)

    def test_versioned_launcher_defaults_and_runner_match(self):
        child = ROOT / 'tools/start_listing_momentum_forward_expansion_tick_v19_visible.ps1'
        wrapper = ROOT / 'tools/start_listing_momentum_expansion_automation_v14_visible.ps1'
        self.assertTrue(child.is_file())
        self.assertTrue(wrapper.is_file())
        self.assertIn('slow-liquidity-listing-momentum-forward-expansion-planonly-20260929-v19.json', child.read_text())
        self.assertIn('slow_liquidity_listing_momentum_forward_expansion_monitor_v19.py', child.read_text())
        self.assertIn('listing-momentum-expansion-automation-planonly-20260929-v14.json', wrapper.read_text())
        self.assertIn('listing_expansion_automation_v14.py', wrapper.read_text())

    def test_successor_builder_missing_seed_fails_without_output(self):
        self.assertIsNotNone(importlib.util.find_spec('slow_liquidity_listing_momentum_forward_expansion_plan_v19'))
        from slow_liquidity_listing_momentum_forward_expansion_plan_v19 import validate_queue_generation
        with self.assertRaises(ValueError):
            validate_queue_generation({'queue_generation': {}})

    def test_real_seed_correlation_is_read_only_and_preserves_all_source_entries(self):
        import slow_liquidity_listing_momentum_forward_expansion_plan_v19 as module
        original = module.SEED.read_bytes()
        generation = dict(seed_path=str(module.SEED), seed_sha256=hashlib.sha256(original).hexdigest(),
                          source_plan_hash=module.PREVIOUS_HASH, expected_source_events=2202)
        seed = module.validate_queue_generation({'queue_generation': generation})
        self.assertEqual(2202, len(seed['entries']))
        self.assertEqual(original, module.SEED.read_bytes())
        self.assertFalse((Path('E:/trading_mvp/listing-momentum-forward-expansion/backlog-v19/events.jsonl')).exists())

    def test_resealed_seed_cannot_change_source_outcome(self):
        import slow_liquidity_listing_momentum_forward_expansion_plan_v19 as module
        payload = json.loads(module.SEED.read_bytes())
        source_id = next(iter(payload['entries']))
        payload['entries'][source_id]['attempt_count'] += 1
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'seed.json'
            raw = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            path.write_bytes(raw)
            generation = dict(seed_path=str(path), seed_sha256=hashlib.sha256(raw).hexdigest(),
                              source_plan_hash=module.PREVIOUS_HASH, expected_source_events=2202)
            with mock.patch.object(module, 'SEED', path):
                with self.assertRaisesRegex(ValueError, 'seed lineage drift'):
                    module.validate_queue_generation({'queue_generation': generation})

    def test_new_artifact_publication_never_overwrites(self):
        import slow_liquidity_listing_momentum_forward_expansion_plan_v19 as module
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'new.json'
            module.publish_new(path, {'first': True})
            original = path.read_bytes()
            with self.assertRaisesRegex(ValueError, 'already exists'):
                module.publish_new(path, {'second': True})
            self.assertEqual(original, path.read_bytes())

    def test_self_resealed_review_manifest_cannot_authorize_changed_overlay(self):
        import slow_liquidity_listing_momentum_forward_expansion_plan_v19 as module
        seed = json.loads(module.SEED.read_bytes())
        review, source, _, _ = module._review_inputs(module.REVIEW)
        bindings = copy.deepcopy(seed['review_bundle'])
        overlay = json.loads(Path(bindings['identity_overlay']['path']).read_bytes())
        next(iter(overlay.values()))['unreviewed_field'] = 'injected'
        with tempfile.TemporaryDirectory() as folder:
            overlay_path = Path(folder) / 'identity_overlay.json'
            overlay_path.write_text(json.dumps(overlay), encoding='utf-8')
            bindings['identity_overlay'] = module._binding(overlay_path)
            manifest = json.loads(Path(bindings['manifest']['path']).read_bytes())
            manifest['output_sha256']['identity_overlay.json'] = bindings['identity_overlay']['sha256']
            manifest_path = Path(folder) / 'manifest.json'
            manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
            bindings['manifest'] = module._binding(manifest_path)
            with self.assertRaisesRegex(ValueError, 'reviewed bundle'):
                module._review_bundle(bindings, review, source)


if __name__ == '__main__':
    unittest.main()
