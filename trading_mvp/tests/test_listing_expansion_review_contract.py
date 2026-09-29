"""Fail-closed immutable offline plan and exclusive bundle publication tests."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
if importlib.util.find_spec('listing_expansion_review_contract'):
    import listing_expansion_review_contract as contract
else:
    contract = None


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def seal(plan):
    content = {k: v for k, v in plan.items() if k != 'plan_hash'}
    plan['plan_hash'] = digest(json.dumps(content, sort_keys=True, ensure_ascii=False,
                                         separators=(',', ':'), allow_nan=False).encode('utf-8'))
    return plan


class ReviewContractTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(contract, 'offline review contract implementation is missing')
        self.temp = tempfile.TemporaryDirectory(prefix='listing-review-contract-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source.json'
        self.source.write_bytes(b'{"source":"preserved"}\n')
        self.plan = dict(
            schema='listing_expansion_review_planonly_v1', plan_id='offline-review-test-v1',
            status='OFFLINE_READY_NOT_ACTIVATED', research_only=True, public_data_only=True,
            network_allowed=False, collector_allowed=False, scheduler_allowed=False,
            private_api=False, live_orders=False,
            policy=dict(retry_slots=10, max_proposals=50, aging_slots_per_venue=1,
                        official_future_horizon_sec=86400, official_active_after_sec=60,
                        recent_metadata_horizon_sec=259200, separate_lanes=True,
                        migration='LINKED_OVERLAY_NO_SOURCE_REWRITE', selection_not_execution=True),
            input_files=[self.binding('source', self.source)], implementation_files=[])
        implementation_names = {
            'review_contract': 'listing_expansion_review_contract.py',
            'review_queue': 'listing_expansion_review_queue.py',
            'evidence_validator': 'listing_expansion_identity_evidence.py',
            'materializer': 'listing_expansion_review_materializer.py',
            'backlog': 'listing_expansion_backlog.py',
        }
        for role, filename in implementation_names.items():
            path = Path(contract.__file__).parent / filename
            self.plan['implementation_files'].append(self.binding(role, path))
        seal(self.plan)

    def binding(self, role, path):
        return dict(role=role, path=str(path.resolve()), sha256=digest(path.read_bytes()))

    def test_validate_all_bytes_and_leave_inputs_and_plan_unchanged(self):
        before = copy.deepcopy(self.plan)
        verified = contract.validate_plan(self.plan)
        self.assertEqual(6, len(verified))
        self.assertEqual(self.source.read_bytes(), verified['source'])
        self.assertEqual(Path(contract.__file__).read_bytes(), verified['review_contract'])
        self.assertEqual(before, self.plan)
        self.assertFalse((self.root / 'output').exists())

    def test_plan_hash_excludes_only_top_level_plan_hash(self):
        self.plan['provenance'] = {'plan_hash': 'nested hash is significant'}
        seal(self.plan)
        contract.validate_plan(self.plan)
        self.plan['provenance']['plan_hash'] = 'changed'
        with self.assertRaisesRegex(ValueError, 'plan hash'):
            contract.validate_plan(self.plan)

    def test_unsafe_flags_or_wrong_schema_status_are_rejected_even_after_reseal(self):
        invalid = {'schema': 'live_v1', 'plan_id': '', 'status': 'ACTIVE',
                   'research_only': False, 'public_data_only': False,
                   'network_allowed': True, 'collector_allowed': True,
                   'scheduler_allowed': True, 'private_api': True, 'live_orders': True}
        for field, value in invalid.items():
            with self.subTest(field=field):
                changed = copy.deepcopy(self.plan)
                changed[field] = value
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))
        changed = copy.deepcopy(self.plan)
        changed['research_only'] = 1
        with self.assertRaises(ValueError):
            contract.validate_plan(seal(changed))

    def test_every_required_policy_value_is_exact_and_typed(self):
        for field, expected in self.plan['policy'].items():
            with self.subTest(field=field):
                changed = copy.deepcopy(self.plan)
                changed['policy'].pop(field)
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))
                changed = copy.deepcopy(self.plan)
                changed['policy'][field] = not expected if type(expected) is bool else 'changed'
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))
        changed = copy.deepcopy(self.plan)
        changed['policy']['aging_slots_per_venue'] = True
        with self.assertRaises(ValueError):
            contract.validate_plan(seal(changed))

    def test_binding_paths_and_hashes_and_roles_are_strict(self):
        for key, value in [('path', 'relative.json'), ('sha256', 'x'*64), ('role', '')]:
            with self.subTest(key=key):
                changed = copy.deepcopy(self.plan)
                changed['input_files'][0][key] = value
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))
        changed = copy.deepcopy(self.plan)
        changed['input_files'].append(copy.deepcopy(changed['implementation_files'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate role'):
            contract.validate_plan(seal(changed))

    def test_missing_implementation_role_or_different_contract_self_is_rejected(self):
        for role in ('review_contract', 'review_queue', 'evidence_validator', 'materializer', 'backlog'):
            with self.subTest(role=role):
                changed = copy.deepcopy(self.plan)
                changed['implementation_files'] = [r for r in changed['implementation_files'] if r['role'] != role]
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))
        changed = copy.deepcopy(self.plan)
        changed['implementation_files'][0] = self.binding('review_contract', self.source)
        with self.assertRaisesRegex(ValueError, 'implementation'):
            contract.validate_plan(seal(changed))

    def test_every_implementation_role_rejects_redirect_to_another_actual_module(self):
        originals = {row['role']: row for row in self.plan['implementation_files']}
        for index, row in enumerate(self.plan['implementation_files']):
            with self.subTest(role=row['role']):
                changed = copy.deepcopy(self.plan)
                replacement_role = ('review_queue' if row['role'] == 'evidence_validator'
                                    else 'evidence_validator')
                replacement = originals[replacement_role]
                changed['implementation_files'][index] = dict(replacement, role=row['role'])
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))

    def test_every_implementation_role_rejects_a_byte_identical_copy_elsewhere(self):
        for index, row in enumerate(self.plan['implementation_files']):
            with self.subTest(role=row['role']):
                copied = self.root / (row['role'] + '-identical.py')
                copied.write_bytes(Path(row['path']).read_bytes())
                self.assertEqual(row['sha256'], digest(copied.read_bytes()))
                changed = copy.deepcopy(self.plan)
                changed['implementation_files'][index] = self.binding(row['role'], copied)
                with self.assertRaises(ValueError):
                    contract.validate_plan(seal(changed))

    def test_unknown_implementation_role_and_duplicate_binding_are_rejected(self):
        changed = copy.deepcopy(self.plan)
        changed['implementation_files'].append(self.binding('not_imported_by_runtime', self.source))
        with self.assertRaisesRegex(ValueError, 'unknown implementation role'):
            contract.validate_plan(seal(changed))
        changed = copy.deepcopy(self.plan)
        changed['implementation_files'].append(copy.deepcopy(changed['implementation_files'][1]))
        with self.assertRaisesRegex(ValueError, 'duplicate role'):
            contract.validate_plan(seal(changed))

    def test_duplicate_implementation_paths_are_rejected(self):
        changed = copy.deepcopy(self.plan)
        changed['implementation_files'][2] = dict(changed['implementation_files'][1],
                                                 role='evidence_validator')
        with self.assertRaises(ValueError):
            contract.validate_plan(seal(changed))

    def test_mutated_source_or_implementation_is_rejected(self):
        self.source.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            contract.validate_plan(self.plan)

    def test_source_larger_than_32_mib_is_rejected(self):
        self.source.write_bytes(b'x' * (32*1024*1024+1))
        self.plan['input_files'][0] = self.binding('source', self.source)
        seal(self.plan)
        with self.assertRaisesRegex(ValueError, 'size limit'):
            contract.validate_plan(self.plan)

    def test_expected_plan_file_must_exist_and_match_passed_plan(self):
        plan_path = self.root / 'plan.json'
        plan_path.write_text(json.dumps(self.plan), encoding='utf-8')
        self.assertEqual(6, len(contract.validate_plan(self.plan, expected_plan_path=plan_path)))
        changed = copy.deepcopy(self.plan)
        changed['plan_id'] = 'different'
        with self.assertRaisesRegex(ValueError, 'plan file'):
            contract.validate_plan(seal(changed), expected_plan_path=plan_path)

    def test_plan_file_duplicate_keys_are_rejected(self):
        path = self.root / 'plan.json'
        raw = json.dumps(self.plan)
        path.write_text('{"schema":"duplicate",' + raw[1:], encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            contract.validate_plan(self.plan, expected_plan_path=path)

    def test_source_directory_is_not_a_file(self):
        self.plan['input_files'][0]['path'] = str(self.root)
        with self.assertRaises(ValueError):
            contract.validate_plan(seal(self.plan))

    def test_symlink_source_and_ancestor_are_rejected_if_available(self):
        linked = self.root / 'linked.json'
        try:
            linked.symlink_to(self.source)
        except (OSError, NotImplementedError):
            self.skipTest('symlinks not available for this Windows account')
        self.plan['input_files'][0]['path'] = str(linked)
        with self.assertRaisesRegex(ValueError, 'link|reparse'):
            contract.validate_plan(seal(self.plan))
        folder = self.root / 'folder'
        folder.mkdir()
        (folder / 'source.json').write_bytes(self.source.read_bytes())
        linked_dir = self.root / 'linked-dir'
        linked_dir.symlink_to(folder, target_is_directory=True)
        self.plan['input_files'][0]['path'] = str(linked_dir / 'source.json')
        with self.assertRaisesRegex(ValueError, 'link|reparse'):
            contract.validate_plan(seal(self.plan))

    def test_exclusive_new_bundle_writes_exact_bytes_and_hashes(self):
        output = self.root / 'new-output'
        files = {'report.json': b'{"proposal_only":true}\n', 'report.md': 'Очередь'.encode()}
        hashes = contract.write_new_bundle(output, files)
        self.assertEqual({name: digest(raw) for name, raw in files.items()}, hashes)
        self.assertEqual(files, {name: (output/name).read_bytes() for name in files})
        with self.assertRaises((ValueError, FileExistsError)):
            contract.write_new_bundle(output, {'other.json': b'x'})
        self.assertFalse((output/'other.json').exists())

    def test_terminal_manifest_is_written_last(self):
        output = self.root / 'manifest-last'
        write_order = []
        original_open = Path.open

        def recording_open(path, mode='r', *args, **kwargs):
            if mode == 'xb':
                write_order.append(path.name)
            return original_open(path, mode, *args, **kwargs)

        with mock.patch.object(Path, 'open', recording_open):
            contract.write_new_bundle(output, {'manifest.json': b'{}', 'queues.json': b'[]',
                                               'audit.json': b'{}'})
        self.assertEqual(['audit.json', 'queues.json', 'manifest.json'], write_order)

    def test_partial_write_retains_files_without_terminal_manifest(self):
        output = self.root / 'partial-output'
        original_open = Path.open

        def failing_open(path, mode='r', *args, **kwargs):
            if mode == 'xb' and path.name == 'queues.json':
                raise OSError('injected queues write failure')
            return original_open(path, mode, *args, **kwargs)

        with mock.patch.object(Path, 'open', failing_open):
            with self.assertRaisesRegex(OSError, 'injected'):
                contract.write_new_bundle(output, {'manifest.json': b'{}', 'queues.json': b'[]',
                                                   'audit.json': b'{}'})
        self.assertTrue((output/'audit.json').exists())
        self.assertFalse((output/'manifest.json').exists())
        with self.assertRaises(ValueError):
            contract.write_new_bundle(output, {'manifest.json': b'{}'})

    def test_invalid_names_or_values_fail_before_directory_creation(self):
        for index, name in enumerate(('../escape', 'sub/file', 'sub\\file', '/absolute', 'C:stream',
                                      '.', '..', 'CON', 'aux.json', 'COM¹.txt', 'LPT².txt',
                                      'file.', 'file ', 'nul.txt', 'a\x00b')):
            with self.subTest(name=name):
                output = self.root / f'invalid-output-{index}'
                with self.assertRaises(ValueError):
                    contract.write_new_bundle(output, {name: b'x'})
                self.assertFalse(output.exists())
        for index, files in enumerate(({}, {'a.json': 'not bytes'}, {'a.json': b'x', 'A.JSON': b'y'})):
            output = self.root / f'invalid-values-{index}'
            with self.assertRaises(ValueError):
                contract.write_new_bundle(output, files)
            self.assertFalse(output.exists())

    def test_output_parent_must_exist_and_output_must_be_absolute(self):
        with self.assertRaises(ValueError):
            contract.write_new_bundle(Path('relative-output'), {'report.json': b'x'})
        with self.assertRaises(ValueError):
            contract.write_new_bundle(self.root/'absent-parent'/'output', {'report.json': b'x'})
        self.assertFalse((self.root/'absent-parent').exists())

    def test_symlink_output_parent_is_rejected_if_available(self):
        real = self.root / 'real'
        real.mkdir()
        linked = self.root / 'linked'
        try:
            linked.symlink_to(real, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest('symlinks not available for this Windows account')
        with self.assertRaisesRegex(ValueError, 'link|reparse'):
            contract.write_new_bundle(linked/'output', {'report.json': b'x'})
        self.assertFalse((real/'output').exists())


if __name__ == '__main__':
    unittest.main()
