from __future__ import annotations

import hashlib
import json
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "trading_mvp" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import listing_expansion_automation_contract as wrapper_contract  # noqa: E402
import slow_liquidity_listing_momentum_forward_expansion_monitor as monitor  # noqa: E402
import slow_liquidity_listing_momentum_forward_expansion_plan as child_contract  # noqa: E402


class PlanSuccessorTests(unittest.TestCase):
    def _child_plan(self) -> dict:
        self.assertTrue(monitor.PLAN_PATH.is_file(), "new child immutable plan has not been prepared")
        return json.loads(monitor.PLAN_PATH.read_text(encoding="utf-8"))

    def test_child_identity_is_a_new_v18_artifact(self) -> None:
        self.assertEqual(
            "slow_liquidity_listing_momentum_forward_expansion_20260925_v18",
            monitor.PLAN_ID,
        )
        self.assertEqual(
            "slow-liquidity-listing-momentum-forward-expansion-planonly-20260925-v18.json",
            monitor.PLAN_PATH.name,
        )
        self.assertEqual(
            "slow-liquidity-listing-momentum-forward-expansion-planonly-20260828-v17.json",
            child_contract.PREVIOUS_EXPANSION_PLAN_PATH.name,
        )

    def test_child_contract_binds_the_reviewed_identity_receipt(self) -> None:
        receipt_path = REPO_ROOT / "docs" / "agent-log" / "bitget-reality-identity-review-20260828.json"
        expected_sha = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        self.assertEqual(receipt_path, child_contract.BITGET_REALITY_IDENTITY_RECEIPT_PATH)
        self.assertEqual(expected_sha, child_contract.BITGET_REALITY_IDENTITY_RECEIPT_FILE_SHA256)

        plan = self._child_plan()
        binding = plan["source_bindings"]["bitget_reality_identity_review"]
        self.assertEqual(str(receipt_path), binding["path"])
        self.assertEqual(expected_sha, binding["file_sha256"])
        self.assertEqual("REVIEWED_OFFICIAL_VENUE_METADATA", binding["status"])

    def test_wrapper_identity_and_both_launcher_defaults_move_together(self) -> None:
        self.assertEqual(
            "listing_momentum_expansion_automation_20260925_v13",
            wrapper_contract.PLAN_ID,
        )
        self.assertEqual(
            "docs/plans/listing-momentum-expansion-automation-planonly-20260925-v13.json",
            wrapper_contract.PLAN_RELATIVE_PATH,
        )
        self.assertEqual(
            "docs/plans/listing-momentum-expansion-automation-planonly-20260925-v12.json",
            wrapper_contract.PREVIOUS_PLAN_RELATIVE_PATH,
        )
        child_launcher = (REPO_ROOT / "tools" / "start_listing_momentum_forward_expansion_tick_visible.ps1").read_text(encoding="utf-8")
        wrapper_launcher = (REPO_ROOT / "tools" / "start_listing_momentum_expansion_automation_visible.ps1").read_text(encoding="utf-8")
        self.assertIn(monitor.PLAN_PATH.name, child_launcher)
        self.assertIn(Path(wrapper_contract.PLAN_RELATIVE_PATH).name, wrapper_launcher)

    def test_wrapper_successor_preserves_and_names_v12_lineage(self) -> None:
        previous_path = REPO_ROOT / "docs" / "plans" / "listing-momentum-expansion-automation-planonly-20260925-v12.json"
        previous = json.loads(previous_path.read_text(encoding="utf-8"))
        self.assertEqual(
            "06dfb0f6bd23b658a6f1e208a576447bc296ccf7ceb5bb5d21ee375394e3c997",
            hashlib.sha256(previous_path.read_bytes()).hexdigest(),
        )
        self.assertEqual("2150fdb52cc034afc1e4f75d530ce1a5bf039b9df8c931394faafc29ac8dbe52", previous["plan_hash"])

        successor_path = REPO_ROOT / wrapper_contract.PLAN_RELATIVE_PATH
        self.assertTrue(successor_path.is_file(), "new wrapper immutable plan has not been prepared")
        successor = json.loads(successor_path.read_text(encoding="utf-8"))
        lineage = successor["technical_rebind"]
        self.assertEqual(previous["plan_id"], lineage["supersedes_plan_id"])
        self.assertEqual(previous["plan_hash"], lineage["supersedes_plan_hash"])
        self.assertEqual(hashlib.sha256(previous_path.read_bytes()).hexdigest(), lineage["supersedes_plan_file_sha256"])
        self.assertEqual(str(previous_path), lineage["supersedes_plan_path"])

    def test_wrapper_v13_binds_current_bytes_and_new_child_without_activating(self) -> None:
        successor_path = REPO_ROOT / wrapper_contract.PLAN_RELATIVE_PATH
        self.assertEqual("listing-momentum-expansion-automation-planonly-20260925-v13.json", successor_path.name)
        self.assertTrue(successor_path.is_file(), "new wrapper immutable plan has not been prepared")
        successor = json.loads(successor_path.read_text(encoding="utf-8"))
        rebuilt = wrapper_contract.build_plan(
            REPO_ROOT, monitor.PLAN_PATH, generated_at_utc=successor["generated_at_utc"])
        self.assertEqual(rebuilt, successor)
        self.assertEqual(self._child_plan()["plan_hash"], successor["child_plan"]["plan_hash"])
        self.assertEqual("durable_discovery_backlog_and_bounded_atomic_replace", successor["technical_rebind"]["reason"])
        self.assertEqual("WITHDRAWN_UNTIL_REVIEWED", successor["technical_rebind"]["autopilot_authority"])

    def test_child_v17_and_wrapper_v12_are_bytewise_preserved(self) -> None:
        paths = {
            "slow-liquidity-listing-momentum-forward-expansion-planonly-20260828-v17.json": "8cfb126c3d8d7c4e2e0b44fe3a5c989b97d11fa27f1737ab2963960d59d3a855",
            "listing-momentum-expansion-automation-planonly-20260925-v12.json": "06dfb0f6bd23b658a6f1e208a576447bc296ccf7ceb5bb5d21ee375394e3c997",
        }
        for name, sha in paths.items():
            with self.subTest(name=name):
                self.assertEqual(sha, hashlib.sha256((REPO_ROOT / "docs/plans" / name).read_bytes()).hexdigest())

    def test_backlog_contract_and_path_are_exact_runtime_bindings(self) -> None:
        self.assertTrue(hasattr(monitor, "BACKLOG_CONTRACT"), "missing preregistered backlog protocol")
        plan = self._child_plan()
        self.assertEqual(monitor.BACKLOG_CONTRACT, plan["backlog_contract"])
        self.assertEqual(str(monitor.BACKLOG_PATH), plan["tick"]["backlog_path"])
        self.assertEqual(50, plan["tick"]["max_new_listings_per_tick"])
        self.assertEqual(600, plan["tick"]["max_runtime_sec"])
        roles = {row["role"] for row in plan["implementation"]["files"]}
        self.assertTrue({"expansion_backlog", "atomic_io"}.issubset(roles))
        child_contract.validate_plan(plan)
        for field, value in (("backlog_contract", {}), ("tick", {**plan["tick"], "backlog_path": str(REPO_ROOT / "other.jsonl")})):
            mutated = deepcopy(plan)
            mutated[field] = value
            mutated["plan_hash"] = child_contract.canonical_hash(mutated)
            with self.subTest(field=field), self.assertRaises(child_contract.ExpansionPlanError):
                child_contract.validate_plan(mutated)

    def test_queue_changes_are_declared_not_technical_only(self) -> None:
        self.assertIsInstance(child_contract.RESEARCH_SCOPE_CHANGE, dict)
        plan = self._child_plan()
        rebind = plan["source_bindings"]["technical_rebind"]
        self.assertIs(True, rebind["research_scope_changed"])
        self.assertEqual("WITHDRAWN_UNTIL_REVIEWED", rebind["research_scope_change"]["autopilot_authority"])
        self.assertEqual(child_contract.RESEARCH_SCOPE_CHANGE, rebind["research_scope_change"])
        altered = deepcopy(plan)
        altered["source_bindings"]["technical_rebind"]["research_scope_changed"] = False
        altered["source_bindings"]["technical_rebind"].pop("research_scope_change")
        altered["plan_hash"] = child_contract.canonical_hash(altered)
        with self.assertRaises(child_contract.ExpansionPlanError):
            child_contract.validate_plan(altered)

    def test_offline_wrapper_build_allows_uncommitted_predecessor_but_runtime_rejects_it(self) -> None:
        # Offline PlanOnly preparation is not the execution validator.
        self.assertEqual("listing_momentum_expansion_automation_20260925_v13", wrapper_contract.PLAN_ID)
        successor_path = REPO_ROOT / wrapper_contract.PLAN_RELATIVE_PATH
        self.assertTrue(successor_path.is_file(), "new wrapper immutable plan has not been prepared")
        successor = wrapper_contract.read_json(successor_path)
        with patch.object(wrapper_contract, "_committed_bytes", side_effect=AssertionError("offline build may not require commit")):
            self.assertEqual(successor, wrapper_contract.build_plan(REPO_ROOT, monitor.PLAN_PATH, generated_at_utc=successor["generated_at_utc"]))

        predecessor = REPO_ROOT / wrapper_contract.PREVIOUS_PLAN_RELATIVE_PATH
        def committed_bytes(root: Path, path: Path) -> bytes:
            if path == predecessor:
                raise wrapper_contract.AutomationContractError("predecessor is not committed")
            return path.read_bytes()
        with patch.object(wrapper_contract, "_committed_bytes", side_effect=committed_bytes), self.assertRaisesRegex(wrapper_contract.AutomationContractError, "predecessor is not committed"):
            wrapper_contract.validate_plan(successor_path)

    def test_offline_build_still_checks_predecessor_file_hash_and_canonical_hash(self) -> None:
        self.assertEqual("listing_momentum_expansion_automation_20260925_v13", wrapper_contract.PLAN_ID)
        self.assertTrue(monitor.PLAN_PATH.is_file(), "new child immutable plan has not been prepared")
        generated = "2026-09-25T23:00:00Z"
        with patch.object(wrapper_contract, "PREVIOUS_PLAN_FILE_SHA256", "0" * 64), self.assertRaisesRegex(wrapper_contract.AutomationContractError, "superseded wrapper plan file sha mismatch"):
            wrapper_contract.build_plan(REPO_ROOT, monitor.PLAN_PATH, generated_at_utc=generated)

        previous_path = REPO_ROOT / wrapper_contract.PREVIOUS_PLAN_RELATIVE_PATH
        previous = wrapper_contract.read_json(previous_path)
        previous["generated_at_utc"] = "2026-09-25T23:01:00Z"
        original_read = wrapper_contract.read_json
        def corrupt_canonical(path: Path) -> dict:
            return previous if path == previous_path else original_read(path)
        with patch.object(wrapper_contract, "read_json", side_effect=corrupt_canonical), self.assertRaisesRegex(wrapper_contract.AutomationContractError, "superseded wrapper plan hash mismatch"):
            wrapper_contract.build_plan(REPO_ROOT, monitor.PLAN_PATH, generated_at_utc=generated)


if __name__ == "__main__":
    unittest.main()
