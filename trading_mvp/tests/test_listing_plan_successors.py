from __future__ import annotations

import hashlib
import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "trading_mvp" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import listing_expansion_automation_contract as wrapper_contract  # noqa: E402
import slow_liquidity_listing_momentum_forward_expansion_monitor as monitor  # noqa: E402
import slow_liquidity_listing_momentum_forward_expansion_plan as child_contract  # noqa: E402


class PlanSuccessorTests(unittest.TestCase):
    def test_child_identity_is_a_new_v17_artifact(self) -> None:
        self.assertEqual(
            "slow_liquidity_listing_momentum_forward_expansion_20260828_v17",
            monitor.PLAN_ID,
        )
        self.assertEqual(
            "slow-liquidity-listing-momentum-forward-expansion-planonly-20260828-v17.json",
            monitor.PLAN_PATH.name,
        )
        self.assertEqual(
            "slow-liquidity-listing-momentum-forward-expansion-planonly-20260827-v16.json",
            child_contract.PREVIOUS_EXPANSION_PLAN_PATH.name,
        )

    def test_child_contract_binds_the_reviewed_identity_receipt(self) -> None:
        receipt_path = REPO_ROOT / "docs" / "agent-log" / "bitget-reality-identity-review-20260828.json"
        expected_sha = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        self.assertEqual(receipt_path, child_contract.BITGET_REALITY_IDENTITY_RECEIPT_PATH)
        self.assertEqual(expected_sha, child_contract.BITGET_REALITY_IDENTITY_RECEIPT_FILE_SHA256)

        plan = json.loads(monitor.PLAN_PATH.read_text(encoding="utf-8"))
        binding = plan["source_bindings"]["bitget_reality_identity_review"]
        self.assertEqual(str(receipt_path), binding["path"])
        self.assertEqual(expected_sha, binding["file_sha256"])
        self.assertEqual("REVIEWED_OFFICIAL_VENUE_METADATA", binding["status"])

    def test_wrapper_identity_and_both_launcher_defaults_move_together(self) -> None:
        self.assertEqual(
            "listing_momentum_expansion_automation_20260828_v10",
            wrapper_contract.PLAN_ID,
        )
        self.assertEqual(
            "docs/plans/listing-momentum-expansion-automation-planonly-20260828-v10.json",
            wrapper_contract.PLAN_RELATIVE_PATH,
        )
        child_launcher = (REPO_ROOT / "tools" / "start_listing_momentum_forward_expansion_tick_visible.ps1").read_text(encoding="utf-8")
        wrapper_launcher = (REPO_ROOT / "tools" / "start_listing_momentum_expansion_automation_visible.ps1").read_text(encoding="utf-8")
        self.assertIn(monitor.PLAN_PATH.name, child_launcher)
        self.assertIn(Path(wrapper_contract.PLAN_RELATIVE_PATH).name, wrapper_launcher)


if __name__ == "__main__":
    unittest.main()
