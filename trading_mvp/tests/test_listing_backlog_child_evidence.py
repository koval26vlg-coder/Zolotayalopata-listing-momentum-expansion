from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


TEST_ROOT = Path(__file__).resolve().parent
if str(TEST_ROOT) not in sys.path:
    sys.path.insert(0, str(TEST_ROOT))

from test_listing_expansion_child_evidence import ChildEvidenceFixture  # noqa: E402


class BacklogChildEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="backlog-child-evidence-")
        self.addCleanup(temporary.cleanup)
        self.fixture = ChildEvidenceFixture(Path(temporary.name))
        self.fixture.manifest["jobs"][0]["category"] = "new_listing_window_complete"
        self.pending = self.fixture.job("bybit", "DEFERRED", pending=True)
        self.pending["failure_reason"] = "not_attempted_tick_cap"
        self.fixture.manifest.update(
            status="PARTIAL_RETRY_NEXT_INTERVAL",
            stop_reason="backlog_pending",
            pending_retry=True,
            retry_disposition="RETRY_NEXT_INTERVAL",
            jobs_pending_retry=1,
            retry_queue=[self.pending],
            backlog_total=2,
            deferred_count=1,
        )
        self.fixture.write()

    def test_deferred_in_progress_job_is_candidate_without_being_attempted(self) -> None:
        result = self.fixture.call()
        self.assertEqual("PARTIAL_RETRY_NEXT_INTERVAL", result["status"])
        self.assertEqual(1, result["counts"]["jobs_attempted"])
        self.assertEqual([self.pending], result["pending_jobs"])
        self.assertTrue(result["cadence_observation"]["candidate"])

    def test_deferred_metadata_cannot_promote_official_cadence(self) -> None:
        self.pending.update(
            timestamp_source="official_announcement",
            official_confirmed=True,
            exact_timestamp=True,
            event_eta_utc="2026-08-26T02:00:00Z",
        )
        self.fixture.write()
        result = self.fixture.call()
        self.assertTrue(result["cadence_observation"]["candidate"])
        self.assertFalse(result["cadence_observation"]["official_confirmed"])
        self.assertFalse(result["cadence_observation"]["exact_timestamp"])
        self.assertEqual("proxy", result["cadence_observation"]["source_class"])
        self.assertTrue(result["cadence_observation"]["proxy_timestamp"])
        self.assertNotIn("event_eta_utc", result["cadence_observation"])

    def test_mature_pending_job_is_not_an_upcoming_candidate(self) -> None:
        self.pending["category"] = "new_listing_window_complete"
        self.fixture.write()
        result = self.fixture.call()
        self.assertEqual("PARTIAL_RETRY_NEXT_INTERVAL", result["status"])
        self.assertFalse(result["cadence_observation"]["candidate"])

    def test_missing_ledger_cannot_promote_pending_job_to_candidate(self) -> None:
        self.fixture.ledger_path.unlink()
        result = self.fixture.call()
        self.assertEqual("RETRY_NEXT_INTERVAL", result["status"])
        self.assertFalse(result["cadence_observation"]["candidate"])
        self.assertEqual({}, result["counts"])

    def test_total_must_still_equal_attempted_jobs_not_whole_backlog(self) -> None:
        self.fixture.manifest["jobs_total"] = 2
        self.fixture.write()
        result = self.fixture.call()
        self.assertEqual("RETRY_NEXT_INTERVAL", result["status"])
        self.assertFalse(result["cadence_observation"]["candidate"])

    def test_pending_count_mismatch_remains_fail_closed(self) -> None:
        self.fixture.manifest["jobs_pending_retry"] = 2
        self.fixture.write()
        result = self.fixture.call()
        self.assertEqual("RETRY_NEXT_INTERVAL", result["status"])
        self.assertFalse(result["cadence_observation"]["candidate"])


if __name__ == "__main__":
    unittest.main()
