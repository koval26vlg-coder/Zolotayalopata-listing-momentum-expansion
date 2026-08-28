from __future__ import annotations

import importlib
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
automation = importlib.import_module("listing_automation_state")


class ListingTerminalClaimCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="listing-terminal-claim-")
        self.addCleanup(self.temp.cleanup)
        self.clock = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)
        self.processes: dict[int, object] = {}
        self.paths = automation.AutomationPaths(Path(self.temp.name) / "automation")
        self.binding = automation.Binding("fixture_plan", "a" * 64, "b" * 64)
        self.engine = automation.AutomationEngine(
            self.paths,
            self.binding,
            now=lambda: self.clock,
            process_probe=lambda pid: self.processes.get(pid, automation.ProbeResult("DEAD")),
        )

    def identity(self, pid: int, offset: int = 0):
        return automation.ProcessIdentity(
            pid,
            (self.clock + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z"),
        )

    def begin_with_child(self, child_status: str):
        self.engine.initialize()
        attempt = self.engine.begin_attempt()
        worker = self.identity(101)
        child = self.identity(202)
        self.processes[worker.pid] = automation.ProbeResult("LIVE", worker)
        self.processes[child.pid] = automation.ProbeResult(child_status, child)
        self.engine.bind_worker(attempt["attempt_id"], attempt["handoff_token"], worker)
        self.engine.attach_child(attempt["attempt_id"], attempt["handoff_token"], child)
        return attempt

    def ledger_rows(self) -> list[dict[str, object]]:
        return [json.loads(line) for line in self.paths.ledger.read_text(encoding="utf-8").splitlines()]

    def test_dead_child_releases_terminal_claim_while_non_writer_wrapper_is_alive(self) -> None:
        attempt = self.begin_with_child("DEAD")
        self.clock += timedelta(minutes=1)

        result = self.engine.finish_attempt(
            attempt["attempt_id"],
            attempt["handoff_token"],
            outcome="COMPLETE",
            cadence_seconds=300,
        )

        self.assertEqual("COMPLETE", result["state_status"])
        self.assertFalse(result["pending_retry"])
        self.assertFalse(self.paths.claim.exists())
        archives = list(self.paths.claim_archive.glob("*.json"))
        self.assertEqual(1, len(archives))
        archived = json.loads(archives[0].read_text(encoding="utf-8"))
        self.assertEqual("TERMINAL_CHILD_EXITED", archived["disposition"])
        self.assertEqual(attempt["attempt_id"], archived["attempt_id"])
        state = json.loads(self.paths.state.read_text(encoding="utf-8"))
        self.assertEqual("COMPLETE", state["status"])
        self.assertEqual("2026-08-28T15:06:00.000000Z", state["next_interval_at_utc"])
        self.assertEqual(1, sum(row["kind"] == "TERMINAL" for row in self.ledger_rows()))

    def test_retry_retains_claim_while_attached_child_is_live_or_unknown(self) -> None:
        for child_status in ("LIVE", "UNKNOWN"):
            with self.subTest(child_status=child_status):
                with tempfile.TemporaryDirectory(prefix="listing-terminal-safety-") as root:
                    paths = automation.AutomationPaths(Path(root) / "automation")
                    processes: dict[int, object] = {}
                    engine = automation.AutomationEngine(
                        paths,
                        self.binding,
                        now=lambda: self.clock,
                        process_probe=lambda pid: processes.get(pid, automation.ProbeResult("DEAD")),
                    )
                    engine.initialize()
                    attempt = engine.begin_attempt()
                    worker = self.identity(301)
                    child = self.identity(302)
                    processes[worker.pid] = automation.ProbeResult("LIVE", worker)
                    processes[child.pid] = automation.ProbeResult(child_status, child)
                    engine.bind_worker(attempt["attempt_id"], attempt["handoff_token"], worker)
                    engine.attach_child(attempt["attempt_id"], attempt["handoff_token"], child)

                    engine.finish_attempt(
                        attempt["attempt_id"],
                        attempt["handoff_token"],
                        outcome="RETRY_NEXT_INTERVAL",
                        reason="fixture",
                    )

                    self.assertTrue(paths.claim.exists())
                    self.assertEqual([], list(paths.claim_archive.glob("*.json")))


if __name__ == "__main__":
    unittest.main()
