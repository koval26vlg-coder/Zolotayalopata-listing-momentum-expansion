from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    import listing_expansion_child_evidence as evidence
except ImportError:  # RED: the pure evidence adapter follows these contract tests.
    evidence = None


class ChildEvidenceFixture:
    def __init__(self, root: Path) -> None:
        self.repo = root / "repo"
        self.repo.mkdir()
        self.tick_root = root / "ticks"
        self.tick_id = "expansion_tick_20260826T0100011234567Z"
        self.started = "2026-08-26T01:00:00.100000Z"
        self.finished = "2026-08-26T01:00:05.500000Z"
        self.identity = {"pid": 4242, "started_at_utc": "2026-08-26T01:00:00.200000Z"}
        self.ledger_path = self.repo / "docs/agent-log/run-gates/listing_momentum_forward_expansion_terminal_attempts.jsonl"
        self.plan = {
            "plan_id": "slow_liquidity_listing_momentum_forward_expansion_20260826_v11",
            "plan_hash": "a" * 64,
            "tick": {
                "tick_output_root": str(self.tick_root),
                "terminal_attempts_ledger_path": str(self.ledger_path),
                "claim_path": str(self.repo / "docs/agent-log/active-market-data-writer-claim.json"),
            },
        }
        self.manifest_path = self.tick_root / self.tick_id / "manifest.json"
        self.handoff_dir = self.repo / "docs/agent-log/run-gates/python-worker-handoffs"
        self.receipt_path = self.handoff_dir / "consumed" / (self.tick_id + ".20260826T010001300000Z.json")
        self.manifest = {
            "schema": "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_tick_manifest_v1",
            "tick_id": self.tick_id,
            "status": "COMPLETED",
            "stop_reason": "completed",
            "retry_disposition": None,
            "pending_retry": False,
            "started_at_utc": "2026-08-26T01:00:01Z",
            "finished_at_utc": "2026-08-26T01:00:04Z",
            "plan_hash": self.plan["plan_hash"],
            "writer_pid": 4243,
            "writer_process_started_at_utc": "2026-08-26T01:00:01.250000Z",
            "now_ts": 1787706001,
            "baseline_as_of_ts": 1786969700,
            "new_listing_count": 1,
            "skipped_backfill_or_relist": [],
            "jobs_total": 1,
            "jobs_attempted": 1,
            "jobs_succeeded": 1,
            "jobs_failed": 0,
            "jobs_pending_retry": 0,
            "jobs": [self.job("okx", "EXAMPLE")],
            "retry_queue": [],
            "rows_written": 72,
            "requests_made": 5,
        }
        # The actual consume function moves ISSUED bytes unchanged into consumed/.
        self.handoff = {
            "schema": "trading_mvp_market_data_worker_handoff_v1",
            "status": "ISSUED",
            "project": "trading_mvp",
            "automation_id": "zolotyaylopata-listing-momentum-forward-expansion",
            "attempt_id": self.tick_id,
            "plan_hash": self.plan["plan_hash"],
            "wrapper_pid": self.identity["pid"],
            "wrapper_process_started_at_utc": self.identity["started_at_utc"],
            "handoff_token_sha256": "b" * 64,
            "claim_run_id": self.plan["plan_id"] + "__" + self.tick_id,
            "claim_owner_kind": "listing_momentum_forward_expansion_monitor_tick",
            "claim_owner_pid": None,
            "claim_owner_process_started_at_utc": None,
            "claim_ownership_token_sha256": "c" * 64,
            "claim_output_namespace": str(self.manifest_path.parent),
            "claim_must_exist": False,
            "issued_at_utc": "2026-08-26T01:00:01.200000Z",
        }
        self.write()

    def ledger_row(self) -> dict:
        return {
            "schema": "trading_mvp_listing_momentum_forward_expansion_terminal_attempt_v1",
            "tick_id": self.tick_id,
            "attempt_id": self.tick_id,
            "run_id": self.plan["plan_id"] + "__" + self.tick_id,
            "plan_id": self.plan["plan_id"],
            "plan_hash": self.plan["plan_hash"],
            "owner_pid": self.manifest["writer_pid"],
            "owner_process_started_at_utc": self.manifest["writer_process_started_at_utc"],
            "ownership_token_sha256": self.handoff["claim_ownership_token_sha256"],
            "claim_path": self.plan["tick"]["claim_path"],
            "output_namespace": str(self.manifest_path.parent),
            "started_at_utc": self.manifest["started_at_utc"],
            "finished_at_utc": self.manifest["finished_at_utc"],
            "status": self.manifest["status"],
            "stop_reason": self.manifest["stop_reason"],
            "pending_retry": self.manifest["pending_retry"],
            "retry_disposition": self.manifest["retry_disposition"],
            "manifest_path": str(self.manifest_path),
            "manifest_sha256": hashlib.sha256(self.manifest_path.read_bytes()).hexdigest(),
            "manifest_status": self.manifest["status"],
            "manifest_unavailable_reason": None,
            "primary_error": None,
            "finalization_error": None,
        }

    @staticmethod
    def job(exchange: str, base: str, *, pending: bool = False) -> dict:
        job = {
            "exchange": exchange,
            "base": base,
            "symbol": base + "USDT",
            "proxy_ts": 1787706001,
            "timestamp_source": "listTime_ms",
            "asset_class": "unclassified",
            "asset_class_source": "unclassified_no_positive_identity",
            "asset_class_acceptance_eligible": False,
            "category": "new_listing_in_progress",
            "flags": ["window_in_progress"],
            "requests": 1,
            "job_status": "SUCCEEDED",
        }
        if pending:
            job.update(
                failure_reason="request_error",
                retry_disposition="RETRY_NEXT_INTERVAL",
                error="fixture network failure",
                job_status="FAILED_RETRY_NEXT_INTERVAL",
            )
        return job

    def write(self) -> None:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.receipt_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(json.dumps(self.manifest) + "\n", encoding="utf-8")
        self.receipt_path.write_text(json.dumps(self.handoff) + "\n", encoding="utf-8")
        self.ledger_path.write_text(json.dumps(self.ledger_row()) + "\n", encoding="utf-8")

    def terminal(self) -> dict:
        return {
            "status": self.manifest["status"],
            "tick_id": self.tick_id,
            "new_listing_count": self.manifest["new_listing_count"],
            "rows_written": self.manifest["rows_written"],
            "state": {
                "status": "ACCRUING",
                "tick_count": 99,
                "complete_window_count": 30,
                "adaptive_cadence": {"stage": "SCHEDULED", "interval_sec": 300},
            },
        }

    def stdout(self) -> str:
        return "=== visible child ===\nplan_hash: " + self.plan["plan_hash"] + "\n" + json.dumps(self.terminal()) + "\ntick exit code: 0\n"

    def call(self, **overrides) -> dict:
        arguments = dict(
            stdout_text=self.stdout(),
            exit_code=0,
            child_plan=self.plan,
            child_identity=self.identity,
            started_at_utc=self.started,
            finished_at_utc=self.finished,
            repo_root=self.repo,
        )
        arguments.update(overrides)
        return evidence.verify_child_outcome(**arguments)


class ChildOutcomeEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertIsNotNone(evidence, "pure child evidence adapter is not implemented")
        self.temporary = tempfile.TemporaryDirectory(prefix="listing-child-evidence-")
        self.addCleanup(self.temporary.cleanup)
        self.fixture = ChildEvidenceFixture(Path(self.temporary.name))

    def assert_retry(self, result: dict) -> None:
        self.assertEqual("RETRY_NEXT_INTERVAL", result["status"])
        self.assertTrue(result["reason"])
        self.assertFalse(result["cadence_observation"]["official_confirmed"])
        self.assertFalse(result["cadence_observation"]["exact_timestamp"])

    def test_complete_requires_real_issued_consumed_receipt_and_manifest(self) -> None:
        result = self.fixture.call()
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(self.fixture.tick_id, result["child_tick_id"])
        self.assertEqual(str(self.fixture.manifest_path), result["child_manifest_path"])
        self.assertEqual(hashlib.sha256(self.fixture.manifest_path.read_bytes()).hexdigest(), result["child_manifest_sha256"])
        self.assertEqual(1, result["counts"]["jobs_succeeded"])
        self.assertEqual([], result["pending_jobs"])

    def test_complete_zero_jobs_is_successful_discovery_not_trading_acceptance(self) -> None:
        for key in ("new_listing_count", "jobs_total", "jobs_attempted", "jobs_succeeded", "rows_written"):
            self.fixture.manifest[key] = 0
        self.fixture.manifest["jobs"] = []
        self.fixture.write()
        result = self.fixture.call()
        self.assertEqual("COMPLETE", result["status"])
        self.assertFalse(result["cadence_observation"]["candidate"])

    def test_partial_requires_successful_jobs_and_real_retry_queue(self) -> None:
        pending = self.fixture.job("bybit", "DELAYED", pending=True)
        self.fixture.manifest.update(
            status="PARTIAL_RETRY_NEXT_INTERVAL", stop_reason="partial_job_request_error",
            pending_retry=True, retry_disposition="RETRY_NEXT_INTERVAL",
            new_listing_count=2, jobs_total=2, jobs_attempted=2,
            jobs_failed=1, jobs_pending_retry=1,
            jobs=self.fixture.manifest["jobs"] + [pending], retry_queue=[pending],
        )
        self.fixture.write()
        result = self.fixture.call(exit_code=1)
        self.assertEqual("PARTIAL_RETRY_NEXT_INTERVAL", result["status"])
        self.assertEqual([pending], result["pending_jobs"])

    def test_zero_success_is_retry_never_partial(self) -> None:
        pending = self.fixture.job("bybit", "DELAYED", pending=True)
        self.fixture.manifest.update(
            status="PARTIAL_RETRY_NEXT_INTERVAL", pending_retry=True,
            retry_disposition="RETRY_NEXT_INTERVAL", jobs_succeeded=0,
            jobs_failed=1, jobs_pending_retry=1, jobs=[pending], retry_queue=[pending],
        )
        self.fixture.write()
        self.assert_retry(self.fixture.call(exit_code=1))

    def test_partial_without_retry_queue_is_rejected(self) -> None:
        self.fixture.manifest.update(status="PARTIAL_RETRY_NEXT_INTERVAL", pending_retry=True)
        self.fixture.write()
        self.assert_retry(self.fixture.call(exit_code=1))

    def test_nonzero_exit_cannot_promote_claimed_complete(self) -> None:
        self.assert_retry(self.fixture.call(exit_code=1))

    def test_missing_terminal_json_does_not_search_latest_manifest(self) -> None:
        self.assert_retry(self.fixture.call(stdout_text="tick exit code: 0\n"))

    def test_multiple_terminal_objects_are_ambiguous_even_if_identical(self) -> None:
        self.assert_retry(self.fixture.call(stdout_text=self.fixture.stdout() + json.dumps(self.fixture.terminal())))

    def test_pretty_terminal_json_and_other_json_logs_are_supported(self) -> None:
        result = self.fixture.call(stdout_text='{"progress": "started"}\n' + json.dumps(self.fixture.terminal(), indent=2))
        self.assertEqual("COMPLETE", result["status"])

    def test_duplicate_terminal_keys_are_rejected(self) -> None:
        text = json.dumps(self.fixture.terminal())[:-1] + ', "status": "COMPLETED"}'
        self.assert_retry(self.fixture.call(stdout_text=text))

    def test_bad_terminal_fields_are_rejected(self) -> None:
        for field, value in (("status", "RUNNING"), ("tick_id", "../escaped"), ("rows_written", 999), ("new_listing_count", True)):
            with self.subTest(field=field):
                payload = self.fixture.terminal()
                payload[field] = value
                self.assert_retry(self.fixture.call(stdout_text=json.dumps(payload)))

    def test_manifest_identity_status_count_mutations_fail_closed(self) -> None:
        original = copy.deepcopy(self.fixture.manifest)
        mutations = (
            ("schema", "wrong"), ("tick_id", "expansion_tick_other"),
            ("plan_hash", "d" * 64), ("status", "RUNNING"),
            ("rows_written", -1), ("jobs_succeeded", True),
            ("jobs_attempted", 2), ("pending_retry", True),
            ("retry_queue", "not-an-array"), ("jobs", []),
        )
        stdout_text = self.fixture.stdout()
        for field, value in mutations:
            with self.subTest(field=field):
                self.fixture.manifest = {**original, field: value}
                self.fixture.write()
                self.assert_retry(self.fixture.call(stdout_text=stdout_text))

    def test_manifest_missing_never_uses_another_tick(self) -> None:
        original_path = self.fixture.manifest_path
        other = self.fixture.tick_root / "expansion_tick_other" / "manifest.json"
        other.parent.mkdir()
        other.write_bytes(original_path.read_bytes())
        original_path.unlink()
        self.assert_retry(self.fixture.call())

    def test_duplicate_manifest_keys_fail_closed(self) -> None:
        raw = json.dumps(self.fixture.manifest)[:-1] + ', "status": "COMPLETED"}'
        self.fixture.manifest_path.write_text(raw, encoding="utf-8")
        self.assert_retry(self.fixture.call())

    def test_stale_or_future_or_naive_manifest_timestamps_are_rejected(self) -> None:
        for field, value in (
            ("started_at_utc", "2026-08-25T01:00:01Z"),
            ("finished_at_utc", "2026-08-26T01:01:00Z"),
            ("finished_at_utc", "2026-08-26T00:59:59Z"),
            ("started_at_utc", "2026-08-26T01:00:01"),
        ):
            with self.subTest(field=field, value=value):
                original = self.fixture.manifest[field]
                self.fixture.manifest[field] = value
                self.fixture.write()
                self.assert_retry(self.fixture.call())
                self.fixture.manifest[field] = original

    def test_second_precision_manifest_is_allowed_inside_exact_launch_window(self) -> None:
        self.fixture.manifest["started_at_utc"] = "2026-08-26T01:00:00Z"
        self.fixture.handoff["issued_at_utc"] = "2026-08-26T01:00:00.800000Z"
        self.fixture.write()
        self.assertEqual("COMPLETE", self.fixture.call()["status"])

    def test_handoff_schema_and_exact_binding_mutations_fail_closed(self) -> None:
        original = copy.deepcopy(self.fixture.handoff)
        mutations = (
            ("schema", "wrong"), ("status", "CONSUMED"), ("project", "other"),
            ("automation_id", "other"), ("attempt_id", "expansion_tick_other"),
            ("plan_hash", "d" * 64), ("wrapper_pid", 4243), ("wrapper_pid", True),
            # Seconds away, not microseconds: a PID wearing a different process is what
            # this check exists to catch, and it is never a microsecond old.
            ("wrapper_process_started_at_utc", "2026-08-26T01:00:07.200000Z"),
            ("claim_run_id", "wrong"), ("claim_owner_kind", "other"),
            ("claim_owner_pid", 999), ("claim_owner_process_started_at_utc", self.fixture.started),
            ("claim_output_namespace", str(self.fixture.repo)), ("claim_must_exist", True),
            ("handoff_token_sha256", "bad"), ("claim_ownership_token_sha256", "bad"),
            ("issued_at_utc", "2026-08-25T01:00:01Z"),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                self.fixture.handoff = {**original, field: value}
                self.fixture.write()
                self.assert_retry(self.fixture.call())

    def shifted_wrapper_start(self, seconds: float) -> str:
        """The fixture's own wrapper start, moved by a given amount.

        Computed rather than written out: a hard-coded timestamp silently stops testing
        what it names the moment the fixture moves, which is how the first version of
        this test passed while asserting nothing."""
        base = datetime.fromisoformat(
            self.fixture.identity["started_at_utc"].replace("Z", "+00:00")
        )
        moved = base + timedelta(seconds=seconds)
        return moved.isoformat().replace("+00:00", "Z")

    def test_a_microsecond_of_measurement_noise_is_not_a_different_process(self) -> None:
        """The defect this pins cost the automation every tick it ran.

        The wrapper reads a launcher's start time through psutil; the launcher records its
        own through .NET. Measured over six spawns on this machine the two differ by one
        microsecond in five of them - psutil converts a float to microseconds, .NET prints
        seven digits. Compared with `==`, that rejected roughly five ticks in six: fifty
        jobs collected, manifest written, outcome recorded as RETRY_NEXT_INTERVAL with
        "the handoff receipt names a wrapper with a different start time"."""
        original = copy.deepcopy(self.fixture.handoff)
        for microseconds in (1, -1):
            with self.subTest(microseconds=microseconds):
                self.fixture.handoff = {
                    **original,
                    "wrapper_process_started_at_utc": self.shifted_wrapper_start(
                        microseconds / 1_000_000
                    ),
                }
                self.fixture.write()
                result = self.fixture.call()
                self.assertEqual("COMPLETE", result["status"], result.get("reason"))

    def test_the_tolerance_has_an_edge_and_the_check_still_bites_past_it(self) -> None:
        original = copy.deepcopy(self.fixture.handoff)
        for seconds, expected in (
            (evidence.PROCESS_START_TOLERANCE_SEC, "COMPLETE"),
            (-evidence.PROCESS_START_TOLERANCE_SEC, "COMPLETE"),
            (evidence.PROCESS_START_TOLERANCE_SEC + 0.001, "RETRY_NEXT_INTERVAL"),
            (-evidence.PROCESS_START_TOLERANCE_SEC - 0.001, "RETRY_NEXT_INTERVAL"),
        ):
            with self.subTest(seconds=seconds):
                self.fixture.handoff = {
                    **original,
                    "wrapper_process_started_at_utc": self.shifted_wrapper_start(seconds),
                }
                self.fixture.write()
                result = self.fixture.call()
                self.assertEqual(expected, result["status"], result.get("reason"))

    def test_handoff_missing_duplicate_or_still_issued_fails_closed(self) -> None:
        self.fixture.receipt_path.unlink()
        self.assert_retry(self.fixture.call())
        self.fixture.write()
        duplicate = self.fixture.receipt_path.with_name(self.fixture.tick_id + ".20260826T010001400000Z.json")
        duplicate.write_bytes(self.fixture.receipt_path.read_bytes())
        self.assert_retry(self.fixture.call())
        duplicate.unlink()
        (self.fixture.handoff_dir / (self.fixture.tick_id + ".json")).write_bytes(self.fixture.receipt_path.read_bytes())
        self.assert_retry(self.fixture.call())

    def test_malformed_or_relative_bound_plan_is_rejected(self) -> None:
        for plan in ({}, {**self.fixture.plan, "plan_hash": "bad"}, {**self.fixture.plan, "tick": {"tick_output_root": "relative"}}):
            with self.subTest(plan=plan):
                self.assert_retry(self.fixture.call(child_plan=plan))

    def test_invalid_outer_identity_or_time_window_is_rejected(self) -> None:
        for overrides in (
            {"child_identity": {"pid": True, "started_at_utc": self.fixture.identity["started_at_utc"]}},
            {"child_identity": {"pid": 4242, "started_at_utc": "invalid"}},
            {"started_at_utc": "2026-08-26T01:00:06Z"},
            {"finished_at_utc": "invalid"},
            {"exit_code": False},
        ):
            with self.subTest(overrides=overrides):
                self.assert_retry(self.fixture.call(**overrides))

    def test_nested_historical_cadence_never_becomes_official(self) -> None:
        result = self.fixture.call()
        self.assertTrue(result["cadence_observation"]["candidate"])
        self.assertEqual("proxy", result["cadence_observation"]["source_class"])
        self.assertTrue(result["cadence_observation"]["proxy_timestamp"])
        self.assertFalse(result["cadence_observation"]["official_confirmed"])
        self.assertFalse(result["cadence_observation"]["exact_timestamp"])
        self.assertNotIn("event_eta_utc", result["cadence_observation"])

    def test_only_completed_windows_do_not_create_upcoming_candidate(self) -> None:
        self.fixture.manifest["jobs"][0]["category"] = "new_listing_complete"
        self.fixture.write()
        self.assertFalse(self.fixture.call()["cadence_observation"]["candidate"])

    def test_adapter_does_not_modify_files_or_inputs(self) -> None:
        def snapshot() -> dict:
            return {str(path): path.read_bytes() for path in Path(self.temporary.name).rglob("*") if path.is_file()}
        before_files = snapshot()
        before_plan = copy.deepcopy(self.fixture.plan)
        before_identity = copy.deepcopy(self.fixture.identity)
        first = self.fixture.call()
        self.assertEqual(first, self.fixture.call())
        self.assertEqual(before_files, snapshot())
        self.assertEqual(before_plan, self.fixture.plan)
        self.assertEqual(before_identity, self.fixture.identity)

    def test_oversized_stdout_fails_closed(self) -> None:
        self.assert_retry(self.fixture.call(stdout_text="x" * (2 * 1024 * 1024)))

    def test_ledger_missing_duplicate_and_corrupt_tail_fail_closed(self) -> None:
        row = json.dumps(self.fixture.ledger_row()) + "\n"
        self.fixture.ledger_path.unlink()
        self.assert_retry(self.fixture.call())
        for raw in (row + row, row + '{"schema":', row.rstrip()):
            with self.subTest(raw=raw[-25:]):
                self.fixture.ledger_path.write_text(raw, encoding="utf-8")
                self.assert_retry(self.fixture.call())

    def test_terminal_ledger_exact_binding_mutations_fail_closed(self) -> None:
        original = self.fixture.ledger_row()
        for field, value in (
            ("schema", "wrong"), ("attempt_id", "other"), ("plan_id", "other"),
            ("plan_hash", "d" * 64), ("run_id", "other"),
            ("owner_pid", 999), ("owner_pid", True),
            ("owner_process_started_at_utc", "2026-08-26T01:00:08.250000Z"),
            ("ownership_token_sha256", "e" * 64),
            ("output_namespace", str(self.fixture.repo)),
            ("claim_path", str(self.fixture.repo / "other.json")),
            ("manifest_path", str(self.fixture.repo / "manifest.json")),
            ("manifest_sha256", "e" * 64), ("manifest_status", "STOPPED_INCOMPLETE"),
            ("status", "STOPPED_INCOMPLETE"), ("pending_retry", True),
            ("finished_at_utc", "2026-08-26T02:00:00Z"),
        ):
            with self.subTest(field=field):
                self.fixture.ledger_path.write_text(json.dumps({**original, field: value}) + "\n", encoding="utf-8")
                self.assert_retry(self.fixture.call())

    def test_unrelated_ledger_rows_never_substitute_exact_tick(self) -> None:
        row = self.fixture.ledger_row()
        row["tick_id"] = "expansion_tick_other"
        row["attempt_id"] = "expansion_tick_other"
        self.fixture.ledger_path.write_text(json.dumps(row) + "\n", encoding="utf-8")
        self.assert_retry(self.fixture.call())

    def test_unrelated_valid_ledger_history_is_preserved_and_read_only(self) -> None:
        row = self.fixture.ledger_row()
        row["tick_id"] = "expansion_tick_other"
        row["attempt_id"] = "expansion_tick_other"
        raw = json.dumps(row) + "\n" + self.fixture.ledger_path.read_text(encoding="utf-8")
        self.fixture.ledger_path.write_text(raw, encoding="utf-8")
        result = self.fixture.call()
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(), result["child_terminal_ledger_sha256"])
        self.assertEqual(raw, self.fixture.ledger_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
