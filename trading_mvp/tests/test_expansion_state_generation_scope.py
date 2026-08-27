from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

TESTS_ROOT = Path(__file__).resolve().parent
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

import slow_liquidity_listing_momentum_forward_expansion_monitor as monitor  # noqa: E402

CURRENT = "c" * 64
RETIRED = "r" * 64


class GenerationScopeTests(unittest.TestCase):
    """A state rebuilt from a shared output root has to say what it is a sample of.

    The tick directory is a volume path, not a per-repository one, so every tick that has
    ever run there is read back - twenty of them across nine plan generations by the time
    this was written. Nothing recorded which plan produced which window, and the cadence
    observation was computed over the whole pile, which meant a marker left by a retired
    plan could hold the scheduler at its tightest interval indefinitely.
    """

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="generation-scope-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.ticks = self.root / "ticks"
        self.ticks.mkdir()
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_text(json.dumps({"plan_hash": CURRENT}), encoding="utf-8")

    def write_tick(
        self,
        tick_id: str,
        plan_hash: str,
        *,
        base: str,
        in_progress: bool = False,
        official: bool = False,
        close: float = 2.0,
    ) -> None:
        directory = self.ticks / tick_id
        directory.mkdir()
        job = {
            "exchange": "bitget",
            "base": base,
            "category": "new_listing_in_progress" if in_progress else "new_listing_complete",
            "timestamp_source": "official_announcement" if official else "listing_proxy",
            "flags": ["window_in_progress"] if in_progress else [],
        }
        directory.joinpath("manifest.json").write_text(
            json.dumps(
                {
                    "tick_id": tick_id,
                    "plan_hash": plan_hash,
                    "status": "COMPLETED",
                    "new_listing_count": 1,
                    "rows_written": 1,
                    "jobs": [job],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        directory.joinpath("ohlcv.jsonl").write_text(
            json.dumps(
                {
                    "exchange": "bitget",
                    "base": base,
                    "ts": 1_710_000_000,
                    "open": 1.0,
                    "high": 2.5,
                    "low": 0.9,
                    "close": close,
                    "volume": 10.0,
                }
            )
            + "\n",
            encoding="utf-8",
        )

    def rebuild(self) -> dict:
        with mock.patch.object(monitor, "TICKS_DIR", self.ticks), mock.patch.object(
            monitor, "STATE_PATH", self.root / "state.json"
        ), mock.patch.object(monitor, "PLAN_PATH", self.plan_path):
            return monitor.rebuild_forward_state()

    def test_every_window_names_the_tick_and_plan_that_produced_it(self) -> None:
        self.write_tick("tick_a", RETIRED, base="OLD")
        self.write_tick("tick_b", CURRENT, base="NEW")
        state = self.rebuild()
        by_base = {window["base"]: window for window in state["windows"]}
        self.assertEqual("tick_a", by_base["OLD"]["source_tick_id"])
        self.assertEqual(RETIRED, by_base["OLD"]["source_plan_hash"])
        self.assertEqual("tick_b", by_base["NEW"]["source_tick_id"])
        self.assertEqual(CURRENT, by_base["NEW"]["source_plan_hash"])

    def test_a_later_tick_that_replaces_a_window_takes_its_attribution_with_it(self) -> None:
        # The last tick to touch a pair replaces it outright rather than merging, so the
        # attribution follows the surviving bars rather than the first sighting.
        self.write_tick("tick_a", RETIRED, base="SAME", close=1.0)
        self.write_tick("tick_b", CURRENT, base="SAME", close=9.0)
        state = self.rebuild()
        self.assertEqual(1, len(state["windows"]))
        window = state["windows"][0]
        self.assertEqual(CURRENT, window["source_plan_hash"])
        self.assertEqual("tick_b", window["source_tick_id"])

    def test_counts_are_reported_per_generation_and_for_the_current_one(self) -> None:
        self.write_tick("tick_a", RETIRED, base="OLDA")
        self.write_tick("tick_b", RETIRED, base="OLDB")
        self.write_tick("tick_c", CURRENT, base="NEW")
        state = self.rebuild()
        self.assertEqual(3, state["window_count"])
        self.assertEqual(CURRENT, state["current_plan_hash"])
        self.assertEqual(1, state["current_plan_window_count"])
        self.assertEqual(2, state["windows_by_plan_hash"][RETIRED]["windows"])
        self.assertEqual(1, state["windows_by_plan_hash"][CURRENT]["windows"])

    def test_a_retired_generation_cannot_drive_the_cadence(self) -> None:
        # The marker that would pin the scheduler to its tightest interval belongs to a
        # plan nobody runs any more.
        self.write_tick("tick_a", RETIRED, base="OLD", in_progress=True, official=True)
        self.write_tick("tick_b", CURRENT, base="NEW")
        observation = self.rebuild()["cadence_observation"]
        self.assertFalse(observation["candidate"])
        self.assertFalse(observation["official_confirmed"])
        self.assertEqual(CURRENT, observation["scoped_to_plan_hash"])

    def test_the_current_generation_still_drives_it(self) -> None:
        self.write_tick("tick_a", RETIRED, base="OLD")
        self.write_tick("tick_b", CURRENT, base="NEW", in_progress=True, official=True)
        observation = self.rebuild()["cadence_observation"]
        self.assertTrue(observation["candidate"])
        self.assertTrue(observation["official_confirmed"])

    def test_the_observation_says_what_it_was_computed_over(self) -> None:
        self.write_tick("tick_a", RETIRED, base="OLDA")
        self.write_tick("tick_b", RETIRED, base="OLDB")
        self.write_tick("tick_c", CURRENT, base="NEW")
        observation = self.rebuild()["cadence_observation"]
        self.assertEqual(1, observation["job_rows_in_scope"])
        self.assertEqual(3, observation["job_rows_in_store"])

    def test_an_unreadable_plan_scopes_to_everything_rather_than_to_nothing(self) -> None:
        # Silently narrowing to zero would report a quiet SEARCH cadence and an empty
        # sample, both of which look like calm rather than like a missing plan.
        self.plan_path.write_text("{ not json", encoding="utf-8")
        self.write_tick("tick_a", RETIRED, base="OLD", in_progress=True, official=True)
        state = self.rebuild()
        self.assertIsNone(state["current_plan_hash"])
        self.assertTrue(state["cadence_observation"]["candidate"])
        self.assertEqual(1, state["cadence_observation"]["job_rows_in_scope"])

    def test_the_tick_list_carries_its_generation(self) -> None:
        self.write_tick("tick_a", RETIRED, base="OLD")
        self.write_tick("tick_b", CURRENT, base="NEW")
        ticks = {row["tick_id"]: row["plan_hash"] for row in self.rebuild()["ticks"]}
        self.assertEqual({"tick_a": RETIRED, "tick_b": CURRENT}, ticks)


if __name__ == "__main__":
    unittest.main()
