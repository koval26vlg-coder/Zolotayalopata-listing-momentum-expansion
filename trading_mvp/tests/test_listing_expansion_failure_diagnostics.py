from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import listing_expansion_automation as runner


class ChildLaunchDiagnosticsTests(unittest.TestCase):
    def test_child_launcher_always_receives_json_for_preflight_refusals(self) -> None:
        plan = {
            "plan_id": "fixture_wrapper", "automation": {"default_interval_sec": 21600,
            "max_runtime_sec": 600, "terminal_grace_sec": 60},
            "child_launcher_path": "fixture.ps1", "child_plan": {"path": "fixture.json"},
        }
        engine = Mock()
        engine.read_due.return_value = {"status": "DUE"}
        engine.begin_attempt.return_value = {
            "status": runner.STATUS_LAUNCHING, "attempt_id": "fixture_attempt",
            "handoff_token": "fixture_token",
        }
        child = Mock(pid=4242)
        with patch.object(runner, "validate_plan", return_value=plan), \
             patch.object(runner, "_engine", return_value=engine), \
             patch.object(runner, "_identity", return_value=Mock()), \
             patch.object(runner, "_powershell", return_value=Path("pwsh.exe")), \
             patch.object(runner.subprocess, "Popen", return_value=child) as popen, \
             patch.object(runner, "_stream_child", return_value=(None, None)):
            result = runner.run_once(Path("wrapper.json"))
        self.assertEqual("CHILD_STILL_RUNNING", result["status"])
        self.assertIn("-Json", popen.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
