"""Fault injection is limited to the OS boundary; payload/ledger bytes stay real."""
from __future__ import annotations

import errno
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
state_module = importlib.import_module("listing_automation_state")
atomic_io = importlib.import_module("listing_atomic_io")


def windows_error(code: int) -> OSError:
    error = PermissionError(errno.EACCES, f"injected Windows error {code}")
    error.winerror = code
    return error


class ListingAtomicIOTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory(prefix="listing-atomic-io-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.paths = state_module.AutomationPaths(self.root / "automation")
        self.engine = state_module.AutomationEngine(
            self.paths,
            state_module.Binding("fixture", "a" * 64, "b" * 64),
            now=lambda: datetime(2026, 9, 25, tzinfo=timezone.utc),
            process_probe=lambda _: state_module.ProbeResult("DEAD"),
        )

    def test_bytes_api_preserves_exact_payload_and_no_temp_remains(self) -> None:
        target = self.root / "nested" / "registry.json"
        payload = b'{"a":1}\r\n\x00'
        atomic_io.atomic_write_bytes(target, payload)
        self.assertEqual(payload, target.read_bytes())
        self.assertEqual([target], list(target.parent.iterdir()))

    def test_serialization_error_occurs_before_any_filesystem_mutation(self) -> None:
        target = self.root / "not-created" / "state.json"
        with self.assertRaises(TypeError):
            atomic_io.atomic_write_json(target, {"bad": object()})
        self.assertFalse(target.parent.exists())
        with self.assertRaises(TypeError):
            atomic_io.atomic_write_bytes(target, "not-bytes")
        self.assertFalse(target.parent.exists())

    @unittest.skipUnless(os.name == "nt", "Windows replacement-denial regression")
    def test_share_and_lock_violations_retry_but_other_windows_errors_do_not(self) -> None:
        for code, expected_calls in ((32, 2), (33, 2), (87, 1)):
            with self.subTest(winerror=code):
                target = self.root / f"state-{code}.json"
                target.write_bytes(b"prior")
                actual_replace = os.replace
                calls = []

                def one_denial(source, destination):
                    calls.append((source, destination))
                    if len(calls) == 1:
                        raise windows_error(code)
                    return actual_replace(source, destination)

                with patch("os.replace", side_effect=one_denial), patch("time.sleep") as sleep:
                    if code == 87:
                        with self.assertRaises(PermissionError):
                            atomic_io.atomic_write_bytes(target, b"replacement")
                    else:
                        atomic_io.atomic_write_bytes(target, b"replacement")
                self.assertEqual(expected_calls, len(calls))
                self.assertEqual(expected_calls - 1, sleep.call_count)
                self.assertEqual(b"prior" if code == 87 else b"replacement", target.read_bytes())

    @unittest.skipUnless(os.name == "nt", "real Windows handle experiment")
    def test_real_windows_handle_without_delete_share_recovers_after_release(self) -> None:
        import ctypes
        from ctypes import wintypes
        import time

        target = self.root / "state-real-handle.json"
        target.write_bytes(b"prior")
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateFileW.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE)
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel.CloseHandle.restype = wintypes.BOOL
        handle = kernel.CreateFileW(str(target), 0x80000000, 0x1 | 0x2, None, 3, 0x80, None)
        self.assertNotEqual(ctypes.c_void_p(-1).value, handle)
        closed = False
        real_sleep = time.sleep

        def release_then_wait(delay):
            nonlocal closed
            if not closed:
                self.assertEqual(b"prior", target.read_bytes())
                self.assertTrue(kernel.CloseHandle(handle))
                closed = True
            real_sleep(delay)

        try:
            with patch("time.sleep", side_effect=release_then_wait) as sleep:
                atomic_io.atomic_write_bytes(target, b"replacement")
            self.assertTrue(closed)
            self.assertGreaterEqual(sleep.call_count, 1)
            self.assertLessEqual(sleep.call_count, 4)
            self.assertEqual(b"replacement", target.read_bytes())
        finally:
            if not closed:
                kernel.CloseHandle(handle)

    @unittest.skipUnless(os.name == "nt", "Windows replacement-denial regression")
    def test_transient_windows_denial_replaces_same_staged_file_after_wait(self) -> None:
        target = self.root / "state.json"
        target.write_bytes(b'{"old":true}\n')
        actual_replace = os.replace
        pairs = []

        def transient(source, destination):
            pairs.append((Path(source), Path(destination)))
            if len(pairs) == 1:
                raise windows_error(5)
            self.assertEqual(b'{"old":true}\n', target.read_bytes())
            return actual_replace(source, destination)

        error = None
        with patch("os.replace", side_effect=transient), patch("time.sleep") as sleep:
            try:
                self.engine._write_json(target, {"new": "данные"})
            except OSError as exc:
                error = exc
        self.assertIsNone(error, f"transient replace denial escaped: {error!r}")
        self.assertEqual(2, len(pairs))
        self.assertEqual(pairs[0], pairs[1])
        sleep.assert_called_once_with(0.025)
        self.assertEqual({"new": "данные"}, json.loads(target.read_bytes()))
        self.assertEqual([], list(self.root.glob("state.json.tmp.*")))

    def test_staged_payload_is_fsynced_and_closed_before_replace(self) -> None:
        target = self.root / "state.json"
        actual_replace = os.replace
        actual_fsync = os.fsync
        descriptors = []

        def sync(descriptor):
            descriptors.append(descriptor)
            actual_fsync(descriptor)

        def replace(source, destination):
            self.assertEqual(1, len(descriptors), "payload was not fsynced")
            with self.assertRaises(OSError):
                os.fstat(descriptors[0])
            return actual_replace(source, destination)

        with patch("os.fsync", side_effect=sync), patch("os.replace", side_effect=replace):
            self.engine._write_json(target, {"x": 1})
        self.assertEqual({"x": 1}, json.loads(target.read_bytes()))

    def test_permanent_non_windows_permission_error_is_not_retried(self) -> None:
        target = self.root / "state.json"
        target.write_bytes(b"old-state\n")
        denied = PermissionError(errno.EACCES, "permanent permission error")
        with patch("os.replace", side_effect=denied) as replace, patch("time.sleep") as sleep:
            with self.assertRaises(PermissionError) as raised:
                self.engine._write_json(target, {"new": True})
        self.assertIs(denied, raised.exception)
        replace.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(b"old-state\n", target.read_bytes())
        staged = list(self.root.glob("state.json.tmp.*"))
        self.assertEqual(1, len(staged))
        self.assertEqual({"new": True}, json.loads(staged[0].read_bytes()))
        self.assertTrue(any(str(staged[0]) in note for note in getattr(denied, "__notes__", [])))

    @unittest.skipUnless(os.name == "nt", "Windows replacement-denial regression")
    def test_persistent_windows_denial_is_bounded_and_preserves_prior_and_staging(self) -> None:
        target = self.root / "state.json"
        target.write_bytes(b"old-state\n")
        denied = windows_error(5)
        with patch("os.replace", side_effect=denied) as replace, patch("time.sleep") as sleep:
            with self.assertRaises(PermissionError) as raised:
                self.engine._write_json(target, {"new": True})
        self.assertIs(denied, raised.exception)
        self.assertEqual(5, replace.call_count)
        self.assertEqual([0.025, 0.05, 0.1, 0.2], [call.args[0] for call in sleep.call_args_list])
        self.assertEqual(1, len({tuple(map(str, call.args)) for call in replace.call_args_list}))
        self.assertEqual(b"old-state\n", target.read_bytes())
        staged = list(self.root.glob("state.json.tmp.*"))
        self.assertEqual(1, len(staged))
        self.assertEqual({"new": True}, json.loads(staged[0].read_bytes()))

    def test_fsync_failure_never_replaces_previous_destination(self) -> None:
        target = self.root / "state.json"
        target.write_bytes(b"previous-state\n")
        with patch("os.fsync", side_effect=OSError(errno.ENOSPC, "disk full")), patch("os.replace") as replace:
            with self.assertRaises(OSError):
                self.engine._write_json(target, {"new": True})
        replace.assert_not_called()
        self.assertEqual(b"previous-state\n", target.read_bytes())

    def test_failed_terminal_state_write_keeps_ledger_and_claim_for_reconcile(self) -> None:
        self.engine.initialize()
        attempt = self.engine.begin_attempt()
        self.engine.bind_worker(attempt["attempt_id"], attempt["handoff_token"], state_module.ProcessIdentity(123, "2026-09-25T00:00:00Z"))
        before_state = self.paths.state.read_bytes()
        before_claim = self.paths.claim.read_bytes()
        before_ledger = self.paths.ledger.read_bytes()
        with patch("os.replace", side_effect=PermissionError(errno.EACCES, "permanent denial")):
            with self.assertRaises(PermissionError):
                self.engine.finish_attempt(attempt["attempt_id"], attempt["handoff_token"], outcome="RETRY_NEXT_INTERVAL", reason="fixture")
        self.assertEqual(before_state, self.paths.state.read_bytes())
        self.assertEqual(before_claim, self.paths.claim.read_bytes())
        after_ledger = self.paths.ledger.read_bytes()
        self.assertTrue(after_ledger.startswith(before_ledger))
        self.assertEqual(1, sum(json.loads(line)["kind"] == "TERMINAL" for line in after_ledger.splitlines()))
        reconciled = self.engine.reconcile()
        self.assertEqual("RETRY_NEXT_INTERVAL", reconciled["state_status"])
        self.assertTrue(reconciled["pending_retry"])
        self.assertEqual(after_ledger, self.paths.ledger.read_bytes())


if __name__ == "__main__":
    unittest.main()
