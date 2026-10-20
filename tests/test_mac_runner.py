"""Portable runner tests: CPU subprocesses, private temporary locks, no services."""
import ctypes
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from cq import agent


class GroupSnapshotTests(unittest.TestCase):
    def test_unknown_ps_is_not_drained_and_zombies_do_not_hold_resources(self):
        host = mock.Mock()
        for result in ((1, ""), (0, ""), (0, "broken\n")):
            host.run.return_value = result
            self.assertIsNone(agent.group_running(host, 42))
        host.run.return_value = (0, "42 42 Z\n43 42 S\n99 99 R\n")
        self.assertTrue(agent.group_running(host, 42))
        host.run.return_value = (0, "42 42 Z\n43 42 Z\n99 99 R\n")
        self.assertFalse(agent.group_running(host, 42))


@unittest.skipUnless(hasattr(os, "fork"), "POSIX runner")
class MacRunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.sdir = self.root / "signals"
        self.sdir.mkdir()
        self.lock = self.root / "gpu.lock"
        self.release = self.root / "release"
        self.ready = self.root / "ready"
        # Copy just agent.py: exercise the installed single-file entry point.
        self.script = self.root / "agent.py"
        shutil.copyfile(agent.__file__, self.script)
        if sys.platform.startswith("linux"):
            # Reap only this test's orphan child, even under container PID 1.
            libc = ctypes.CDLL(None, use_errno=True)
            previous = ctypes.c_int()
            if libc.prctl(37, ctypes.byref(previous), 0, 0, 0) != 0:
                raise OSError(ctypes.get_errno(), "PR_GET_CHILD_SUBREAPER")
            if libc.prctl(36, 1, 0, 0, 0) != 0:
                raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
            self.addCleanup(libc.prctl, 36, previous.value, 0, 0, 0)

    def wait_for(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("timed out waiting for runner evidence")

    def state(self):
        try:
            return json.loads((self.sdir / "runner.json").read_text())
        except FileNotFoundError:
            return {}

    def locked(self):
        with open(self.lock, "a") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            return False

    def launch(self, code, lock=True, grace=None, fail_handshake=False):
        argv = [sys.executable, str(self.script)]
        if grace is not None or fail_handshake:
            boot = ("import runpy,sys; sys.argv.pop(0); n=runpy.run_path(sys.argv[0]); "
                    f"n['mac_runner'].__globals__['KILL_GRACE_S']={grace or 20}\n")
            if fail_handshake:
                boot += (
                    "original=n['atomic_write']; failed=False\n"
                    "def fail_once(path, text):\n"
                    " global failed\n"
                    " if path.endswith('runner.json') and 'pgid' in text and not failed:\n"
                    "  failed=True\n"
                    "  raise OSError('injected handshake persistence failure')\n"
                    " return original(path, text)\n"
                    "n['mac_runner'].__globals__['atomic_write']=fail_once\n"
                )
            boot += "sys.exit(n['main']())"
            argv = [sys.executable, "-c", boot, str(self.script)]
        argv += ["--mac-runner", str(self.sdir)]
        if lock:
            argv += ["--gpu-lock", str(self.lock)]
        argv += ["--", sys.executable, "-c", code]
        p = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             start_new_session=True, cwd=self.root)
        self.addCleanup(self.cleanup_runner, p)
        return p

    def cleanup_runner(self, p):
        self.release.touch()
        try:
            p.communicate(timeout=12)
        finally:
            # The fixture records the exact orphan we own; never reap other tests' children.
            if sys.platform.startswith("linux") and self.ready.exists():
                try:
                    os.waitpid(int(self.ready.read_text()), 0)
                except ChildProcessError:
                    pass

    def lingering_code(self, ignore_term=False):
        child = (
            "import os,time,signal; from pathlib import Path; "
            + ("signal.signal(signal.SIGTERM, signal.SIG_IGN); " if ignore_term else "")
            + f"Path({str(self.ready)!r}).write_text(str(os.getpid())); "
            "end=time.monotonic()+10\n"
            f"while not Path({str(self.release)!r}).exists() and time.monotonic()<end: time.sleep(.02)\n"
        )
        return ("import subprocess,sys; "
                f"subprocess.Popen([sys.executable,'-c',{child!r}]); sys.exit(7)")

    def test_lock_survives_main_exit_until_last_child_and_rc_is_durable(self):
        p = self.launch(self.lingering_code())
        self.wait_for(self.ready.exists)
        self.wait_for(lambda: "pgid" in self.state())
        pgid = self.state()["pgid"]
        # Linux runs the command directly; Darwin adds caffeinate as group leader.
        if sys.platform.startswith("linux"):
            self.wait_for(lambda: subprocess.run(
                ["ps", "-p", str(pgid), "-o", "stat="], capture_output=True,
                text=True).stdout.strip().startswith("Z"))
        self.assertIsNone(p.poll())
        self.assertTrue(self.locked())
        self.assertFalse(self.state().get("done", False))
        self.assertFalse((self.sdir / "rc").exists())
        self.release.touch()
        _, err = p.communicate(timeout=8)
        self.assertEqual(p.returncode, 0, err)
        self.assertEqual(self.state()["rc"], 7)
        self.assertTrue(self.state()["done"])
        self.assertEqual((self.sdir / "rc").read_text(), "7\n")
        self.assertFalse(self.locked())

    def test_stop_escalates_with_exited_leader_without_dropping_lock(self):
        p = self.launch(self.lingering_code(ignore_term=True), grace=0.6)
        self.wait_for(self.ready.exists)
        self.assertTrue(self.locked())
        (self.sdir / "stop").write_text("cancel")
        time.sleep(0.15)
        self.assertIsNone(p.poll())
        self.assertTrue(self.locked())
        _, err = p.communicate(timeout=8)
        self.assertEqual(p.returncode, 0, err)
        self.assertTrue(self.state()["done"])
        self.assertFalse(self.locked())

    def test_contended_lock_blocks_exec_and_wait_can_be_cancelled(self):
        with open(self.lock, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            p = self.launch(f"from pathlib import Path; Path({str(self.ready)!r}).touch()")
            self.wait_for(lambda: "pid" in self.state())
            time.sleep(0.15)
            self.assertFalse(self.ready.exists())
            self.assertNotIn("pgid", self.state())
            (self.sdir / "stop").write_text("cancel")
            _, err = p.communicate(timeout=8)
            self.assertEqual(p.returncode, 0, err)
            self.assertEqual(self.state()["rc"], 143)
            self.assertFalse(self.ready.exists())

    def test_cpu_runs_while_temporary_gpu_lock_is_held(self):
        with open(self.lock, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            p = self.launch("import sys; sys.exit(23)", lock=False)
            _, err = p.communicate(timeout=8)
            self.assertEqual(p.returncode, 0, err)
            self.assertEqual(self.state()["rc"], 23)

    def test_lock_open_failure_never_executes_work(self):
        self.lock = self.root / "missing" / "gpu.lock"
        p = self.launch(f"from pathlib import Path; Path({str(self.ready)!r}).touch()")
        _, err = p.communicate(timeout=8)
        self.assertEqual(p.returncode, 0, err)
        self.assertEqual(self.state()["rc"], agent.LAUNCH_FAILED_RC)
        self.assertFalse(self.ready.exists())

    def test_failed_group_handshake_never_executes_work(self):
        p = self.launch(f"from pathlib import Path; Path({str(self.ready)!r}).touch()",
                        fail_handshake=True)
        _, err = p.communicate(timeout=8)
        self.assertEqual(p.returncode, 0, err)
        self.assertEqual(self.state()["rc"], agent.LAUNCH_FAILED_RC)
        self.assertFalse(self.ready.exists())
        self.assertFalse(self.locked())


if __name__ == "__main__":
    unittest.main()
