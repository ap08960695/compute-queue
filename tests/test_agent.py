"""Agent tests: fake OS host (systemctl, footprint, sysctl, journalctl, ps) + fake Coordinator."""
import fcntl
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cq import agent  # noqa: E402

GIB = 2**30


def setUpModule():
    os.environ["no_proxy"] = "127.0.0.1,localhost"


class FakeProc:
    def __init__(self, pid):
        self.pid, self.returncode = pid, None

    def poll(self):
        return self.returncode


class FakeHost:
    """Records every OS interaction; answers like systemctl/ps/footprint/sysctl/journalctl."""

    def __init__(self):
        self.cmds, self.spawned, self.signals = [], [], []
        self.units = {}        # unit -> property overrides for `systemctl show`
        self.listed = set()    # unit names `systemctl list-units` prints
        self.footprints = {}   # pid -> bytes
        self.lstart = {}       # pid -> `ps -o lstart=` output
        self.groups = {}       # pid -> pgid, rows for `ps -axo pid=,pgid=`
        self.pressure = 1
        self.journal = (0, "")
        self.fail_systemd_run = False
        self.fail_units = set()  # `--unit` names for which systemd-run fails
        self.next_pid = 4000

    def run(self, argv, timeout=30):
        argv = list(argv)
        self.cmds.append(argv)
        c = argv[0]
        if c == "systemd-run":
            unit = argv[argv.index("--unit") + 1]
            if self.fail_systemd_run or unit in self.fail_units:
                return 1, "boom"
            self.listed.add(unit + ".service")
            return 0, ""
        if c == "systemctl":
            if argv[2] == "show":
                return 0, "\n".join(f"{k}={v}" for k, v in self.props(argv[3]).items())
            if argv[2] == "list-units":
                return 0, "".join(f"{u} loaded active running desc\n" for u in sorted(self.listed))
            if argv[2] == "stop":  # the unit is gone by the next tick
                self.units[argv[-1]] = {"ActiveState": "inactive", "SubState": "dead"}
                self.listed.discard(argv[-1])
            return 0, ""
        if c == "sudo":
            return self.journal
        if c == "sysctl":
            vals = {"kern.memorystatus_vm_pressure_level": f"{self.pressure}\n",
                    "hw.memsize": "17179869184\n", "machdep.cpu.brand_string": "Apple M2\n"}
            return 0, vals.get(argv[2], "")
        if c == "vm_stat":
            return 0, ("Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
                       "Pages free: 1000.\nPages inactive: 1000.\nPages speculative: 0.\n")
        if c == "ps":
            if argv[1] == "-axo":
                return 0, "999 999\n" + "".join(f"{p} {g}\n" for p, g in self.groups.items())
            return 0, self.lstart.get(int(argv[2]), "")
        if c == "footprint":
            return 0, f"Footprint: {self.footprints.get(int(argv[2]), 0)} B\n"
        return 127, ""

    def props(self, unit):
        p = {"ActiveState": "active", "SubState": "running", "LoadState": "loaded",
             "ExecMainStatus": "0", "ExecMainCode": "0", "ControlGroup": f"/cq/{unit}"}
        if unit not in self.units and unit not in self.listed:
            p.update({"ActiveState": "inactive", "SubState": "dead", "LoadState": "not-found"})
        p.update(self.units.get(unit, {}))
        return p

    def spawn(self, argv, cwd, env, log_path):
        p = FakeProc(self.next_pid)
        self.next_pid += 10
        self.spawned.append({"argv": list(argv), "cwd": cwd, "env": env, "proc": p})
        self.lstart[p.pid] = "Tue Oct 20 10:00:00 2026"
        self.groups[p.pid] = p.pid
        self.groups[p.pid + 1] = p.pid  # a child in the same group
        return p

    def killpg(self, pgid, sig):
        self.signals.append((pgid, sig))

    def group_alive(self, pgid):
        return pgid in self.groups.values()

    def systemctl(self, verb):
        return [c[-1] for c in self.cmds if c[:3] == ["systemctl", "--user", verb]]


class Coord:
    """Fake Coordinator on 127.0.0.1:0 that records requests and replies once per heartbeat."""

    def __init__(self):
        self.events, self.next_reply = [], {}
        self.report_status = 200
        coord = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                coord.events.append((self.path, body, self.headers.get("Authorization")))
                out = {}
                if self.path == "/agent/heartbeat":
                    out = {"start": [], "preempt": [], "cancel": [], **coord.next_reply}
                    coord.next_reply = {}
                elif self.path == "/nodes/register":
                    out = {"token": "tok-secret"}
                data = json.dumps(out).encode()
                self.send_response(coord.report_status if self.path == "/agent/report" else 200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def reports(self):
        return [b for p, b, _ in self.events if p == "/agent/report"]

    def heartbeats(self):
        return [b for p, b, _ in self.events if p == "/agent/heartbeat"]


def dead_url():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}"


class Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.t = Path(tmp.name)
        self.spool = self.t / "spool"
        self.coord = Coord()
        self.addCleanup(self.coord.stop)
        self.host = FakeHost()
        self.clock = [1000.0]

    def mk(self, os_name="linux", gpu=False, vram_used=0, **cfg):
        proc, sysfs = self.t / "proc", self.t / "sys"
        proc.mkdir(exist_ok=True)
        (proc / "meminfo").write_text("MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n")
        if gpu:
            dev = sysfs / "class/drm/card0/device"
            dev.mkdir(parents=True, exist_ok=True)
            (dev / "mem_info_vram_total").write_text("17163091968")
            (dev / "mem_info_vram_used").write_text(str(vram_used))
            (dev / "gpu_busy_percent").write_text("0")
        cfg = {"coordinator": self.coord.url, "token": "t0k", "node": "n", "os": os_name, **cfg}
        return agent.Agent(cfg, str(self.spool), host=self.host, now=lambda: self.clock[0],
                           proc=str(proc), sysfs=str(sysfs), cgroup=str(self.t / "cgroup"))

    def set_vram_used(self, n):
        (self.t / "sys/class/drm/card0/device/mem_info_vram_used").write_text(str(n))

    def set_mem(self, unit, anon, kernel=0, shmem=0):
        d = self.t / "cgroup/cq" / unit
        d.mkdir(parents=True, exist_ok=True)
        (d / "memory.stat").write_text(f"anon {anon}\nkernel {kernel}\nshmem {shmem}\nfile 99999\n")

    def start_job(self, a, jid=7, gpu=False, **extra):
        spec = {"id": jid, "argv": ["python3", "x.py"], "cwd": "/work", "env": {}, "gpu": gpu,
                "effective_ram": 1000, **extra}
        self.coord.next_reply = {"start": [spec]}
        a.tick()


class LinuxTests(Base):
    def test_start_builds_exact_systemd_run_argv(self):
        a = self.mk()
        self.start_job(a, env={"A": "1"}, effective_ram=6 * GIB)
        runs = [c for c in self.host.cmds if c[0] == "systemd-run"]
        self.assertEqual(runs, [[
            "systemd-run", "--user", "--unit", "cq-job-7-a0",
            f"--property=MemoryMax={6 * GIB}", "--property=MemorySwapMax=0",
            "--property=KillMode=control-group", "--property=RemainAfterExit=yes",
            "--property=Type=exec", "--working-directory=/work",
            "--setenv=A=1", "--setenv=CQ_JOB_ID=7", f"--setenv=CQ_SIGNAL_DIR={self.spool}/signals/7",
            "--", "python3", "x.py"]])
        self.assertNotIn("--collect", runs[0])
        saved = json.loads((self.spool / "allocs.json").read_text())
        self.assertEqual([x["job_id"] for x in saved], [7])

    def test_gpu_job_is_wrapped_in_flock(self):
        a = self.mk(gpu=True, gpu_locks=["/tmp/gpu0.lock", "/tmp/gpu1.lock"])
        self.start_job(a, gpu=True)
        run = next(c for c in self.host.cmds if c[0] == "systemd-run")
        self.assertEqual(run[run.index("--") + 1:], ["flock", "-x", "/tmp/gpu0.lock", "python3", "x.py"])

    def test_cpu_job_is_not_wrapped_in_flock(self):
        a = self.mk(gpu=True, gpu_locks=["/tmp/gpu0.lock"])
        self.start_job(a, gpu=False)
        run = next(c for c in self.host.cmds if c[0] == "systemd-run")
        self.assertEqual(run[run.index("--") + 1:], ["python3", "x.py"])

    def test_heartbeat_carries_stats_and_allocs(self):
        a = self.mk()
        self.start_job(a)
        self.set_mem("cq-job-7-a0.service", anon=100, kernel=20, shmem=3)
        a.tick()
        hb = self.coord.heartbeats()[-1]
        self.assertEqual(hb["stats"]["mem_available"], 8000000 * 1024)
        self.assertEqual(hb["allocs"], [{"job_id": 7, "attempt": 0, "state": "running", "current_ram": 123, "current_vram": 0}])
        self.assertEqual(self.coord.events[-1][2], "Bearer t0k")

    def test_finish_detected_and_rc_reported(self):
        a = self.mk()
        self.start_job(a)
        self.set_mem("cq-job-7-a0.service", anon=100, kernel=20, shmem=3)
        self.clock[0] += 20
        a.tick()
        self.assertEqual(self.coord.reports(), [])
        self.host.units["cq-job-7-a0.service"] = {"SubState": "exited", "ExecMainStatus": "3", "ExecMainCode": "1"}
        self.clock[0] += 30
        a.tick()
        self.assertEqual(self.coord.reports(),
                         [{"job_id": 7, "attempt": 0, "rc": 3, "peaks": {"peak_ram": 123, "peak_vram": 0}, "wall_s": 50.0}])
        self.assertEqual(self.host.systemctl("stop"), ["cq-job-7-a0.service"])
        self.assertEqual(self.host.systemctl("reset-failed"), ["cq-job-7-a0.service"])
        self.assertEqual(a.allocs, {})
        self.assertEqual(json.loads((self.spool / "allocs.json").read_text()), [])

    def test_killed_by_signal_reports_128_plus(self):
        a = self.mk()
        self.start_job(a)
        self.host.units["cq-job-7-a0.service"] = {"ActiveState": "failed", "ExecMainStatus": "9", "ExecMainCode": "2"}
        a.tick()
        self.assertEqual(self.coord.reports()[0]["rc"], 137)

    def test_outbox_when_coordinator_down_then_flushed(self):
        a = self.mk()
        self.start_job(a)
        self.host.units["cq-job-7-a0.service"] = {"SubState": "exited"}
        a.cfg["coordinator"] = dead_url()
        self.coord.next_reply = {"start": [{"id": 8, "argv": ["true"], "effective_ram": 1}]}
        a.tick()
        lines = (self.spool / "outbox.jsonl").read_text().splitlines()
        self.assertEqual([json.loads(x)["job_id"] for x in lines], [7])
        self.assertEqual(self.coord.reports(), [])
        self.assertEqual(len([c for c in self.host.cmds if c[0] == "systemd-run"]), 1)  # nothing new started
        self.assertEqual(a.allocs, {})
        a.cfg["coordinator"] = self.coord.url
        a.tick()
        self.assertEqual([r["job_id"] for r in self.coord.reports()], [7])
        self.assertEqual((self.spool / "outbox.jsonl").read_text(), "")
        paths = [p for p, _, _ in self.coord.events]
        self.assertLess(paths.index("/agent/report"), len(paths) - 1 - paths[::-1].index("/agent/heartbeat"))

    def test_outbox_survives_agent_restart(self):
        a = self.mk()
        self.start_job(a)
        self.host.units["cq-job-7-a0.service"] = {"SubState": "exited"}
        a.cfg["coordinator"] = dead_url()
        a.tick()
        b = self.mk()
        b.tick()
        self.assertEqual([r["job_id"] for r in self.coord.reports()], [7])

    def test_reattach_from_allocs_json(self):
        self.start_job(self.mk())
        b = self.mk()
        self.assertEqual(list(b.allocs), [7])
        self.host.units["cq-job-7-a0.service"] = {"SubState": "exited", "ExecMainStatus": "0", "ExecMainCode": "1"}
        b.tick()
        self.assertEqual([(r["job_id"], r["rc"]) for r in self.coord.reports()], [(7, 0)])
        self.assertEqual(len([c for c in self.host.cmds if c[0] == "systemd-run"]), 1)

    def test_duplicate_start_is_ignored(self):
        a = self.mk()
        self.start_job(a)
        self.start_job(a)
        self.assertEqual(len([c for c in self.host.cmds if c[0] == "systemd-run"]), 1)

    def test_launch_failure_reports_125(self):
        a = self.mk()
        self.host.fail_systemd_run = True
        self.start_job(a)
        a.tick()
        self.assertEqual([(r["job_id"], r["rc"]) for r in self.coord.reports()], [(7, 125)])
        self.assertEqual(a.allocs, {})

    def test_preempt_creates_signal_file_and_never_kills(self):
        a = self.mk()
        self.start_job(a)
        sig = self.spool / "signals/7/LEASE-END-REQUESTED"
        self.assertFalse(sig.exists())
        self.coord.next_reply = {"preempt": [7, 99]}
        a.tick()
        self.assertTrue(sig.exists())
        self.assertFalse((self.spool / "signals/99").exists())
        self.assertEqual(self.host.systemctl("stop"), [])
        self.assertEqual(self.host.signals, [])
        self.assertEqual(list(a.allocs), [7])

    def test_cancel_stops_only_own_unit_and_reports_cancelled(self):
        a = self.mk()
        self.start_job(a)
        self.coord.next_reply = {"cancel": [7, 99]}
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), ["cq-job-7-a0.service"])
        self.assertEqual(self.coord.reports(), [])  # finish is detected on a later tick
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), ["cq-job-7-a0.service"])
        self.assertEqual(self.host.signals, [])
        self.assertEqual(self.coord.reports(), [
            {"job_id": 7, "attempt": 0, "rc": 143, "peaks": {"peak_ram": 0, "peak_vram": 0}, "wall_s": 0.0, "cancelled": True}])

    def test_systemctl_stop_is_no_block(self):
        a = self.mk()
        self.start_job(a)
        self.coord.next_reply = {"cancel": [7]}
        a.tick()
        stops = [c for c in self.host.cmds if c[:3] == ["systemctl", "--user", "stop"]]
        self.assertEqual(stops, [["systemctl", "--user", "stop", "--no-block", "cq-job-7-a0.service"]])
        self.host.units["cq-job-8-a0.service"] = {"SubState": "exited"}
        self.start_job(a, jid=8)
        a.tick()
        stops = [c for c in self.host.cmds if c[:3] == ["systemctl", "--user", "stop"] and c[-1].endswith("8-a0.service")]
        self.assertEqual(stops, [["systemctl", "--user", "stop", "--no-block", "cq-job-8-a0.service"]])

    def test_stopping_alloc_stays_in_heartbeat_until_gone(self):
        a = self.mk()
        self.start_job(a)
        a.terminate(a.allocs[7], "cancel")
        self.host.units["cq-job-7-a0.service"] = {"ActiveState": "deactivating", "SubState": "stop-sigterm"}
        a.tick()
        self.assertEqual([x["job_id"] for x in self.coord.heartbeats()[-1]["allocs"]], [7])
        self.assertEqual(self.coord.reports(), [])
        self.host.units["cq-job-7-a0.service"] = {"ActiveState": "inactive", "SubState": "dead"}
        a.tick()
        self.assertEqual([r["rc"] for r in self.coord.reports()], [143])
        self.assertEqual(self.coord.heartbeats()[-1]["allocs"], [])

    def test_heartbeat_lists_live_allocs_every_time(self):
        a = self.mk()
        a.tick()
        self.assertEqual(self.coord.heartbeats()[-1]["allocs"], [])
        self.assertEqual(self.coord.heartbeats()[-1]["stats"]["allocs"], [])
        self.start_job(a, jid=7)
        self.start_job(a, jid=8)
        a.tick()
        hb = self.coord.heartbeats()[-1]
        self.assertEqual(sorted(x["job_id"] for x in hb["allocs"]), [7, 8])
        self.assertEqual(hb["stats"]["allocs"], hb["allocs"])

    def test_http_timeout_is_35_seconds(self):
        seen = []

        class R:
            def __enter__(self):
                return self

            def __exit__(self, *e):
                return False

            def read(self):
                return b"{}"

        def fake_urlopen(req, timeout=None):
            seen.append(timeout)
            return R()

        a = self.mk()
        with mock.patch.object(agent.urllib.request, "urlopen", fake_urlopen):
            a.post("/agent/heartbeat", {})
        self.assertEqual(seen, [35])

    def test_failing_start_does_not_skip_the_rest(self):
        a = self.mk()
        self.host.fail_units = {"cq-job-7-a0"}
        self.coord.next_reply = {"start": [
            {"id": 7, "argv": ["true"], "effective_ram": 10},
            {"id": 8, "argv": ["true"], "effective_ram": 10},
            {"id": 9, "effective_ram": 10},  # no argv
            {"id": 10, "argv": ["true"], "effective_ram": 10}]}
        a.tick()
        self.assertEqual(sorted(a.allocs), [8, 10])
        a.tick()
        self.assertEqual(sorted((r["job_id"], r["rc"]) for r in self.coord.reports()), [(7, 125), (9, 125)])

    def test_start_exception_in_one_spec_is_contained(self):
        a = self.mk()
        self.coord.next_reply = {"start": [{"argv": ["true"]}, {"id": 8, "argv": ["true"], "effective_ram": 10}]}
        a.tick()
        self.assertEqual(list(a.allocs), [8])

    def test_missing_or_zero_cap_is_refused_never_infinity(self):
        a = self.mk()
        self.coord.next_reply = {"start": [
            {"id": 1, "argv": ["true"]},
            {"id": 2, "argv": ["true"], "effective_ram": 0},
            {"id": 3, "argv": ["true"], "effective_ram": None},
            {"id": 4, "argv": ["true"], "ram_req": 5 * GIB},  # ram_req is not a cap
            {"id": 5, "argv": ["true"], "effective_ram": -1}]}
        a.tick()
        a.tick()
        self.assertEqual([c for c in self.host.cmds if c[0] == "systemd-run"], [])
        self.assertFalse(any("infinity" in x for c in self.host.cmds for x in c))
        self.assertEqual(sorted((r["job_id"], r["rc"]) for r in self.coord.reports()),
                         [(i, 125) for i in range(1, 6)])
        self.assertEqual(a.allocs, {})
        self.assertFalse((self.spool / "signals/1").exists())

    def test_mac_also_refuses_missing_cap(self):
        a = self.mk("darwin")
        self.coord.next_reply = {"start": [{"id": 3, "argv": ["sleep", "9"]}]}
        a.tick()
        self.assertEqual(self.host.spawned, [])
        a.tick()
        self.assertEqual([(r["job_id"], r["rc"]) for r in self.coord.reports()], [(3, 125)])

    def test_outbox_drops_only_400_404_409(self):
        a = self.mk()
        for code in (400, 404, 409):
            self.coord.report_status = code
            a.report(7, 0, {"peak_ram": 0, "peak_vram": 0}, 1)
            a.tick()
            self.assertEqual((self.spool / "outbox.jsonl").read_text(), "", code)
        self.assertEqual(len(self.coord.reports()), 3)

    def test_outbox_keeps_report_on_401_403_5xx_and_blocks_later_ones(self):
        a = self.mk()
        for code in (401, 403, 500, 502):
            self.coord.report_status = code
            n = len(self.coord.heartbeats())
            a.report(7, 0, {"peak_ram": 0, "peak_vram": 0}, 1)
            a.report(8, 0, {"peak_ram": 0, "peak_vram": 0}, 1)
            a.tick()
            lines = (self.spool / "outbox.jsonl").read_text().splitlines()
            self.assertEqual([json.loads(x)["job_id"] for x in lines][:2], [7, 8], code)
            self.assertEqual(len(self.coord.heartbeats()), n, code)  # nothing sent past the stuck report
            (self.spool / "outbox.jsonl").write_text("")
        self.coord.report_status = 200
        a.report(7, 0, {"peak_ram": 0, "peak_vram": 0}, 1)
        a.tick()
        self.assertEqual((self.spool / "outbox.jsonl").read_text(), "")

    def test_gpu_lock_probe_reports_outside_gpu_busy(self):
        lock = self.t / "gpu.lock"
        lock.write_text("")
        a = self.mk(gpu=True, gpu_locks=[str(self.t / "missing.lock"), str(lock)])
        self.assertFalse(a.stats()["outside_gpu_busy"])
        fd = os.open(lock, os.O_RDWR)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX)
        self.assertTrue(a.stats()["outside_gpu_busy"])
        a.tick()
        self.assertTrue(self.coord.heartbeats()[-1]["stats"]["outside_gpu_busy"])
        fcntl.flock(fd, fcntl.LOCK_UN)
        self.assertFalse(a.stats()["outside_gpu_busy"])

    def test_gpu_lock_probe_ignored_while_we_hold_a_gpu_alloc(self):
        lock = self.t / "gpu.lock"
        lock.write_text("")
        a = self.mk(gpu=True, gpu_locks=[str(lock)])
        self.start_job(a, gpu=True)
        fd = os.open(lock, os.O_RDWR)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX)
        self.assertFalse(a.stats()["outside_gpu_busy"])

    def test_no_gpu_locks_means_no_probe(self):
        self.assertFalse(self.mk(gpu=True).stats()["outside_gpu_busy"])


class AttemptTests(Base):
    def saved(self):
        return json.loads((self.spool / "allocs.json").read_text())

    def test_unit_name_carries_attempt(self):
        a = self.mk()
        self.start_job(a, attempt=2)
        run = next(c for c in self.host.cmds if c[0] == "systemd-run")
        self.assertEqual(run[run.index("--unit") + 1], "cq-job-7-a2")
        self.assertEqual(a.allocs[7]["unit"], "cq-job-7-a2.service")
        self.assertEqual(self.saved()[0]["attempt"], 2)

    def test_attempt_in_heartbeat_allocs_and_report(self):
        a = self.mk()
        self.start_job(a, attempt=3)
        a.tick()
        self.assertEqual(self.coord.heartbeats()[-1]["allocs"][0]["attempt"], 3)
        self.host.units["cq-job-7-a3.service"] = {"SubState": "exited", "ExecMainStatus": "0", "ExecMainCode": "1"}
        a.tick()
        self.assertEqual([(r["job_id"], r["attempt"], r["rc"]) for r in self.coord.reports()], [(7, 3, 0)])

    def test_launch_failure_report_carries_attempt(self):
        a = self.mk()
        self.host.fail_systemd_run = True
        self.start_job(a, attempt=4)
        a.tick()
        self.assertEqual([(r["attempt"], r["rc"]) for r in self.coord.reports()], [(4, 125)])
        self.assertEqual(self.saved(), [])  # the intent is removed again

    def test_intent_is_saved_before_launch(self):
        a = self.mk()
        seen = []
        real = self.host.run

        def run(argv, timeout=30):
            if argv[0] == "systemd-run":
                seen.append(self.saved())
            return real(argv, timeout)

        self.host.run = run
        self.start_job(a, attempt=1)
        self.assertEqual(len(seen[0]), 1)
        self.assertEqual({k: seen[0][0][k] for k in ("job_id", "attempt", "unit", "state")},
                         {"job_id": 7, "attempt": 1, "unit": "cq-job-7-a1.service", "state": "launching"})
        self.assertNotIn("state", self.saved()[0])  # promoted to a plain allocation

    def test_crash_between_launch_and_save_is_adopted_on_restart(self):
        a = self.mk()
        real = a.save_allocs
        calls = []

        def crashing_save():
            calls.append(1)
            if len(calls) == 2:  # the save that follows systemd-run
                raise SystemExit("crash")
            real()

        a.save_allocs = crashing_save
        self.coord.next_reply = {"start": [{"id": 7, "attempt": 1, "argv": ["x"], "effective_ram": 1000}]}
        with self.assertRaises(SystemExit):
            a.tick()
        self.assertEqual([x["state"] for x in self.saved()], ["launching"])
        self.assertIn("cq-job-7-a1.service", self.host.listed)
        b = self.mk()
        self.assertEqual(list(b.allocs), [7])
        self.assertNotIn("state", b.allocs[7])
        self.assertEqual(self.host.systemctl("stop"), [])  # adopted, not stopped
        self.assertEqual(sum(1 for c in self.host.cmds if c[0] == "systemd-run"), 1)  # and not relaunched
        self.host.units["cq-job-7-a1.service"] = {"SubState": "exited", "ExecMainStatus": "0", "ExecMainCode": "1"}
        b.tick()
        self.assertEqual([(r["job_id"], r["attempt"], r["rc"]) for r in self.coord.reports()], [(7, 1, 0)])

    def test_intent_without_a_listed_unit_is_dropped(self):
        a = self.mk()
        a.intents[7] = {"job_id": 7, "attempt": 0, "unit": "cq-job-7-a0.service", "state": "launching",
                        "signal_dir": str(self.spool / "signals/7")}
        a.save_allocs()
        b = self.mk()
        self.assertEqual((b.allocs, b.intents), ({}, {}))
        self.assertEqual(self.saved(), [])
        self.assertEqual(self.host.systemctl("stop"), [])

    def test_unmatched_cq_job_units_are_stopped_others_untouched(self):
        self.start_job(self.mk(), jid=7, attempt=0)
        self.host.listed |= {"cq-job-9-a0.service", "cq-job-7-a1.service", "other-service-a.service",
                             "cq-agent.service", "other-service-b.service"}
        b = self.mk()
        self.assertEqual(list(b.allocs), [7])
        stops = [c for c in self.host.cmds if c[:3] == ["systemctl", "--user", "stop"]]
        self.assertEqual(sorted(c[-1] for c in stops), ["cq-job-7-a1.service", "cq-job-9-a0.service"])
        self.assertTrue(all(c[3] == "--no-block" for c in stops))
        self.assertEqual(self.host.signals, [])

    def test_unit_list_failure_stops_nothing(self):
        self.start_job(self.mk())
        self.host.listed.add("cq-job-9-a0.service")
        real = self.host.run
        self.host.run = lambda argv, timeout=30: (1, "") if "list-units" in argv else real(argv, timeout)
        b = self.mk()
        self.assertEqual(list(b.allocs), [7])
        self.assertEqual(self.host.systemctl("stop"), [])

    def test_unit_list_failure_keeps_intents_as_allocations(self):
        a = self.mk()
        a.intents[7] = {"job_id": 7, "attempt": 0, "unit": "cq-job-7-a0.service", "state": "launching",
                        "signal_dir": str(self.spool / "signals/7")}
        a.save_allocs()
        real = self.host.run
        self.host.run = lambda argv, timeout=30: (1, "") if "list-units" in argv else real(argv, timeout)
        b = self.mk()
        self.assertEqual((list(b.allocs), b.intents), ([7], {}))
        self.assertNotIn("state", b.allocs[7])
        self.assertNotIn("state", self.saved()[0])
        b.tick()  # no such unit: reported as a launch failure
        self.assertEqual([(r["job_id"], r["rc"]) for r in self.coord.reports()], [(7, 125)])

    def test_launch_failure_stops_the_attempts_unit(self):
        a = self.mk()
        self.host.fail_systemd_run = True
        self.start_job(a, attempt=4)
        stops = [c for c in self.host.cmds if c[:3] == ["systemctl", "--user", "stop"]]
        self.assertEqual([c[3:] for c in stops], [["--no-block", "cq-job-7-a4.service"]])

    def test_spec_gpu_locks_are_ignored(self):
        a = self.mk(gpu=True)
        self.start_job(a, gpu=True, gpu_locks=["/tmp/evil.lock"])
        run = next(c for c in self.host.cmds if c[0] == "systemd-run")
        self.assertEqual(run[run.index("--") + 1:], ["python3", "x.py"])

    def test_gpu_locks_come_from_agent_json_not_the_spec(self):
        a = self.mk(gpu=True, gpu_locks=["/tmp/gpu0.lock"])
        self.start_job(a, gpu=True, gpu_locks=["/tmp/evil.lock"])
        run = next(c for c in self.host.cmds if c[0] == "systemd-run")
        self.assertEqual(run[run.index("--") + 1:], ["flock", "-x", "/tmp/gpu0.lock", "python3", "x.py"])


class GuardTests(Base):
    def test_vram_over_cap_stops_gpu_alloc_and_reports_fault(self):
        a = self.mk(gpu=True, vram_cap=1000)
        self.start_job(a, gpu=True)
        self.set_vram_used(2000)
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), ["cq-job-7-a0.service"])
        a.tick()
        r = self.coord.reports()[0]
        self.assertEqual((r["job_id"], r["rc"], r["fault"]), (7, 137, "vram"))
        self.assertEqual(r["peaks"]["peak_vram"], 2000)

    def test_vram_under_cap_leaves_alloc_alone(self):
        a = self.mk(gpu=True, vram_cap=1000)
        self.start_job(a, gpu=True)
        self.set_vram_used(900)
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), [])

    def test_vram_guard_ignores_cpu_alloc(self):
        a = self.mk(gpu=True, vram_cap=1000)
        self.start_job(a, gpu=False)
        self.set_vram_used(2000)
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), [])

    def test_new_kernel_fault_line_stops_gpu_alloc(self):
        a = self.mk(gpu=True)
        self.start_job(a, gpu=True)
        self.host.journal = (0, "kernel: amdgpu 0000:03:00.0: GPU reset begin!\n")
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), ["cq-job-7-a0.service"])
        a.tick()
        self.assertEqual(self.coord.reports()[0]["fault"], "kernel")
        self.assertIn(["sudo", "-n", "journalctl", "-k", "-b", "--no-pager"], self.host.cmds)

    def test_kernel_lines_from_before_agent_start_are_ignored(self):
        self.host.journal = (0, "kernel: amdgpu: page fault old\nkernel: oom-kill:constraint=x\n")
        a = self.mk(gpu=True)
        self.start_job(a, gpu=True)
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), [])
        self.host.journal = (0, self.host.journal[1] + "HSA_STATUS_ERROR_EXCEPTION\n")
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), ["cq-job-7-a0.service"])

    def test_kernel_guard_skipped_without_sudo(self):
        self.host.journal = (1, "sudo: a password is required")
        a = self.mk(gpu=True)
        self.start_job(a, gpu=True)
        a.tick()
        a.tick()
        self.assertEqual(self.host.systemctl("stop"), [])
        self.assertEqual(len([c for c in self.host.cmds if c[0] == "sudo"]), 1)


class MacTests(Base):
    def mac_start(self, a, ram=1000, jid=3):
        self.start_job(a, jid=jid, effective_ram=ram, argv=["sleep", "9"])
        return self.host.spawned[-1]

    def test_start_runs_under_caffeinate_in_own_session(self):
        a = self.mk("darwin")
        sp = self.mac_start(a)
        self.assertEqual(sp["argv"][:2], ["/bin/sh", "-c"])
        self.assertIn("caffeinate -i", sp["argv"][2])
        self.assertEqual(sp["argv"][-2:], ["sleep", "9"])
        self.assertEqual(sp["cwd"], "/work")
        self.assertEqual(sp["env"]["CQ_JOB_ID"], "3")
        self.assertEqual(sp["env"]["CQ_SIGNAL_DIR"], f"{self.spool}/signals/3")
        self.assertEqual([c for c in self.host.cmds if c[0] == "systemd-run"], [])

    def test_finish_reads_rc_file(self):
        a = self.mk("darwin")
        sp = self.mac_start(a)
        (self.spool / "signals/3/rc").write_text("0\n")
        sp["proc"].returncode = 0
        a.tick()
        self.assertEqual([(r["job_id"], r["rc"]) for r in self.coord.reports()], [(3, 0)])

    def test_cap_exceeded_term_then_kill_after_grace(self):
        a = self.mk("darwin")
        sp = self.mac_start(a)
        pid = sp["proc"].pid
        self.host.footprints[pid] = 1200
        self.host.footprints[pid + 1] = 300  # child counts toward the cap
        a.tick()
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM)])
        self.clock[0] += 10
        a.tick()
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM)])
        self.clock[0] += 15
        a.tick()
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM), (pid, signal.SIGKILL)])
        sp["proc"].returncode = -9
        self.host.groups.clear()
        a.tick()
        r = self.coord.reports()[0]
        self.assertEqual((r["job_id"], r["rc"]), (3, 137))
        self.assertNotIn("fault", r)

    def test_stop_waits_for_the_whole_group_before_reporting(self):
        a = self.mk("darwin")
        sp = self.mac_start(a)
        pid = sp["proc"].pid
        self.coord.next_reply = {"cancel": [3]}
        a.tick()
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM)])
        sp["proc"].returncode = 0  # leader exits on SIGTERM, a child (pid+1) lingers in the group
        for _ in range(3):
            self.clock[0] += 5
            a.tick()
        self.assertEqual(self.coord.reports(), [])
        self.assertEqual(list(a.allocs), [3])
        self.assertEqual([x["job_id"] for x in self.coord.heartbeats()[-1]["allocs"]], [3])
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM)])
        self.clock[0] += 10  # past 20 s since SIGTERM
        a.tick()
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM), (pid, signal.SIGKILL)])
        self.assertEqual(self.coord.reports(), [])  # SIGKILL sent, group not yet confirmed gone
        self.host.groups.clear()
        a.tick()
        self.assertEqual([(r["job_id"], r["rc"], r["cancelled"]) for r in self.coord.reports()],
                         [(3, 143, True)])
        self.assertEqual(a.allocs, {})
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM), (pid, signal.SIGKILL)])

    def test_stop_with_leader_and_children_gone_before_kill_needs_no_sigkill(self):
        a = self.mk("darwin")
        sp = self.mac_start(a)
        pid = sp["proc"].pid
        self.host.footprints[pid] = 5000
        a.tick()
        sp["proc"].returncode = -15
        self.host.groups.clear()
        self.clock[0] += 1
        a.tick()
        self.assertEqual([r["rc"] for r in self.coord.reports()], [137])
        self.assertEqual(self.host.signals, [(pid, signal.SIGTERM)])

    def test_real_host_group_alive_tracks_process_group(self):
        p = subprocess.Popen(["sleep", "30"], start_new_session=True)
        self.addCleanup(p.wait)
        self.addCleanup(p.kill)
        host = agent.Host()
        self.assertTrue(host.group_alive(p.pid))
        p.kill()
        p.wait()
        self.assertFalse(host.group_alive(p.pid))

    def test_under_cap_is_left_alone(self):
        a = self.mk("darwin")
        sp = self.mac_start(a)
        self.host.footprints[sp["proc"].pid] = 900
        a.tick()
        self.assertEqual(self.host.signals, [])

    def test_memory_pressure_level_4_kills_largest(self):
        a = self.mk("darwin")
        s1 = self.mac_start(a, ram=10_000, jid=3)
        s2 = self.mac_start(a, ram=10_000, jid=4)
        self.host.footprints[s1["proc"].pid] = 100
        self.host.footprints[s2["proc"].pid] = 500
        self.host.pressure = 4
        a.tick()
        self.assertEqual(self.host.signals, [(s2["proc"].pid, signal.SIGTERM)])


class OwnPidTests(Base):
    def entry(self, pid, ident):
        return {"job_id": 5, "gpu": False, "cap": 1000, "signal_dir": str(self.spool / "signals/5"),
                "started": 1000.0, "current_ram": 0, "current_vram": 0, "peak_ram": 0, "peak_vram": 0,
                "pid": pid, "pgid": pid, "ident": ident}

    def seed(self, pid, ident):
        self.spool.mkdir(parents=True)
        (self.spool / "allocs.json").write_text(json.dumps([self.entry(pid, ident)]))

    def test_cancel_unknown_job_signals_nothing(self):
        a = self.mk("darwin")
        self.start_job(a, jid=3, argv=["sleep", "9"])
        self.coord.next_reply = {"cancel": [99], "preempt": [98]}
        a.tick()
        self.assertEqual(self.host.signals, [])

    def test_reused_pid_is_never_signalled(self):
        self.seed(4242, "Mon Oct 19 09:00:00 2026")
        self.host.lstart[4242] = "Tue Oct 20 11:11:11 2026"  # someone else now owns this pid
        self.host.groups[4242] = 4242
        a = self.mk("darwin")
        self.coord.next_reply = {"cancel": [5]}
        a.tick()
        self.assertEqual(self.host.signals, [])
        self.assertEqual([(r["job_id"], r["rc"]) for r in self.coord.reports()], [(5, 1)])

    def test_reattached_process_with_matching_identity_can_be_cancelled(self):
        self.seed(4242, "Mon Oct 19 09:00:00 2026")
        self.host.lstart[4242] = "Mon Oct 19 09:00:00 2026"
        self.host.groups[4242] = 4242
        a = self.mk("darwin")
        self.coord.next_reply = {"cancel": [5]}
        a.tick()
        self.assertEqual(self.host.signals, [(4242, signal.SIGTERM)])

    def test_linux_only_touches_own_units(self):
        a = self.mk()
        self.start_job(a)
        self.coord.next_reply = {"cancel": [7, 1, 2]}
        a.tick()
        touched = {c[-1] for c in self.host.cmds
                   if c[:2] == ["systemctl", "--user"] and c[2] not in ("show", "list-units")}
        self.assertEqual(touched, {"cq-job-7-a0.service"})
        self.assertEqual(self.host.signals, [])


class HwTests(Base):
    def test_linux_hw(self):
        proc, dev = self.t / "proc", self.t / "sys/class/drm/card0/device"
        proc.mkdir()
        dev.mkdir(parents=True)
        (proc / "meminfo").write_text("MemTotal:       16000 kB\n")
        (proc / "cpuinfo").write_text("processor\t: 0\nmodel name\t: Test CPU @ 3GHz\n")
        (dev / "mem_info_vram_total").write_text("17163091968\n")
        hw = agent.hw_info(self.host, "linux", str(proc), str(self.t / "sys"))
        self.assertEqual(hw, {"os": "linux", "ram_total": 16000 * 1024, "cpu": "Test CPU @ 3GHz",
                              "gpus": [{"name": "card0", "vram_total": 17163091968}]})

    def test_macos_hw(self):
        hw = agent.hw_info(self.host, "darwin")
        self.assertEqual(hw, {"os": "darwin", "ram_total": 17179869184, "cpu": "Apple M2", "gpus": []})


@unittest.skipUnless(sys.platform.startswith("linux"), "linux installer path")
class InstallerTests(Base):
    def run_install(self, *args, gpus=None):
        """gpus: None runs the real `agent.py --hw`; otherwise a stub python3 fakes its gpu list."""
        home, fakebin = self.t / "home", self.t / "fakebin"
        home.mkdir(exist_ok=True)
        fakebin.mkdir(exist_ok=True)
        fake = fakebin / "systemctl"
        fake.write_text(f'#!/bin/sh\necho "$@" >> "{self.t}/systemctl.log"\n')
        fake.chmod(0o755)
        if gpus is not None:
            hw = json.dumps({"os": "linux", "ram_total": 1, "cpu": "x",
                             "gpus": [{"name": "g", "vram_total": 1}] * gpus})
            py = fakebin / "python3"
            py.write_text(f"#!/bin/sh\ncase \"$*\" in *--hw*) echo '{hw}';; "
                          f"*) exec {sys.executable} \"$@\";; esac\n")
            py.chmod(0o755)
        env = {"HOME": str(home), "PATH": f"{fakebin}:{os.environ['PATH']}", "no_proxy": "127.0.0.1"}
        return subprocess.run(["bash", str(ROOT / "install/install.sh"), *args], env=env,
                              capture_output=True, text=True, timeout=60)

    def test_syntax(self):
        self.assertEqual(subprocess.run(["bash", "-n", str(ROOT / "install/install.sh")]).returncode, 0)

    def test_usage_error(self):
        self.assertEqual(self.run_install(self.coord.url, "code").returncode, 2)

    def test_install_registers_and_writes_private_config(self):
        r = self.run_install(self.coord.url, "invite-123", "ai")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("registered ai as pending; ask the Owner to run: cq node approve ai", r.stdout)
        home = self.t / "home"
        reg = next(b for p, b, _ in self.coord.events if p == "/nodes/register")
        self.assertEqual((reg["name"], reg["code"]), ("ai", "invite-123"))
        self.assertIn("ram_total", reg["hw"])
        conf = home / ".config/compute-queue/agent.json"
        self.assertEqual(stat.S_IMODE(conf.stat().st_mode), 0o600)
        want = {"coordinator": self.coord.url, "token": "tok-secret", "node": "ai", "os": "linux"}
        if reg["hw"]["gpus"]:
            want["gpu_locks"] = ["/tmp/compute-queue-gpu.lock"]
        self.assertEqual(json.loads(conf.read_text()), want)
        self.assertTrue((home / ".local/share/compute-queue/agent/agent.py").exists())
        unit = (home / ".config/systemd/user/cq-agent.service").read_text()
        self.assertIn("Restart=always", unit)
        self.assertNotIn("tok-secret", unit)
        log = (self.t / "systemctl.log").read_text()
        self.assertIn("--user enable --now cq-agent.service", log)
        self.assertNotIn("tok-secret", log)

    def conf(self):
        return json.loads((self.t / "home/.config/compute-queue/agent.json").read_text())

    def test_gpu_node_gets_default_gpu_lock(self):
        r = self.run_install(self.coord.url, "c", "ai", gpus=1)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.conf()["gpu_locks"], ["/tmp/compute-queue-gpu.lock"])

    def test_node_without_gpu_gets_no_gpu_lock(self):
        r = self.run_install(self.coord.url, "c", "ai", gpus=0)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn("gpu_locks", self.conf())

    def test_gpu_lock_option_overrides_default_in_either_position(self):
        r = self.run_install("--gpu-lock", "/run/x.lock", self.coord.url, "c", "ai", gpus=1)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.conf()["gpu_locks"], ["/run/x.lock"])
        r = self.run_install(self.coord.url, "c", "ai", "--gpu-lock=/run/y.lock", gpus=0)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.conf()["gpu_locks"], ["/run/y.lock"])
        self.assertEqual(self.conf()["token"], "tok-secret")

    def test_bad_gpu_lock_option_is_a_usage_error(self):
        self.assertEqual(self.run_install("--gpu-lock", "rel.lock", self.coord.url, "c", "ai").returncode, 2)
        self.assertEqual(self.run_install(self.coord.url, "c", "ai", "--gpu-lock").returncode, 2)
        self.assertFalse(any(p == "/nodes/register" for p, _, _ in self.coord.events))


if __name__ == "__main__":
    unittest.main()
