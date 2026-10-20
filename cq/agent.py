#!/usr/bin/env python3
"""Node Agent: pulls work from the Coordinator, runs it within limits, reports back."""
import argparse
import fcntl
import glob
import json
import logging
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

HEARTBEAT_S = 10
HTTP_TIMEOUT_S = 35  # above the Coordinator's 30 s SQLite busy timeout
DROP_CODES = (400, 404, 409)  # a report the Coordinator will never accept
KILL_GRACE_S = 20
VRAM_CAP = 19.5e9
LAUNCH_FAILED_RC = 125
KERNEL_SCAN_S = 60  # periodic scans; startup and GPU admission explicitly probe
FAULT_RE = re.compile(r"amdgpu.*(page fault|reset|timeout|hang)|HSA_STATUS_ERROR|oom-kill", re.I)
UNITS = {"B": 1, "KB": 2**10, "MB": 2**20, "GB": 2**30}

log = logging.getLogger("cq-agent")


class Unreachable(Exception):
    """Coordinator did not answer (network error or 5xx)."""


class ApiError(Exception):
    """Coordinator answered 4xx."""

    def __init__(self, msg, code=0):
        super().__init__(msg)
        self.code = code


class Host:
    """The only place the agent touches the OS: commands, process spawn, signals."""

    def run(self, argv, timeout=30):
        """Run a command; return (returncode, stdout). Missing binaries give 127."""
        try:
            p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError) as e:
            return 127, str(e)
        return p.returncode, p.stdout

    def spawn(self, argv, cwd, env, log_path):
        """Start argv in its own session (process group == pid)."""
        out = open(log_path, "ab")
        try:
            return subprocess.Popen(argv, cwd=cwd or None, env=env, start_new_session=True,
                                    stdin=subprocess.DEVNULL, stdout=out, stderr=out)
        finally:
            out.close()

    def killpg(self, pgid, sig):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass

    def group_alive(self, pgid):
        """True while any process is left in the process group."""
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            pass
        return True


def read(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def read_int(path):
    try:
        return int(read(path).strip())
    except ValueError:
        return 0


def meminfo(proc, key):
    m = re.search(rf"^{key}:\s+(\d+) kB", read(f"{proc}/meminfo"), re.M)
    return int(m.group(1)) * 1024 if m else 0


def drm_sum(sysfs, name):
    return sum(read_int(p) for p in glob.glob(f"{sysfs}/class/drm/card[0-9]*/device/{name}"))


def hw_info(host, os_name, proc="/proc", sysfs="/sys"):
    """Hardware summary sent at registration."""
    if os_name == "darwin":
        total = int(host.run(["sysctl", "-n", "hw.memsize"])[1].strip() or 0)
        cpu = host.run(["sysctl", "-n", "machdep.cpu.brand_string"])[1].strip()
        return {"os": os_name, "ram_total": total, "cpu": cpu, "gpus": []}
    m = re.search(r"^model name\s*:\s*(.+)$", read(f"{proc}/cpuinfo"), re.M)
    gpus = []
    for p in sorted(glob.glob(f"{sysfs}/class/drm/card[0-9]*/device/mem_info_vram_total")):
        dev = os.path.dirname(p)
        name = read(f"{dev}/product_name").strip() or dev.split("/")[-2]
        gpus.append({"name": name, "vram_total": read_int(p)})
    return {"os": os_name, "ram_total": meminfo(proc, "MemTotal"),
            "cpu": m.group(1) if m else "", "gpus": gpus}


def parse_props(out):
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def atomic_write(path, text):
    """Durable rename: launch intent must reach disk before any child can run."""
    with open(path + ".tmp", "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    fd = os.open(os.path.dirname(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def group_running(host, pgid):
    """Ignore zombies (no resources/work); a failed ps is unknown, never drained."""
    rc, out = host.run(["ps", "-axo", "pid=,pgid=,stat="])
    if rc:
        return None
    rows = [row.split() for row in out.splitlines()]
    if not rows or any(len(row) != 3 for row in rows):
        return None
    return any(row[1] == str(pgid) and not row[2].startswith("Z") for row in rows)


def mac_runner(sdir, argv, lock_path=None):
    """Single-file supervisor outside the workload group, surviving agent restarts.

    Do NOT reap the direct child until its whole group drains. Even after it exits,
    its zombie reserves the PID/PGID, so TERM/KILL cannot target a reused group.
    The exclusive flock stays here, outside the group targeted by those signals.
    """
    host = Host()
    state = {"pid": os.getpid(), "ident": host.run(
        ["ps", "-p", str(os.getpid()), "-o", "lstart="])[1].strip()}
    status_path = f"{sdir}/runner.json"
    atomic_write(status_path, json.dumps(state))
    # Service/agent termination must not drop the GPU lock under running children.
    signal.signal(signal.SIGTERM, lambda *_: None)
    signal.signal(signal.SIGINT, lambda *_: None)
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)  # never auto-reap our PID reservation
    lock_fd = None
    child = None
    gate_r, gate_w = os.pipe()
    ready_r, ready_w = os.pipe()
    try:
        if lock_path:
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if os.path.exists(f"{sdir}/stop"):
                        state.update(done=True, rc=143)
                        atomic_write(status_path, json.dumps(state))
                        os.close(lock_fd)
                        return 0
                    time.sleep(0.1)
        if os.path.exists(f"{sdir}/stop"):
            state.update(done=True, rc=143)
            atomic_write(status_path, json.dumps(state))
            if lock_fd is not None:
                os.close(lock_fd)
            return 0
        child = os.fork()
        if child == 0:
            try:
                os.close(gate_w)
                os.close(ready_r)
                if lock_fd is not None:
                    os.close(lock_fd)
                os.setsid()
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                os.write(ready_w, b"1")
                os.close(ready_w)
                # EOF means supervisor died before persisting our group identity.
                if os.read(gate_r, 1) != b"1":
                    os._exit(LAUNCH_FAILED_RC)
                os.close(gate_r)
                cmd = (["caffeinate", "-i"] if sys.platform == "darwin" else []) + argv
                os.execvpe(cmd[0], cmd, os.environ)
            except BaseException:
                os._exit(LAUNCH_FAILED_RC)
        os.close(gate_r)
        gate_r = None
        os.close(ready_w)
        ready_w = None
        if os.read(ready_r, 1) != b"1":
            raise OSError("worker failed before setsid handshake")
        state["pgid"] = child
        atomic_write(status_path, json.dumps(state))
        os.write(gate_w, b"1")
    except Exception:
        if child is None:
            state.update(done=True, rc=LAUNCH_FAILED_RC)
            atomic_write(status_path, json.dumps(state))
        else:
            # Closing the gate prevents exec if setup/persistence failed.
            log.exception("Mac runner setup failed")
    finally:
        for fd in (gate_r, gate_w, ready_r, ready_w):
            if fd is not None:
                os.close(fd)
    if child is not None:
        term_at, killed = None, False
        # ps enumerates PIDs before reading their state. A parent may fork and
        # exit during that snapshot, leaving a live child outside its PID list.
        # Confirm an empty group with a fresh enumeration while retaining the
        # unreaped leader/PGID reservation and the GPU lock.
        while (group_running(host, child) is not False
               or group_running(host, child) is not False):
            if os.path.exists(f"{sdir}/stop"):
                try:
                    if term_at is None:
                        host.killpg(child, signal.SIGTERM)
                        term_at = time.monotonic()
                    elif not killed and time.monotonic() - term_at >= KILL_GRACE_S:
                        host.killpg(child, signal.SIGKILL)
                        killed = True
                except OSError:
                    # Signal failure must not unwind the supervisor and drop its lock.
                    log.exception("cannot stop Mac workload group %s; retaining lock", child)
                    time.sleep(1)
            time.sleep(0.1)
        _, status = os.waitpid(child, 0)  # only now may the PID be reused
        rc = os.waitstatus_to_exitcode(status)
        rc = 128 - rc if rc < 0 else rc
        atomic_write(f"{sdir}/rc", str(rc) + "\n")
        state.update(done=True, rc=rc)
        atomic_write(status_path, json.dumps(state))
    if lock_fd is not None:
        os.close(lock_fd)
    return 0


class Agent:
    def __init__(self, cfg, spool, host=None, now=time.time, proc="/proc", sysfs="/sys",
                 cgroup="/sys/fs/cgroup"):
        self.cfg, self.spool, self.now = cfg, spool, now
        self.host = host or Host()
        self.proc, self.sysfs, self.cgroup = proc, sysfs, cgroup
        self.mac = cfg.get("os", "linux").lower() in ("darwin", "mac", "macos")
        self.vram_cap = cfg.get("vram_cap", VRAM_CAP)
        self.procs = {}  # job_id -> Popen, only for children started by this agent process
        self.journal_ok = True
        self.kernel_cursor = None
        self.kernel_scan_at = None
        self.kernel_fault_latched = os.path.exists(f"{spool}/KERNEL-FAULT.json")
        os.makedirs(f"{spool}/signals", exist_ok=True)
        os.makedirs(f"{spool}/logs", exist_ok=True)
        saved = json.loads(read(f"{spool}/allocs.json") or "[]")
        self.allocs = {a["job_id"]: a for a in saved if a.get("state") != "launching"}
        self.intents = {a["job_id"]: a for a in saved if a.get("state") == "launching"}
        if self.mac:
            # A spawn may have succeeded before the allocation save. Never respawn
            # an ambiguous intent: the runner's durable handshake resolves it.
            for jid, a in list(self.intents.items()):
                self.allocs[jid] = self.alloc_from_intent(self.intents.pop(jid))
            self.save_allocs()
        else:
            self.adopt_units()
        if self.allocs:
            log.info("reattached %s", sorted(self.allocs))
        self.gpu_node = not self.mac and drm_sum(sysfs, "mem_info_vram_total") > 0
        if self.gpu_node:
            self.kernel_faults(force=True, baseline=True)  # only initial historical snapshot is baseline

    # -- coordinator transport -------------------------------------------------

    def post(self, path, body):
        req = urllib.request.Request(
            self.cfg["coordinator"].rstrip("/") + path, data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + self.cfg["token"],
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            if e.code >= 500:
                raise Unreachable(str(e)) from e
            raise ApiError(f"{e.code} {e.read()[:200]!r}", e.code) from e
        except (OSError, ValueError) as e:
            raise Unreachable(str(e)) from e

    def flush_outbox(self):
        """Send queued reports in order. Returns False if the Coordinator is unreachable."""
        path = f"{self.spool}/outbox.jsonl"
        lines = [ln for ln in read(path).splitlines() if ln.strip()]
        keep, ok = [], True
        for i, line in enumerate(lines):
            try:
                self.post("/agent/report", json.loads(line))
            except Unreachable:
                keep, ok = lines[i:], False
                break
            except ApiError as e:
                if e.code not in DROP_CODES:
                    log.warning("report kept in outbox: %s", e)
                    keep, ok = lines[i:], False
                    break
                log.warning("dropping report %s: %s", line[:120], e)
            except ValueError as e:
                log.warning("dropping report %s: %s", line[:120], e)
        if len(keep) != len(lines):
            self.write(path, "".join(ln + "\n" for ln in keep))
        return ok

    def write(self, path, text):
        atomic_write(path, text)

    def save_allocs(self):
        self.write(f"{self.spool}/allocs.json",
                   json.dumps(list(self.allocs.values()) + list(self.intents.values())))

    def alloc_from_intent(self, a):
        """Promote a launch intent to an allocation, filling any field the intent lacks."""
        a.pop("state", None)
        defaults = {"attempt": 0, "gpu": False, "cap": 0, "signal_dir": "", "started": self.now(),
                    "current_ram": 0, "current_vram": 0, "peak_ram": 0, "peak_vram": 0}
        for k, v in defaults.items():
            a.setdefault(k, v)
        return a

    def adopt_units(self):
        """Linux restart: adopt cq-job-* units that match an intent/alloc entry, stop the rest.
        Only units named cq-job-*.service are ever touched."""
        rc, out = self.host.run(["systemctl", "--user", "list-units", "cq-job-*", "--all",
                                 "--plain", "--no-legend", "--no-pager"])
        if rc != 0:
            log.warning("cannot list cq-job units (rc=%s); adopting and stopping nothing", rc)
            for jid, a in list(self.intents.items()):  # poll_linux finds the unit or reports rc 125
                self.allocs[jid] = self.alloc_from_intent(self.intents.pop(jid))
            self.save_allocs()
            return
        listed = {t for ln in out.splitlines() for t in ln.split()[:2]
                  if t.startswith("cq-job-") and t.endswith(".service")}
        for jid, a in list(self.intents.items()):
            del self.intents[jid]
            if a.get("unit") in listed:
                self.allocs[jid] = self.alloc_from_intent(a)
                log.info("adopted %s from launch intent", a["unit"])
            else:
                log.info("dropping launch intent for job %s: %s is not running", jid, a.get("unit"))
                shutil.rmtree(a.get("signal_dir") or "", ignore_errors=True)
        known = {a.get("unit") for a in self.allocs.values()}
        for unit in sorted(listed - known):
            log.warning("stopping unmatched unit %s", unit)
            self.host.run(["systemctl", "--user", "stop", "--no-block", unit], timeout=120)
        self.save_allocs()

    # -- sampling ----------------------------------------------------------------

    def mem_available(self):
        if not self.mac:
            return meminfo(self.proc, "MemAvailable")
        out = self.host.run(["vm_stat"])[1]
        ps = re.search(r"page size of (\d+) bytes", out)
        pages = sum(int(m.group(1)) for m in re.finditer(
            r"^Pages (?:free|inactive|speculative):\s+(\d+)", out, re.M))
        return pages * int(ps.group(1) if ps else 16384)

    def stats(self):
        vram = 0 if self.mac else drm_sum(self.sysfs, "mem_info_vram_used")
        own_gpu = any(a["gpu"] for a in self.allocs.values())
        if self.gpu_node and not own_gpu:
            self.kernel_faults()  # discover telemetry loss while idle, too
        busy = max([read_int(p) for p in glob.glob(
            f"{self.sysfs}/class/drm/card[0-9]*/device/gpu_busy_percent")] or [0])
        return {"mem_available": self.mem_available(), "vram_used": vram,
                "outside_gpu_busy": bool((not self.mac and (not self.journal_ok or self.kernel_fault_latched)) or
                                         (not own_gpu and (busy >= 50 or self.gpu_lock_held())))}

    def gpu_lock_held(self):
        """Non-blocking probe: True when someone else holds any configured GPU lock."""
        for path in self.cfg.get("gpu_locks") or []:
            try:
                fd = os.open(path, os.O_RDONLY)
            except OSError:
                continue
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            except OSError:
                pass
            finally:
                os.close(fd)
        return False

    def cgroup_ram(self, group):
        """Unreclaimable RAM of a cgroup: anon + kernel + shmem."""
        text = read(f"{self.cgroup}/{group.strip('/')}/memory.stat") if group else ""
        vals = dict(ln.split() for ln in text.splitlines() if len(ln.split()) == 2)
        return sum(int(vals.get(k, 0)) for k in ("anon", "kernel", "shmem")) if vals else None

    def cgroup_drained(self, group):
        """cgroup.events includes descendants; an unreadable existing group is not drained."""
        if not group:
            return True
        directory = f"{self.cgroup}/{group.strip('/')}"
        try:
            with open(f"{directory}/cgroup.events") as f:
                events = dict(line.split() for line in f if len(line.split()) == 2)
            return events.get("populated") == "0"
        except FileNotFoundError:
            return not os.path.exists(directory)
        except (OSError, ValueError):
            return False

    def footprint(self, a):
        """Sum of phys footprint over every process in the allocation's process group."""
        rows = self.host.run(["ps", "-axo", "pid=,pgid="])[1].split("\n")
        pids = [r.split()[0] for r in rows if len(r.split()) == 2 and r.split()[1] == str(a["pgid"])]
        total = 0
        for pid in pids:
            m = re.search(r"Footprint:\s*([\d.]+)\s*(B|KB|MB|GB)", self.host.run(["footprint", "-p", pid])[1])
            if m:
                total += int(float(m.group(1)) * UNITS[m.group(2)])
        return total

    def kernel_faults(self, force=False, baseline=False):
        """Fault lines logged since the previous scan, or None when no scan ran (journalctl
        unavailable, or under KERNEL_SCAN_S since the last one). Reads only new lines."""
        if not self.journal_ok:
            return None
        t = self.now()
        if not force and self.kernel_scan_at is not None and t - self.kernel_scan_at < KERNEL_SCAN_S:
            return None
        argv = ["sudo", "-n", "journalctl", "-k", "--no-pager", "--show-cursor"]
        argv += [f"--after-cursor={self.kernel_cursor}"] if self.kernel_cursor else ["-b"]
        rc, out = self.host.run(argv, timeout=60)
        if rc != 0:
            self.journal_ok = False
            log.error("GPU work blocked: sudo -n journalctl -k failed (rc=%s)", rc)
            return None
        self.kernel_scan_at = t
        n = 0
        faults = []
        for ln in out.splitlines():
            if ln.startswith("-- cursor: "):
                self.kernel_cursor = ln[len("-- cursor: "):].strip()
            elif FAULT_RE.search(ln):
                n += 1
                faults.append(ln)
        if n and not baseline:
            self.kernel_fault_latched = True
            self.write(f"{self.spool}/KERNEL-FAULT.json", json.dumps(
                {"at": t, "cursor": self.kernel_cursor, "lines": faults,
                 "policy": "new fault; manual review required; no automatic retry"}))
        return n

    # -- allocations ---------------------------------------------------------------

    def start(self, spec):
        jid = int(spec["id"])
        if jid in self.allocs or jid in self.intents:
            return
        sdir = f"{self.spool}/signals/{jid}"
        attempt = int(spec.get("attempt") or 0)
        if not self.mac and spec.get("gpu"):
            self.kernel_faults(force=True)  # admission must not rely on stale telemetry
            if not self.journal_ok or self.kernel_fault_latched:
                self.report(jid, LAUNCH_FAILED_RC, {"peak_ram": 0, "peak_vram": 0},
                            0, attempt, fault="kernel" if self.kernel_fault_latched else "telemetry")
                return
        try:
            ram = int(spec.get("effective_ram") or 0)
            if ram <= 0:
                raise ValueError("no memory cap (effective_ram missing or 0)")
            shutil.rmtree(sdir, ignore_errors=True)
            os.makedirs(sdir)
            env = {**(spec.get("env") or {}), "CQ_JOB_ID": str(jid), "CQ_SIGNAL_DIR": sdir}
            a = {"job_id": jid, "attempt": attempt, "gpu": bool(spec.get("gpu")), "cap": ram, "signal_dir": sdir,
                 "started": self.now(), "current_ram": 0, "current_vram": 0, "peak_ram": 0, "peak_vram": 0}
            argv = list(spec["argv"])
            if self.mac:
                a["mac_runner"] = True
                self.intents[jid] = {**a, "state": "launching"}
                self.save_allocs()
                self.launch_mac(a, argv, spec.get("cwd"), env)
            else:
                a["unit"] = f"cq-job-{jid}-a{attempt}.service"
                self.intents[jid] = {**a, "state": "launching"}
                self.save_allocs()  # a crash after systemd-run but before the next save is adopted on restart
                self.launch_linux(a, argv, spec, env)
        except Exception as e:
            log.error("job %s failed to launch: %s", jid, e)
            if self.mac and jid in self.procs:
                # Spawn succeeded; an identity/save failure is not a launch failure.
                self.allocs[jid] = self.alloc_from_intent(self.intents.pop(jid))
                self.save_allocs()
                return
            shutil.rmtree(sdir, ignore_errors=True)
            intent = self.intents.pop(jid, None)
            if intent:
                self.save_allocs()
                if not self.mac:
                    self.host.run(["systemctl", "--user", "stop", "--no-block", intent["unit"]], timeout=120)
            self.report(jid, LAUNCH_FAILED_RC, {"peak_ram": 0, "peak_vram": 0}, 0, attempt)
            return
        self.intents.pop(jid, None)
        self.allocs[jid] = a
        self.save_allocs()
        log.info("started job %s", jid)

    def launch_linux(self, a, argv, spec, env):
        cap = f"MemoryMax={a['cap']}"
        locks = self.cfg.get("gpu_locks") or []  # agent.json only; a start spec never sets locks
        if a["gpu"] and locks:
            argv = ["flock", "-x", locks[0]] + argv
        cmd = ["systemd-run", "--user", "--unit", a["unit"].removesuffix(".service"), f"--property={cap}",
               "--property=MemorySwapMax=0", "--property=KillMode=control-group",
               "--property=RemainAfterExit=yes", "--property=Type=exec"]
        if spec.get("cwd"):
            cmd.append(f"--working-directory={spec['cwd']}")
        cmd += [f"--setenv={k}={v}" for k, v in env.items()] + ["--"] + argv
        rc, out = self.host.run(cmd)
        if rc != 0:
            raise OSError(f"systemd-run rc={rc}: {out.strip()[:200]}")

    def launch_mac(self, a, argv, cwd, env):
        wrapped = [sys.executable, os.path.abspath(__file__), "--mac-runner", a["signal_dir"]]
        locks = self.cfg.get("gpu_locks") or []
        if a["gpu"] and locks:
            wrapped += ["--gpu-lock", locks[0]]
        wrapped += ["--"] + argv
        p = self.host.spawn(wrapped, cwd, {**os.environ, **env}, f"{self.spool}/logs/{a['job_id']}.log")
        self.procs[a["job_id"]] = p
        a["pid"] = p.pid
        a["ident"] = self.host.run(["ps", "-p", str(p.pid), "-o", "lstart="])[1].strip()

    def mac_alive(self, a):
        """True only for a process this agent started (same Popen, or same pid + start time)."""
        p = self.procs.get(a["job_id"])
        if p is not None:
            return p.poll() is None
        return bool(a.get("pid") and a.get("ident")) and self.host.run(
            ["ps", "-p", str(a["pid"]), "-o", "lstart="])[1].strip() == a["ident"]

    def mac_group_alive(self, a):
        """Occupancy is conservative; it is NOT permission to signal an orphan group."""
        return self.mac_alive(a) or self.host.group_alive(a["pgid"])

    def terminate(self, a, reason):
        """Stop one of our own allocations; poll() reports it once it is gone.
        reason: cancel | vram | kernel | telemetry | cap | pressure."""
        if a.get("stop"):
            return
        a["stop"] = reason
        log.warning("stopping job %s: %s", a["job_id"], reason)
        if self.mac:
            a["term_at"] = self.now()
            if a.get("mac_runner"):
                self.write(f"{a['signal_dir']}/stop", reason)
            elif self.mac_alive(a):
                self.host.killpg(a["pgid"], signal.SIGTERM)
        else:
            self.host.run(["systemctl", "--user", "stop", "--no-block", a["unit"]], timeout=120)
        self.save_allocs()

    def finish(self, a, rc):
        reason = a.get("stop")
        if reason:
            rc = 143 if reason == "cancel" else 137
        if not self.mac:
            if not reason:
                self.host.run(["systemctl", "--user", "stop", "--no-block", a["unit"]], timeout=120)
            self.host.run(["systemctl", "--user", "reset-failed", a["unit"]])
        extra = {}
        if reason in ("vram", "kernel", "telemetry"):
            extra["fault"] = reason
        if reason == "cancel":
            extra["cancelled"] = True
        peaks = {"peak_ram": max(a["peak_ram"], a["current_ram"]),
                 "peak_vram": max(a["peak_vram"], a["current_vram"])}
        self.report(a["job_id"], rc, peaks, round(self.now() - a["started"], 1), a.get("attempt", 0), **extra)
        self.allocs.pop(a["job_id"], None)
        self.procs.pop(a["job_id"], None)
        shutil.rmtree(a["signal_dir"], ignore_errors=True)
        self.save_allocs()

    def report(self, jid, rc, peaks, wall_s, attempt=0, **extra):
        """Queue a report in the outbox; it is sent by flush_outbox."""
        body = {"job_id": jid, "attempt": attempt, "rc": rc, "peaks": peaks, "wall_s": wall_s, **extra}
        with open(f"{self.spool}/outbox.jsonl", "a") as f:
            f.write(json.dumps(body) + "\n")

    def poll(self, a, vram_used):
        """Refresh one allocation; report it if it finished."""
        if a["gpu"]:
            a["current_vram"] = vram_used
        if self.mac:
            self.poll_mac(a)
        else:
            self.poll_linux(a)
        if a["job_id"] in self.allocs:
            a["peak_ram"] = max(a["peak_ram"], a["current_ram"])
            a["peak_vram"] = max(a["peak_vram"], a["current_vram"])

    def poll_linux(self, a):
        rc, out = self.host.run(["systemctl", "--user", "show", a["unit"], "--property",
                                 "ActiveState,SubState,LoadState,ExecMainStatus,ExecMainCode,ControlGroup"])
        if rc != 0:
            return
        p = parse_props(out)
        group = p.get("ControlGroup") or a.get("cgroup")
        if group and a.get("cgroup") != group:
            a["cgroup"] = group
            self.save_allocs()
        ram = self.cgroup_ram(group)
        if ram is not None:
            a["current_ram"] = ram
        if p.get("ActiveState") in ("inactive", "failed") or p.get("SubState") == "exited":
            # The main PID can exit before children (including nested cgroups).
            # Keep the allocation and heartbeat until the complete group drains.
            if not self.cgroup_drained(group):
                return
            status = int(p.get("ExecMainStatus") or 0)
            if p.get("LoadState") == "not-found":
                status = LAUNCH_FAILED_RC
            elif p.get("ExecMainCode") in ("2", "3"):
                status += 128
            self.finish(a, status)

    def poll_mac(self, a):
        if a.get("mac_runner"):
            try:
                state = json.loads(read(f"{a['signal_dir']}/runner.json"))
            except (ValueError, TypeError):
                return  # uncertain launch: retain allocation, never duplicate it
            if state.get("done"):
                self.finish(a, state["rc"])
                return
            a.update({k: state[k] for k in ("pid", "ident", "pgid") if k in state})
            if "pgid" not in a:
                return  # runner waiting for lock, or setup not durably acknowledged
            if not self.mac_group_alive(a):
                self.finish(a, LAUNCH_FAILED_RC)
                return
        elif not self.mac_group_alive(a):
            rc_text = read(f"{a['signal_dir']}/rc").strip()
            self.finish(a, int(rc_text) if rc_text.isdigit() else 1)
            return
        a["current_ram"] = self.footprint(a)
        if (not a.get("mac_runner") and self.mac_alive(a) and a.get("term_at")
                and self.now() - a["term_at"] >= KILL_GRACE_S):
            self.host.killpg(a["pgid"], signal.SIGKILL)
        elif not a.get("stop") and 0 < a["cap"] < a["current_ram"]:
            self.terminate(a, "cap")

    def guards(self, vram_used):
        if self.mac:
            live = [a for a in self.allocs.values() if not a.get("stop")]
            rc, out = self.host.run(["sysctl", "-n", "kern.memorystatus_vm_pressure_level"]) if live else (1, "")
            if rc == 0 and out.strip().isdigit() and int(out) >= 4:
                self.terminate(max(live, key=lambda a: a["current_ram"]), "pressure")
            return
        gpu = [a for a in self.allocs.values() if a["gpu"] and not a.get("stop")]
        if not gpu:
            return
        fault = None
        if vram_used > self.vram_cap:
            fault = "vram"
        else:
            n = self.kernel_faults()
            if not self.journal_ok:
                fault = "telemetry"
            elif n or self.kernel_fault_latched:
                fault = "kernel"
        if fault:
            for a in gpu:
                self.terminate(a, fault)

    def preempt(self, jid):
        a = self.allocs.get(jid)
        if a:
            open(f"{a['signal_dir']}/LEASE-END-REQUESTED", "w").close()
            log.info("lease end requested for job %s", jid)

    def cancel(self, jid):
        a = self.allocs.get(jid)
        if a:
            self.terminate(a, "cancel")

    # -- main loop -----------------------------------------------------------------

    def tick(self):
        """One cycle: monitor, guard, flush reports, heartbeat, act on the reply."""
        stats = self.stats()
        for a in list(self.allocs.values()):
            self.poll(a, stats["vram_used"])
        self.guards(stats["vram_used"])
        if not self.mac and (not self.journal_ok or self.kernel_fault_latched):
            stats["outside_gpu_busy"] = True
        if not self.flush_outbox():
            log.warning("coordinator unreachable; reports queued, starting nothing")
            return
        allocs = [{"job_id": a["job_id"], "attempt": a.get("attempt", 0), "state": "running",
                   "current_ram": a["current_ram"], "current_vram": a["current_vram"]} for a in self.allocs.values()]
        try:
            reply = self.post("/agent/heartbeat", {"stats": {**stats, "allocs": allocs}, "allocs": allocs})
        except Unreachable as e:
            log.warning("heartbeat failed: %s", e)
            return
        except ApiError as e:
            log.error("heartbeat rejected: %s", e)
            return
        for jid in reply.get("preempt", []):
            self.preempt(jid)
        for jid in reply.get("cancel", []):
            self.cancel(jid)
        for spec in reply.get("start", []):
            try:
                self.start(spec)
            except Exception:
                log.exception("start failed for %r", spec)

    def run_forever(self):
        while True:
            try:
                self.tick()
            except Exception:
                log.exception("tick failed")
            time.sleep(HEARTBEAT_S)


def main(argv=None):
    home = os.path.expanduser("~")
    ap = argparse.ArgumentParser(description="compute-queue Node Agent")
    ap.add_argument("--hw", action="store_true", help="print hardware JSON and exit")
    ap.add_argument("--config", default=f"{home}/.config/compute-queue/agent.json")
    ap.add_argument("--spool", default=f"{home}/.local/share/compute-queue/agent")
    ap.add_argument("--mac-runner", help=argparse.SUPPRESS)
    ap.add_argument("--gpu-lock", help=argparse.SUPPRESS)
    ap.add_argument("command", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.mac_runner:
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        return mac_runner(args.mac_runner, command, args.gpu_lock)
    if args.hw:
        print(json.dumps(hw_info(Host(), platform.system().lower())))
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if os.stat(args.config).st_mode & 0o077:
        log.warning("%s is readable by others; chmod 600 it", args.config)
    with open(args.config) as f:
        cfg = json.load(f)
    Agent(cfg, args.spool).run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
