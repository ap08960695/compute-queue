"""SQLite state, resource sizing, and scheduling."""

import json
import math
import os
import sqlite3


JOB_STATES = {"queued", "running", "preempting", "cancelling", "done",
              "failed", "blocked", "cancelled"}
NODE_STATES = {"pending", "active", "faulted", "disabled"}
ACTIVE = ("running", "preempting", "cancelling")
TERMINAL = ("done", "failed", "blocked", "cancelled")
ROLES = {"owner", "operator", "submitter", "node"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY, os TEXT NOT NULL, status TEXT NOT NULL
        CHECK(status IN ('pending','active','faulted','disabled')),
    ram_total REAL NOT NULL, vram_total REAL NOT NULL, vram_cap REAL NOT NULL,
    ram_floor REAL NOT NULL, gpu_locks TEXT NOT NULL, last_seen REAL,
    stats TEXT NOT NULL DEFAULT '{}', created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, submitter TEXT NOT NULL,
    kind TEXT NOT NULL, note TEXT NOT NULL, argv TEXT NOT NULL,
    cwd TEXT NOT NULL, env TEXT NOT NULL, node_pref TEXT NOT NULL,
    gpu INTEGER NOT NULL, ram_req REAL NOT NULL, vram_req REAL NOT NULL,
    priority INTEGER NOT NULL, position REAL NOT NULL, deps TEXT NOT NULL,
    leaseable INTEGER NOT NULL, est_s REAL, urgent INTEGER NOT NULL,
    hold INTEGER NOT NULL, hold_until REAL, state TEXT NOT NULL
        CHECK(state IN ('queued','running','preempting','cancelling',
                       'done','failed','blocked','cancelled')),
    node TEXT, attempt INTEGER NOT NULL DEFAULT 0, rc INTEGER,
    peaks TEXT NOT NULL DEFAULT '{}', created REAL NOT NULL,
    started REAL, ended REAL, state_note TEXT NOT NULL DEFAULT '',
    ram_floor REAL NOT NULL DEFAULT 0, ram_start_min REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state, node_pref);
CREATE TABLE IF NOT EXISTS profiles (kind TEXT PRIMARY KEY, runs TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tokens (
    hash TEXT PRIMARY KEY, principal TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('owner','operator','submitter','node')),
    node TEXT
);
CREATE TABLE IF NOT EXISTS invites (
    code_hash TEXT PRIMARY KEY, node_name TEXT NOT NULL, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL,
    principal TEXT NOT NULL, action TEXT NOT NULL, job_id INTEGER,
    detail TEXT NOT NULL
);
-- Additive migration: existing queues retain all jobs, tokens and node states.
CREATE TABLE IF NOT EXISTS diagnostic_grants (
    job_id INTEGER PRIMARY KEY REFERENCES jobs(id),
    node TEXT NOT NULL REFERENCES nodes(name),
    principal TEXT NOT NULL, reason TEXT NOT NULL,
    created REAL NOT NULL, consumed REAL, revoked REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS diagnostic_pending_node
    ON diagnostic_grants(node) WHERE consumed IS NULL AND revoked IS NULL;
"""


def connect(path):
    """Open a WAL database and ensure its schema exists."""
    def secure_files():
        if str(path) != ":memory:":
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.chmod(f"{path}{suffix}", 0o600)
                except FileNotFoundError:
                    pass

    secure_files()
    db = sqlite3.connect(path, timeout=30)
    try:
        secure_files()
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(SCHEMA)
        # Serialize discovery and ALTER for concurrent opens of an older queue.
        with db:
            db.execute("BEGIN IMMEDIATE")
            columns = {r["name"] for r in db.execute("PRAGMA table_info(jobs)")}
            for column in ("ram_floor", "ram_start_min"):
                if column not in columns:
                    db.execute(f"ALTER TABLE jobs ADD COLUMN {column} REAL NOT NULL DEFAULT 0")
        secure_files()
    except Exception:
        db.close()
        raise
    return db


def number(value, name, *, nullable=False):
    if value is None and nullable:
        return None
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0):
        raise ValueError(f"{name} must be a nonnegative finite number")
    return value


def job_dict(row):
    """Decode a job's JSON and boolean columns."""
    job = dict(row)
    for key in ("argv", "env", "deps", "peaks"):
        if isinstance(job[key], str):
            job[key] = json.loads(job[key])
    for key in ("gpu", "leaseable", "urgent", "hold"):
        job[key] = bool(job[key])
    return job


def register_node(db, name, os, hw, status="pending", ram_floor=4*2**30,
                  vram_cap=None, gpu_locks=()):
    import time

    if not isinstance(name, str) or not name or status not in NODE_STATES:
        raise ValueError("invalid node name or status")
    if not isinstance(os, str) or not isinstance(hw, dict):
        raise ValueError("invalid node hardware")
    ram = number(hw.get("ram_total", 0), "ram_total")
    gpus = hw.get("gpus", [])
    if not isinstance(gpus, list) or not all(isinstance(g, dict) for g in gpus):
        raise ValueError("gpus must be a list of objects")
    vram = hw.get("vram_total")
    if vram is None:
        vram = sum(number(g.get("vram_total", 0), "vram_total") for g in gpus)
    number(vram, "vram_total")
    if os.lower() in ("mac", "macos", "darwin"):
        vram = 0
    if vram_cap is None:
        vram_cap = 19.5e9 if vram and os.lower() == "linux" else vram
    number(vram_cap, "vram_cap")
    number(ram_floor, "ram_floor")
    if not all(isinstance(p, str) for p in gpu_locks):
        raise ValueError("invalid GPU locks")
    db.execute("""INSERT INTO nodes
        (name,os,status,ram_total,vram_total,vram_cap,ram_floor,gpu_locks,created)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (name, os, status, ram, vram, vram_cap, ram_floor,
         json.dumps(list(gpu_locks)), time.time()))


def set_node_status(db, name, status):
    if status not in NODE_STATES:
        raise ValueError("invalid node status")
    if not db.execute("UPDATE nodes SET status=? WHERE name=?",
                      (status, name)).rowcount:
        raise KeyError(name)


def record_heartbeat(db, name, stats, now):
    if not isinstance(stats, dict):
        raise ValueError("stats must be an object")
    for key in ("mem_available", "vram_used"):
        number(stats.get(key, 0), key)
    if not isinstance(stats.get("outside_gpu_busy", False), bool):
        raise ValueError("outside_gpu_busy must be boolean")
    allocs = stats.get("allocs", [])
    if not isinstance(allocs, list):
        raise ValueError("allocs must be a list")
    for alloc in allocs:
        if not isinstance(alloc, dict) or type(alloc.get("job_id")) is not int:
            raise ValueError("invalid allocation")
        # Legacy model callers omit attempt for attempt 0; HTTP requires it.
        if type(alloc.get("attempt", 0)) is not int or alloc.get("attempt", 0) < 0:
            raise ValueError("invalid allocation attempt")
        number(alloc.get("current_ram", 0), "current_ram")
        number(alloc.get("current_vram", 0), "current_vram")
    if not db.execute("UPDATE nodes SET stats=?, last_seen=? WHERE name=?",
                      (json.dumps(stats), now, name)).rowcount:
        raise KeyError(name)


def create_job(db, submitter, argv=(), node_pref="any", gpu=False, ram_req=0,
               vram_req=0, priority=0, kind="", note="", deps=(),
               leaseable=False, est_s=None, urgent=False, hold=False,
               hold_until=None, cwd="", env=None, now=0, ram_floor=0, ram_start_min=0):
    for key, value in (("submitter", submitter), ("node_pref", node_pref),
                       ("kind", kind), ("note", note), ("cwd", cwd)):
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a string")
    if not submitter or not node_pref:
        raise ValueError("submitter and node_pref are required")
    if not isinstance(argv, (list, tuple)) or not all(
            isinstance(arg, str) for arg in argv):
        raise ValueError("argv must be a list of strings")
    if hold and argv:
        raise ValueError("hold jobs have no argv")
    if env is None:
        env = {}
    if not isinstance(env, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise ValueError("env must contain strings")
    for key, value in (("gpu", gpu), ("leaseable", leaseable),
                       ("urgent", urgent), ("hold", hold)):
        if not isinstance(value, bool):
            raise ValueError(f"{key} must be boolean")
    for key, value in (("ram_req", ram_req), ("vram_req", vram_req),
                       ("ram_floor", ram_floor), ("ram_start_min", ram_start_min)):
        number(value, key)
    number(est_s, "est_s", nullable=True)
    number(hold_until, "hold_until", nullable=True)
    number(now, "now")
    if type(priority) is not int:
        raise ValueError("priority must be an integer")
    if not isinstance(deps, (list, tuple)):
        raise ValueError("deps must be a list")
    for dep in deps:
        if type(dep) is not int or not db.execute(
                "SELECT 1 FROM jobs WHERE id=?", (dep,)).fetchone():
            raise ValueError(f"unknown dependency {dep}")
    position = db.execute("SELECT COALESCE(MAX(position),0)+1 FROM jobs").fetchone()[0]
    cur = db.execute("""INSERT INTO jobs
        (submitter,kind,note,argv,cwd,env,node_pref,gpu,ram_req,vram_req,
         priority,position,deps,leaseable,est_s,urgent,hold,hold_until,state,created,
         ram_floor,ram_start_min)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'queued',?,?,?)""",
        (submitter, kind, note, json.dumps(argv), cwd, json.dumps(env), node_pref,
         gpu, ram_req, vram_req, priority, position, json.dumps(deps), leaseable,
         est_s, urgent, hold, hold_until, now, ram_floor, ram_start_min))
    return cur.lastrowid


def effective(db, job):
    job = dict(job)
    profile = db.execute("SELECT runs FROM profiles WHERE kind=?",
                         (job["kind"],)).fetchone()
    runs = json.loads(profile["runs"]) if profile else []
    ram = max((r["peak_ram"] for r in runs if r["peak_ram"] > 0), default=0)
    ram = max(1.3 * ram, 1.3 * job["ram_req"], 256 * 2**20)
    vram = max(r["peak_vram"] for r in runs) if runs else job["vram_req"]
    est = job["est_s"]
    if est is None and runs:
        est = max(r["wall_s"] for r in runs)
    return ram, vram + 0.5e9, est


def authorize_diagnostic(db, node, job, principal, reason, now):
    """Create one durable grant in the caller's write transaction.

    The authenticated coordinator must check owner permissions first.
    """
    if node["status"] != "disabled":
        raise ValueError("diagnostic requires a disabled node")
    if (job["node_pref"] != node["name"] or job["state"] != "queued"
            or job["attempt"] != 0 or job["node"] is not None
            or job["started"] is not None or job["hold"] or not job["argv"]):
        raise ValueError("diagnostic requires an exact-node queued executable job at attempt 0")
    if json.loads(node["stats"]).get("allocs") or db.execute(
            "SELECT 1 FROM jobs WHERE node=? AND state IN (?,?,?)",
            (node["name"], *ACTIVE)).fetchone():
        raise ValueError("diagnostic requires no current allocations")
    db.execute("""INSERT INTO diagnostic_grants
        (job_id,node,principal,reason,created) VALUES (?,?,?,?,?)""",
        (job["id"], node["name"], principal, reason, now))
    return dict(db.execute("SELECT * FROM diagnostic_grants WHERE job_id=?",
                           (job["id"],)).fetchone())


def revoke_diagnostic(db, job_id, now, reason):
    if db.execute("""UPDATE diagnostic_grants SET revoked=?
            WHERE job_id=? AND consumed IS NULL AND revoked IS NULL""",
            (now, job_id)).rowcount:
        db.execute("""INSERT INTO audit(at,principal,action,job_id,detail)
            VALUES (?,?,?,?,?)""", (now, "coordinator", "diagnostic_revoke", job_id,
                                    json.dumps({"reason": reason})))


def plan_for_node(db, node_name, now):
    """Apply starts and blocks in the caller's write transaction."""
    result = {"start": [], "preempt": [], "blocked": []}
    node = db.execute("SELECT * FROM nodes WHERE name=?", (node_name,)).fetchone()
    if node is None or node["status"] not in ("active", "disabled"):
        return result
    diagnostic = node["status"] == "disabled"
    grant = db.execute("""SELECT * FROM diagnostic_grants
        WHERE node=? AND consumed IS NULL AND revoked IS NULL""",
        (node_name,)).fetchone() if diagnostic else None
    if diagnostic and grant is None:
        return result
    stats = json.loads(node["stats"])
    observed = {(a["job_id"], a.get("attempt", 0)): a for a in stats.get("allocs", [])}
    running = [job_dict(r) for r in db.execute(
        "SELECT * FROM jobs WHERE node=? AND state IN (?,?,?)",
        (node_name, *ACTIVE))]
    if diagnostic and (running or observed):
        return result
    sizes = {j["id"]: effective(db, j) for j in running}
    growth = sum(max(0, sizes[j["id"]][0] - observed.get((j["id"], j["attempt"]), {}).get(
        "current_ram", 0)) for j in running)
    ram_available = stats.get("mem_available", 0) - growth
    running_floors = {j["id"]: j["ram_floor"] for j in running}
    planned_floor = node["ram_floor"]
    ram_floor = max([planned_floor, *running_floors.values()])
    vram_free = node["vram_cap"] - stats.get("vram_used", 0)
    gpu_free = not stats.get("outside_gpu_busy", False) and not any(
        j["gpu"] for j in running)
    gpu_count = sum(j["gpu"] for j in running)
    queue = [job_dict(r) for r in db.execute("""SELECT * FROM jobs
        WHERE state='queued' AND node_pref IN (?, 'any')
        ORDER BY urgent DESC, priority DESC, position ASC, id ASC""", (node_name,))]
    if diagnostic:
        queue = [j for j in queue if j["id"] == grant["job_id"]
                 and j["node_pref"] == node_name and j["attempt"] == 0]
    head = None
    for job in queue:
        deps = [db.execute("SELECT state FROM jobs WHERE id=?", (dep,)).fetchone()
                for dep in job["deps"]]
        bad = next((dep for dep, row in zip(job["deps"], deps)
                    if row and row["state"] in ("failed", "cancelled", "blocked")), None)
        if bad is not None:
            db.execute("UPDATE jobs SET state='blocked',state_note=?,ended=? WHERE id=?",
                       (f"dependency {bad}", now, job["id"]))
            revoke_diagnostic(db, job["id"], now, f"dependency {bad}")
            result["blocked"].append(job["id"])
            continue
        if any(row is None or row["state"] != "done" for row in deps):
            continue
        ram, vram, est = effective(db, job)
        job_floor = max(ram_floor, job["ram_floor"])
        fits = (ram_available >= max(job_floor + ram, job["ram_start_min"])
                and (not job["gpu"] or gpu_free and vram <= vram_free))
        if not fits:
            if head is None:
                head = job
            continue
        if head is not None:
            backfill = job["leaseable"] or est is not None and est <= 1800
            # SPEC step 6(b): preserve the head's GPU slot and RAM headroom.
            independent = (not (job["gpu"] and head["gpu"])
                           and ram_available - ram >= max(
                               max(job_floor, head["ram_floor"]) + effective(db, head)[0],
                               head["ram_start_min"]))
            if not (backfill or independent):
                continue
        if diagnostic:
            if not db.execute("""UPDATE diagnostic_grants SET consumed=?
                    WHERE job_id=? AND consumed IS NULL AND revoked IS NULL""",
                    (now, job["id"])).rowcount:
                return result
            db.execute("""INSERT INTO audit(at,principal,action,job_id,detail)
                VALUES (?,?,?,?,?)""", (now, "coordinator", "diagnostic_consume", job["id"],
                json.dumps({"node": node_name, "attempt": 0, "reason": grant["reason"]})))
        db.execute("""UPDATE jobs SET state='running',node=?,started=?,ended=NULL,
            state_note='' WHERE id=?""", (node_name, now, job["id"]))
        job.update(state="running", node=node_name, started=now, ended=None,
                   state_note="", effective_ram=ram, effective_vram=vram, est_s=est,
                   effective_ram_floor=job_floor)
        if not job["hold"]:
            result["start"].append(job)
        ram_available -= ram
        planned_floor = max(planned_floor, job["ram_floor"])
        ram_floor = job_floor
        if job["gpu"]:
            gpu_free = False
            gpu_count += 1
            vram_free -= vram

    if head is None:
        return result
    candidates = [j for j in running if j["leaseable"] and not j["hold"]
                  and j["state"] != "cancelling"
                  and (j["urgent"], j["priority"]) < (head["urgent"], head["priority"])]
    if not (head["urgent"] or now - head["created"] > 600 and candidates):
        return result
    candidates.sort(key=lambda j: (j["urgent"], j["priority"], j["id"]))
    ram, vram, _ = effective(db, head)
    for job in candidates:
        projected_floor = max(planned_floor, head["ram_floor"], *running_floors.values())
        if ram_available >= max(projected_floor + ram, head["ram_start_min"]) and (not head["gpu"] or (
                gpu_count == 0 and not stats.get("outside_gpu_busy", False)
                and vram <= vram_free)):
            break
        ram_available += max(sizes[job["id"]][0],
                             observed.get((job["id"], job["attempt"]), {}).get("current_ram", 0))
        running_floors.pop(job["id"], None)
        if job["gpu"]:
            gpu_count -= 1
            vram_free += observed.get((job["id"], job["attempt"]), {}).get("current_vram", 0)
        db.execute("UPDATE jobs SET state='preempting' WHERE id=?", (job["id"],))
        result["preempt"].append(job["id"])
    return result


def report(db, job_id, rc, peaks, wall_s, now, fault=None, cancelled=False):
    job = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if job is None:
        raise KeyError(job_id)
    if job["state"] not in ACTIVE:
        raise ValueError("job has no active allocation")
    if type(rc) is not int or not isinstance(peaks, dict):
        raise ValueError("invalid report")
    peaks = {k: number(peaks.get(k, 0), k) for k in ("peak_ram", "peak_vram")}
    number(wall_s, "wall_s")
    diagnostic = db.execute("""SELECT 1 FROM diagnostic_grants
        WHERE job_id=? AND consumed IS NOT NULL""", (job_id,)).fetchone() is not None
    cancelled = job["state"] == "cancelling"
    state = "cancelled" if cancelled else "done" if rc == 0 else "queued" if rc == 6 else "failed"
    if diagnostic and not cancelled:
        state = "done" if rc == 0 and not fault else "failed"
    note = "cancelled by request" if cancelled else str(fault or "")
    if diagnostic and state == "failed" and not note:
        note = "diagnostic failed; no retry"
    db.execute("""UPDATE jobs SET state=?,rc=?,peaks=?,ended=?,
        attempt=attempt+?,node=?,started=?,state_note=? WHERE id=?""",
        (state, rc, json.dumps(peaks), None if state == "queued" else now,
         int(state == "queued"), None if state == "queued" else job["node"],
         None if state == "queued" else job["started"],
         note, job_id))
    if rc in (0, 6) and not cancelled and (not diagnostic or state == "done"):
        row = db.execute("SELECT runs FROM profiles WHERE kind=?", (job["kind"],)).fetchone()
        runs = json.loads(row["runs"]) if row else []
        runs.append(dict(peaks, wall_s=wall_s, at=now))
        db.execute("INSERT OR REPLACE INTO profiles(kind,runs) VALUES (?,?)",
                   (job["kind"], json.dumps(runs[-5:])))
    if diagnostic:
        set_node_status(db, job["node"], "disabled")
    elif fault and job["node"]:
        set_node_status(db, job["node"], "faulted")


def allowed(role, principal, action, job):
    if role not in ROLES:
        return False
    job = dict(job) if job is not None else {}
    if action in ("heartbeat", "report"):
        return role == "node" and job.get("node", job.get("name")) == principal
    if role == "node":
        return False
    if action in ("submit", "view", "list", "show"):
        return True
    if action == "node_admin":
        # A node can execute/report any job assigned to it. Letting operators
        # enroll their own node would bypass the foreign-job cancellation rule.
        return role == "owner"
    if action in ("urgent", "preempt"):
        return role in ("owner", "operator")
    if action in ("move", "priority"):
        return role in ("owner", "operator") or job.get("submitter") == principal
    if action in ("cancel", "renew", "finish"):
        return role == "owner" or job.get("submitter") == principal
    return False


def expire_holds(db, now):
    ids = [r["id"] for r in db.execute("""SELECT id FROM jobs WHERE hold=1
        AND hold_until IS NOT NULL AND hold_until+1800<?
        AND state IN ('queued','running','preempting','cancelling') ORDER BY id""", (now,))]
    for job_id in ids:
        db.execute("UPDATE jobs SET state='done',ended=?,state_note='hold expired' WHERE id=?",
                   (now, job_id))
        db.execute("INSERT INTO audit(at,principal,action,job_id,detail) VALUES (?,?,?,?,?)",
                   (now, "coordinator", "hold_expired", job_id,
                    json.dumps({"reason": "hold expired"})))
    return ids
