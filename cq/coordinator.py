"""Authenticated HTTP coordinator and compatibility board."""

import argparse
from contextlib import closing
import fcntl
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import tempfile
import time
from urllib.parse import parse_qs, unquote, urlsplit

from . import model


DEFAULT_STATE = "~/.local/share/compute-queue"
DEFAULT_BOARD = str(Path("~/.local/share/compute-queue/compute-board.json").expanduser())


class APIError(Exception):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)


def token_hash(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def add_token(db, name, role, node=None):
    """Return a new token; retain only its hash."""
    if not name or role not in model.ROLES:
        raise ValueError("invalid principal or role")
    token = secrets.token_urlsafe(32)
    db.execute("INSERT INTO tokens(hash,principal,role,node) VALUES (?,?,?,?)",
               (token_hash(token), name, role, (node or name) if role == "node" else None))
    return token


def audit(db, principal, action, now, job_id=None, detail=None):
    detail = dict(detail or {})
    detail.setdefault("reason", "")
    db.execute("INSERT INTO audit(at,principal,action,job_id,detail) VALUES (?,?,?,?,?)",
               (now, principal, action, job_id, json.dumps(detail)))


def write_board(db, path, lock_path=None):
    """Rewrite the old board under the legacy tool's advisory lock."""
    path = Path(path)
    if lock_path is not None and Path(lock_path).resolve() != path.resolve():
        raise ValueError("board_lock must refer to the board file itself")
    path.parent.mkdir(parents=True, exist_ok=True)
    # Retain the legacy board lock while atomically replacing its contents.
    try:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    except FileNotFoundError:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        mode = 0o644
    else:
        mode = stat.S_IMODE(os.fstat(fd).st_mode)
    with os.fdopen(fd, "r+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        cloud = []
        previous_next = 1
        stream.seek(0)
        raw = stream.read()
        if raw.strip():
            previous = json.loads(raw)
            cloud = [j for j in previous.get("jobs", []) if j.get("resource") == "cloud"]
            previous_next = previous.get("next_id", 1)
        jobs = []
        for row in db.execute("""SELECT * FROM jobs WHERE
                state IN ('queued','running','preempting','cancelling') ORDER BY id"""):
            job = model.job_dict(row)
            node = job["node"] or job["node_pref"]
            resource = "mac" if node == "mac" else "gpu" if (
                job["gpu"] and node in ("ai", "any")) else None
            if resource is None:
                continue
            _, _, est = model.effective(db, job)
            eta = job["hold_until"]
            if eta is None and est is not None:
                eta = (job["started"] or job["created"]) + est
            jobs.append({"id": job["id"], "resource": resource,
                         "who": job["submitter"], "note": job["note"],
                         "booked": job["created"], "eta": eta,
                         "status": "queued" if job["state"] == "queued" else "running",
                         "started": job["started"], "ended": job["ended"]})
        seq = db.execute("SELECT seq FROM sqlite_sequence WHERE name='jobs'").fetchone()
        next_id = max(previous_next, (seq[0] if seq else 0) + 1,
                      max((j["id"] for j in cloud), default=0) + 1)
        board = {"next_id": next_id, "jobs": jobs + cloud}
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                             dir=path.parent, prefix=path.name + ".",
                                             delete=False) as output:
                temporary = Path(output.name)
                json.dump(board, output, ensure_ascii=False, indent=1)
                output.write("\n")
                output.flush()
                os.fchmod(output.fileno(), mode)
                os.fsync(output.fileno())
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def bind_address(value):
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return str(value[0]), int(value[1])
    host, port = value.rsplit(":", 1)
    return host, int(port)


class Coordinator(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, config):
        self.config = dict(config)
        state = Path(config.get("state", DEFAULT_STATE)).expanduser()
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        state.chmod(0o700)
        self.db_path = state / "queue.sqlite3"
        self.board_path = Path(config.get("board", DEFAULT_BOARD)).expanduser()
        self.board_lock = config.get("board_lock")
        if (self.board_lock is not None
                and Path(self.board_lock).resolve() != self.board_path.resolve()):
            raise ValueError("board_lock must refer to the board file itself")
        self.clock = config.get("clock", time.time)
        with closing(model.connect(self.db_path)):
            pass
        self.db_path.chmod(0o600)
        super().__init__(bind_address(config.get("bind", "127.0.0.1:8765")), Handler)

    def write_board(self, db):
        try:
            write_board(db, self.board_path, self.board_lock)
        except Exception:
            logging.exception("board update failed")

    def service_actions(self):
        try:
            with closing(model.connect(self.db_path)) as db:
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    expired = model.expire_holds(db, self.clock())
                if expired:
                    self.write_board(db)
        except (OSError, sqlite3.Error, ValueError):
            logging.exception("hold expiry or board update failed")


def make_server(config):
    """Build a server; callers control its serving thread."""
    return Coordinator(config)


class Handler(BaseHTTPRequestHandler):
    server_version = "compute-queue/1"

    def log_message(self, fmt, *args):
        logging.info("%s %s", self.client_address[0], fmt % args)

    def respond(self, status, payload):
        data = json.dumps(payload, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.handle_api(False)

    def do_POST(self):
        self.handle_api(True)

    def body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise APIError(400, "invalid content length") from None
        if length < 0 or length > 1024*1024:
            raise APIError(400, "invalid content length")
        if self.headers.get("Transfer-Encoding"):
            raise APIError(400, "transfer encoding is unsupported")
        try:
            data = json.loads(self.rfile.read(length) if length else b"{}",
                              parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError, UnicodeError):
            raise APIError(400, "invalid JSON") from None
        if not isinstance(data, dict):
            raise APIError(400, "JSON body must be an object")
        return data

    def identity(self, db):
        auth = self.headers.get("Authorization", "")
        scheme, _, token = auth.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise APIError(401, "bad token")
        row = db.execute("SELECT * FROM tokens WHERE hash=?", (token_hash(token),)).fetchone()
        if row is None:
            raise APIError(401, "bad token")
        return dict(row)

    def require(self, identity, action, job=None, data=None):
        principal = identity["node"] if identity["role"] == "node" else identity["principal"]
        if not model.allowed(identity["role"], principal, action, job):
            message = "needs owner" if (action in ("cancel", "finish", "renew")
                        and identity["role"] == "operator" and job is not None
                        and job["submitter"] != principal) else "forbidden"
            raise APIError(403, message)
        if (identity["role"] == "operator" and job is not None
                and job["submitter"] != principal and action != "view"):
            if not isinstance((data or {}).get("reason"), str) or not data["reason"].strip():
                raise APIError(400, "reason required for operator actions on others' jobs")

    def handle_api(self, mutation):
        try:
            url = urlsplit(self.path)
            parts = [unquote(p) for p in url.path.strip("/").split("/")]
            with closing(model.connect(self.server.db_path)) as db:
                public = mutation and parts == ["nodes", "register"]
                identity = None if public else self.identity(db)
                data = self.body() if mutation else {}
                if "reason" in data and not isinstance(data["reason"], str):
                    raise APIError(400, "reason must be a string")
                now = self.server.clock()
                if mutation:
                    with db:
                        db.execute("BEGIN IMMEDIATE")
                        model.expire_holds(db, now)
                        payload, action, job_id, detail = self.mutate(db, parts, identity, data, now)
                        audit(db, identity["principal"] if identity else data["name"],
                              action, now, job_id, detail)
                    self.server.write_board(db)
                else:
                    payload = self.read(db, parts, parse_qs(url.query), identity)
            self.respond(200, payload)
        except APIError as exc:
            self.respond(exc.status, {"error": str(exc)})
        except sqlite3.IntegrityError:
            self.respond(409, {"error": "state conflict"})
        except (ValueError, TypeError, OverflowError) as exc:
            self.respond(400, {"error": str(exc) or "bad input"})
        except KeyError as exc:
            self.respond(400, {"error": f"missing field {exc.args[0]}"})
        except (OSError, sqlite3.Error):
            logging.exception("coordinator request failed")
            self.respond(500, {"error": "coordinator storage error"})

    def check_priority(self, identity, priority):
        if type(priority) is not int:
            raise APIError(400, "priority must be an integer")
        if identity["role"] == "submitter" and not -10 <= priority <= 0:
            raise APIError(403, "submitter priority must be between -10 and 0")

    def check_hold_until(self, identity, hold_until, now):
        if identity["role"] == "submitter":
            if hold_until is None:
                return now + 12 * 3600
            model.number(hold_until, "hold_until")
            if hold_until > now + 12 * 3600:
                raise APIError(400, "submitter holds are limited to 12 hours")
        return hold_until

    def read(self, db, parts, query, identity):
        if len(parts) == 3 and parts[0] == "nodes" and parts[2] == "diagnostic":
            self.require(identity, "node_admin")
            return {"grants": [dict(r) for r in db.execute(
                "SELECT * FROM diagnostic_grants WHERE node=? ORDER BY job_id", (parts[1],))]}
        self.require(identity, "view")
        if parts == ["jobs"]:
            state = query.get("state", [None])[0]
            if state is not None and state not in model.JOB_STATES:
                raise APIError(400, "invalid state")
            rows = db.execute("SELECT * FROM jobs WHERE state=? ORDER BY id", (state,)) if state else db.execute(
                "SELECT * FROM jobs ORDER BY id")
            return {"jobs": [model.job_dict(row) for row in rows]}
        if len(parts) == 2 and parts[0] == "jobs":
            return self.get_job(db, parts[1])
        if parts == ["nodes"]:
            nodes = []
            for row in db.execute("SELECT * FROM nodes ORDER BY name"):
                node = dict(row)
                for key in ("gpu_locks", "stats"):
                    node[key] = json.loads(node[key])
                nodes.append(node)
            return {"nodes": nodes}
        if parts == ["profiles"]:
            return {"profiles": [{"kind": r["kind"], "runs": json.loads(r["runs"])}
                                 for r in db.execute("SELECT * FROM profiles ORDER BY kind")]}
        raise APIError(404, "not found")

    def get_job(self, db, value):
        try:
            job_id = int(value)
        except ValueError:
            raise APIError(400, "invalid job id") from None
        row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise APIError(404, "job not found")
        return model.job_dict(row)

    def mutate(self, db, parts, identity, data, now):
        detail = {"reason": data.get("reason", "")}
        if parts == ["jobs"]:
            self.require(identity, "submit")
            keys = {"argv", "node_pref", "gpu", "ram_req", "vram_req", "priority",
                    "kind", "note", "deps", "leaseable", "est_s", "urgent", "hold",
                    "hold_until", "cwd", "env"}
            if set(data) - keys - {"reason"}:
                raise APIError(400, "unknown job fields")
            if data.get("urgent", False):
                self.require(identity, "urgent")
            args = {k: v for k, v in data.items() if k in keys}
            self.check_priority(identity, args.get("priority", 0))
            if args.get("hold"):
                args["hold_until"] = self.check_hold_until(identity, args.get("hold_until"), now)
            job_id = model.create_job(db, identity["principal"], now=now, **args)
            if not args.get("hold") and args.get("ram_req", 0) == 0 and not db.execute(
                    "SELECT 1 FROM profiles WHERE kind=?", (args.get("kind", ""),)).fetchone():
                raise APIError(400, "ram_req is required without a Profile")
            return {"id": job_id}, "submit", job_id, detail
        if len(parts) == 3 and parts[0] == "jobs":
            job = self.get_job(db, parts[1])
            action = parts[2]
            if action not in ("cancel", "move", "priority", "preempt", "renew", "finish"):
                raise APIError(404, "not found")
            self.require(identity, action, job, data)
            if job["state"] in model.TERMINAL:
                raise APIError(409, "job is finished")
            if action == "cancel":
                state = "cancelled" if job["state"] == "queued" or job["hold"] else "cancelling"
                db.execute("UPDATE jobs SET state=?,ended=?,state_note=? WHERE id=?",
                           (state, now if state == "cancelled" else None, data.get("reason", ""), job["id"]))
                if state == "cancelled":
                    model.revoke_diagnostic(db, job["id"], now, data.get("reason", "cancelled"))
            elif action == "move":
                if job["state"] != "queued":
                    raise APIError(409, "only queued jobs can move")
                if "before" in data:
                    if type(data["before"]) is not int:
                        raise APIError(400, "before must be a job id")
                    before = self.get_job(db, data["before"])
                    if before["state"] != "queued" or before["id"] == job["id"]:
                        raise APIError(409, "invalid move target")
                    lower = db.execute("SELECT MAX(position) FROM jobs WHERE state='queued' AND position<? AND id!=?",
                                       (before["position"], job["id"])).fetchone()[0]
                    position = (lower + before["position"]) / 2 if lower is not None else before["position"] - 1
                else:
                    position = data["position"]
                    if isinstance(position, bool) or not isinstance(position, (float, int)):
                        raise APIError(400, "invalid position")
                    import math
                    if not math.isfinite(position):
                        raise APIError(400, "invalid position")
                db.execute("UPDATE jobs SET position=? WHERE id=?", (position, job["id"]))
            elif action == "priority":
                self.check_priority(identity, data["priority"])
                urgent = data.get("urgent", job["urgent"])
                if "urgent" in data:
                    self.require(identity, "urgent", job, data)
                    if type(urgent) is not bool:
                        raise APIError(400, "urgent must be boolean")
                db.execute("UPDATE jobs SET priority=?,urgent=? WHERE id=?",
                           (data["priority"], urgent, job["id"]))
            elif action == "preempt":
                if job["state"] not in ("running", "preempting") or not job["leaseable"] or job["hold"]:
                    raise APIError(409, "job cannot be preempted")
                if db.execute("SELECT 1 FROM diagnostic_grants WHERE job_id=?",
                              (job["id"],)).fetchone():
                    raise APIError(409, "diagnostic cannot be preempted")
                db.execute("UPDATE jobs SET state='preempting' WHERE id=?", (job["id"],))
            else:
                if not job["hold"]:
                    raise APIError(409, "job is not a hold")
                if action == "renew":
                    model.number(data["hold_until"], "hold_until")
                    self.check_hold_until(identity, data["hold_until"], now)
                    db.execute("UPDATE jobs SET hold_until=? WHERE id=?", (data["hold_until"], job["id"]))
                else:
                    db.execute("UPDATE jobs SET state='done',ended=? WHERE id=?", (now, job["id"]))
            detail.update({k: v for k, v in data.items() if k in ("before", "position", "priority", "urgent", "hold_until")})
            return self.get_job(db, job["id"]), action, job["id"], detail
        if parts == ["nodes", "invite"]:
            self.require(identity, "node_admin")
            name = data["name"]
            if not isinstance(name, str) or not name:
                raise APIError(400, "invalid node name")
            if db.execute("SELECT 1 FROM nodes WHERE name=?", (name,)).fetchone():
                raise APIError(409, "node already exists")
            code = secrets.token_urlsafe(32)
            db.execute("INSERT INTO invites(code_hash,node_name,expires) VALUES (?,?,?)",
                       (token_hash(code), name, now + 86400))
            detail["name"] = name
            return {"code": code}, "node_invite", None, detail
        if parts == ["nodes", "register"]:
            name, code, hw = data["name"], data["code"], data["hw"]
            if not isinstance(code, str) or not isinstance(name, str) or not isinstance(hw, dict):
                raise APIError(400, "invalid registration")
            invite = db.execute("SELECT * FROM invites WHERE code_hash=?", (token_hash(code),)).fetchone()
            if invite is None or invite["node_name"] != name or invite["expires"] <= now:
                raise APIError(403, "invalid or expired invite")
            if db.execute("SELECT 1 FROM nodes WHERE name=?", (name,)).fetchone():
                raise APIError(409, "node already exists")
            model.register_node(db, name, hw.get("os", data.get("os", "linux")), hw)
            token = add_token(db, name, "node", node=name)
            db.execute("DELETE FROM invites WHERE node_name=?", (name,))
            detail["name"] = name
            return {"token": token}, "node_register", None, detail
        if len(parts) == 3 and parts[0] == "nodes":
            self.require(identity, "node_admin")
            node = db.execute("SELECT * FROM nodes WHERE name=?", (parts[1],)).fetchone()
            if node is None:
                raise APIError(404, "node not found")
            action = parts[2]
            if action == "diagnostic":
                if (set(data) - {"job_id", "reason"}
                        or type(data.get("job_id")) is not int or data["job_id"] <= 0
                        or not isinstance(data.get("reason"), str) or not data["reason"].strip()):
                    raise APIError(400, "diagnostic requires a positive job_id and nonempty reason")
                job = self.get_job(db, data["job_id"])
                try:
                    grant = model.authorize_diagnostic(
                        db, node, job, identity["principal"], data["reason"], now)
                except ValueError as exc:
                    raise APIError(409, str(exc)) from exc
                detail.update(node=parts[1], attempt=0)
                return grant, "diagnostic_authorize", job["id"], detail
            if action not in ("approve", "disable", "clear-fault"):
                raise APIError(404, "not found")
            if action == "approve" and node["status"] not in ("pending", "disabled"):
                raise APIError(409, "node must be pending or disabled")
            if action == "clear-fault" and node["status"] != "faulted":
                raise APIError(409, "node is not faulted")
            if action != "disable" and db.execute("""SELECT 1 FROM diagnostic_grants g
                    JOIN jobs j ON j.id=g.job_id WHERE g.node=? AND g.revoked IS NULL
                    AND (g.consumed IS NULL OR j.state IN ('running','preempting','cancelling'))""",
                    (parts[1],)).fetchone():
                raise APIError(409, "node has a pending or running diagnostic")
            model.set_node_status(db, parts[1], "disabled" if action == "disable" else "active")
            detail["node"] = parts[1]
            return {"status": "disabled" if action == "disable" else "active"}, "node_" + action, None, detail
        if parts == ["agent", "heartbeat"]:
            node = identity["node"]
            self.require(identity, "heartbeat", {"node": node})
            if not db.execute("SELECT 1 FROM nodes WHERE name=?", (node,)).fetchone():
                raise APIError(404, "node not found")
            stats = dict(data["stats"]) if isinstance(data.get("stats"), dict) else None
            if stats is None:
                raise APIError(400, "stats must be an object")
            stats["allocs"] = data.get("allocs", stats.get("allocs", []))
            model.record_heartbeat(db, node, stats, now)
            if any("attempt" not in alloc for alloc in stats["allocs"]):
                raise APIError(400, "allocation attempt required")
            live = {(alloc["job_id"], alloc["attempt"]) for alloc in stats["allocs"]}
            lost = [row for row in db.execute("""SELECT id,attempt,state FROM jobs
                WHERE node=? AND state IN ('running','preempting','cancelling')
                AND hold=0 AND started<? ORDER BY id""", (node, now - 60))
                if (row["id"], row["attempt"]) not in live]
            for job in lost:
                note = "lost on node"
                if job["state"] == "cancelling":
                    note = "cancelled; lost on node"
                    db.execute("""UPDATE jobs SET state='cancelled',ended=?,state_note=?
                        WHERE id=?""", (now, note, job["id"]))
                elif db.execute("""SELECT 1 FROM diagnostic_grants
                        WHERE job_id=? AND consumed IS NOT NULL""", (job["id"],)).fetchone():
                    note = "diagnostic lost on node; no retry"
                    db.execute("""UPDATE jobs SET state='failed',ended=?,state_note=?
                        WHERE id=?""", (now, note, job["id"]))
                else:
                    db.execute("""UPDATE jobs SET state='queued',attempt=attempt+1,
                        node=NULL,started=NULL,ended=NULL,rc=NULL,state_note='lost on node'
                        WHERE id=?""", (job["id"],))
                audit(db, identity["principal"], "reconcile", now, job["id"],
                      {"reason": note, "node": node})
            plan = model.plan_for_node(db, node, now)
            preempt = [r["id"] for r in db.execute("SELECT id FROM jobs WHERE node=? AND state='preempting' ORDER BY id", (node,))]
            cancel = [r["id"] for r in db.execute("SELECT id FROM jobs WHERE node=? AND state='cancelling' ORDER BY id", (node,))]
            detail.update(node=node, start=[j["id"] for j in plan["start"]], preempt=preempt,
                          cancel=cancel, blocked=plan["blocked"])
            return {"start": plan["start"], "preempt": preempt, "cancel": cancel}, "heartbeat", None, detail
        if parts == ["agent", "report"]:
            job = self.get_job(db, data["job_id"])
            self.require(identity, "report", {"node": identity["node"]})
            if job["node"] != identity["node"]:
                raise APIError(409, "job is not assigned to this node")
            attempt = data["attempt"]
            if type(attempt) is not int or attempt < 0:
                raise APIError(400, "attempt must be a nonnegative integer")
            if job["attempt"] != attempt:
                raise APIError(409, "allocation attempt differs")
            if job["state"] not in model.ACTIVE:
                raise APIError(409, "job has no active allocation")
            if "fault" in data and not isinstance(data["fault"], (str, type(None))):
                raise APIError(400, "fault must be a string")
            cancelled = data.get("cancelled", False)
            if type(cancelled) is not bool:
                raise APIError(400, "cancelled must be boolean")
            model.report(db, job["id"], data["rc"], data.get("peaks", {}),
                         data.get("wall_s", 0), now, data.get("fault"), cancelled)
            detail.update(rc=data["rc"], fault=data.get("fault"))
            return self.get_job(db, job["id"]), "report", job["id"], detail
        raise APIError(404, "not found")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--bind", default="127.0.0.1:8765")
    parser.add_argument("--board", default=DEFAULT_BOARD)
    parser.add_argument("--board-lock")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--init-owner", metavar="NAME")
    group.add_argument("--add-token", nargs=2, metavar=("NAME", "ROLE"))
    args = parser.parse_args(argv)
    if args.init_owner or args.add_token:
        state = Path(args.state).expanduser()
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        state.chmod(0o700)
        path = state / "queue.sqlite3"
        with closing(model.connect(path)) as db:
            path.chmod(0o600)
            with db:
                db.execute("BEGIN IMMEDIATE")
                if args.init_owner:
                    if db.execute("SELECT 1 FROM tokens WHERE role='owner'").fetchone():
                        parser.error("an owner already exists; use --add-token")
                    name, role = args.init_owner, "owner"
                else:
                    name, role = args.add_token
                if role not in model.ROLES:
                    parser.error("role must be owner, operator, submitter, or node")
                token = add_token(db, name, role)
                audit(db, name, "token_add", time.time(), detail={"role": role})
        print(token)
        return
    logging.basicConfig(level=logging.INFO)
    server = make_server(vars(args))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
