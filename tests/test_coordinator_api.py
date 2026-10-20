"""Coordinator tests use only temporary state and loopback sockets."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import fcntl
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from cq import model
from cq.coordinator import Handler, add_token, make_server, write_board


GB = 10**9


class CoordinatorAPI(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 2_000_000_000
        self.board = self.root / "board.json"
        self.server = make_server({"state": self.root / "state", "bind": ("127.0.0.1", 0),
                                   "board": self.board, "clock": lambda: self.now})
        with closing(model.connect(self.server.db_path)) as db, db:
            self.tokens = {name: add_token(db, name, role) for name, role in (
                ("owner", "owner"), ("ops", "operator"),
                ("alice", "submitter"), ("bob", "submitter"))}
            model.register_node(db, "ai", "linux", {"ram_total": 64*GB, "vram_total": 24*GB},
                                status="active", ram_floor=4*GB)
            self.tokens["ai"] = add_token(db, "ai", "node")
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)

    def close_server(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()

    def request(self, method, path, data=None, who="owner", token=None):
        conn = http.client.HTTPConnection(*self.server.server_address, timeout=10)
        headers = {"Content-Type": "application/json"}
        if who is not None:
            headers["Authorization"] = "Bearer " + (token or self.tokens[who])
        try:
            conn.request(method, path, body=json.dumps(data) if data is not None else None,
                         headers=headers)
            response = conn.getresponse()
            return response.status, json.loads(response.read())
        finally:
            conn.close()

    def post(self, path, data=None, who="owner", status=200):
        actual, payload = self.request("POST", path, data or {}, who)
        self.assertEqual(actual, status, payload)
        return payload

    def submit(self, who="alice", **values):
        data = {"argv": ["python3", "-c", "pass"], "ram_req": GB, "kind": "test"}
        data.update(values)
        return self.post("/jobs", data, who)["id"]

    def job(self, job_id):
        status, result = self.request("GET", f"/jobs/{job_id}")
        self.assertEqual(status, 200, result)
        return result

    def heartbeat(self, who="ai", allocs=None, **stats):
        values = {"mem_available": 60*GB, "vram_used": 0, "outside_gpu_busy": False}
        values.update(stats)
        return self.post("/agent/heartbeat", {"stats": values, "allocs": allocs or []}, who)

    def report(self, job_id, rc, **extra):
        attempt = extra.pop("attempt", self.job(job_id)["attempt"])
        return self.post("/agent/report", dict(job_id=job_id, attempt=attempt, rc=rc,
                         peaks={"peak_ram": 2*GB, "peak_vram": GB}, wall_s=12, **extra), "ai")

    def rows(self, sql, params=()):
        with closing(model.connect(self.server.db_path)) as db:
            return [dict(r) for r in db.execute(sql, params)]

    def test_submit_list_show_and_auth(self):
        job_id = self.submit(note="a job", cwd="/tmp", env={"X": "1"})
        job = self.job(job_id)
        self.assertEqual((job["state"], job["submitter"]), ("queued", "alice"))
        self.assertEqual(job["env"], {"X": "1"})
        status, listing = self.request("GET", "/jobs?state=queued", who="bob")
        self.assertEqual(status, 200)
        self.assertEqual([j["id"] for j in listing["jobs"]], [job_id])
        self.assertEqual(self.request("GET", "/jobs", who=None)[0], 401)
        self.assertEqual(self.request("GET", "/jobs", token="bad")[0], 401)
        self.assertEqual(self.request("GET", "/jobs", who="ai")[0], 403)
        self.assertEqual(self.request("GET", "/jobs/987654")[0], 404)
        self.assertEqual(self.request("GET", "/missing")[0], 404)

    def test_heartbeat_starts_once_with_limits(self):
        job_id = self.submit(gpu=True, node_pref="ai", vram_req=3*GB)
        start = self.heartbeat()["start"]
        self.assertEqual([j["id"] for j in start], [job_id])
        self.assertEqual(start[0]["effective_ram"], 1.3*GB)
        self.assertEqual(start[0]["effective_vram"], 3.5*GB)
        self.assertEqual(start[0]["argv"], ["python3", "-c", "pass"])
        self.assertEqual(start[0]["attempt"], 0)
        self.assertEqual(self.job(job_id)["started"], self.now)
        self.assertEqual(self.heartbeat()["start"], [])

    def test_report_zero_six_one_and_profile(self):
        job_id = self.submit(gpu=True)
        position = self.job(job_id)["position"]
        self.heartbeat()
        result = self.report(job_id, 6)
        self.assertEqual((result["state"], result["attempt"], result["position"]),
                         ("queued", 1, position))
        self.assertIsNone(result["node"])
        self.assertIsNone(result["ended"])
        start = self.heartbeat()["start"][0]
        self.assertEqual(start["attempt"], 1)
        self.assertEqual((start["effective_ram"], start["effective_vram"], start["est_s"]),
                         (2.6*GB, 1.5*GB, 12))
        self.assertEqual(self.report(job_id, 0)["state"], "done")
        second = self.submit()
        self.heartbeat()
        self.assertEqual(self.report(second, 1)["state"], "failed")
        runs = json.loads(self.rows("SELECT runs FROM profiles WHERE kind='test'")[0]["runs"])
        self.assertEqual(len(runs), 2)
        self.post("/agent/report", {"job_id": second, "attempt": 0, "rc": 0}, "ai", status=409)

    def test_cancel_permissions_and_requested_cancel(self):
        job_id = self.submit()
        error = self.post(f"/jobs/{job_id}/cancel", {"reason": "stop"}, "ops", status=403)
        self.assertEqual(error["error"], "needs owner")
        self.post(f"/jobs/{job_id}/cancel", {}, "bob", status=403)
        self.heartbeat()
        self.post(f"/jobs/{job_id}/cancel", {"reason": "stop"}, "alice")
        self.assertEqual(self.heartbeat()["cancel"], [job_id])
        self.assertEqual(self.report(job_id, 1)["state"], "cancelled")
        queued = self.submit()
        self.post(f"/jobs/{queued}/cancel", {}, "owner")
        self.assertEqual(self.job(queued)["state"], "cancelled")

    def test_operator_reason_move_priority_and_urgent(self):
        first, second = self.submit(), self.submit()
        self.post(f"/jobs/{second}/move", {"before": first}, "ops", status=400)
        self.post(f"/jobs/{second}/move", {"before": first, "reason": "queue order"}, "ops")
        self.assertLess(self.job(second)["position"], self.job(first)["position"])
        self.post(f"/jobs/{first}/priority", {"priority": 7, "reason": "deadline"}, "ops")
        self.post(f"/jobs/{first}/priority", {"priority": 8, "urgent": True}, "alice", status=403)
        self.post(f"/jobs/{first}/priority", {"priority": 8, "urgent": True,
                  "reason": "deadline"}, "ops")
        self.assertTrue(self.job(first)["urgent"])
        self.post("/jobs", {"urgent": True}, "alice", status=403)
        audits = self.rows("SELECT * FROM audit WHERE principal='ops'")
        self.assertEqual(len(audits), 3)
        self.assertTrue(all(json.loads(r["detail"])["reason"] for r in audits))

    def test_invite_register_approve_and_node_isolation(self):
        self.post("/nodes/invite", {"name": "mac"}, "alice", status=403)
        self.post("/nodes/invite", {"name": "mac"}, "ops", status=403)
        code = self.post("/nodes/invite", {"name": "mac"})["code"]
        data = {"name": "mac", "code": code, "hw": {"os": "darwin", "ram_total": 64*GB}}
        status, registration = self.request("POST", "/nodes/register", data, who=None)
        self.assertEqual(status, 200, registration)
        self.tokens["mac"] = registration["token"]
        queued = self.submit(node_pref="mac")
        self.assertEqual(self.heartbeat("mac")["start"], [])
        self.assertEqual(self.rows("SELECT status FROM nodes WHERE name='mac'")[0]["status"], "pending")
        self.assertEqual(self.request("POST", "/nodes/register", data, who=None)[0], 403)
        self.post("/nodes/mac/approve", {}, "ops", status=403)
        self.post("/nodes/mac/approve", {})
        self.assertEqual([j["id"] for j in self.heartbeat("mac")["start"]], [queued])
        self.post("/agent/report", {"job_id": queued, "attempt": 0, "rc": 0}, "ai", status=409)
        self.post("/agent/heartbeat", {"stats": {}}, "owner", status=403)
        hashes = self.rows("SELECT hash FROM tokens")
        self.assertIn(hashlib.sha256(self.tokens["mac"].encode()).hexdigest(), [r["hash"] for r in hashes])
        self.assertFalse(any(self.tokens["mac"] in str(r) or code in str(r)
                             for r in self.rows("SELECT * FROM audit")))
        self.assertEqual(self.rows("SELECT * FROM invites"), [])

    def test_invite_expiry_and_wrong_node(self):
        code = self.post("/nodes/invite", {"name": "other"})["code"]
        body = {"name": "wrong", "code": code, "hw": {}}
        self.assertEqual(self.request("POST", "/nodes/register", body, who=None)[0], 403)
        self.now += 86400
        body["name"] = "other"
        self.assertEqual(self.request("POST", "/nodes/register", body, who=None)[0], 403)

    def test_fault_requires_clear_and_disable_stops_placement(self):
        first = self.submit(gpu=True)
        self.heartbeat()
        self.report(first, 1, fault="vram")
        second = self.submit(gpu=True)
        self.assertEqual(self.heartbeat()["start"], [])
        self.post("/nodes/ai/approve", {}, status=409)
        self.post("/nodes/ai/clear-fault", {}, "ops", status=403)
        self.assertEqual(self.heartbeat()["start"], [])
        self.post("/nodes/ai/clear-fault", {})
        self.assertEqual(self.heartbeat()["start"][0]["id"], second)
        self.post("/nodes/ai/disable", {}, "ops", status=403)
        self.post("/nodes/ai/disable", {})
        self.submit()
        self.assertEqual(self.heartbeat()["start"], [])

    def test_hold_renew_finish_expire_and_board(self):
        job_id = self.submit(argv=[], hold=True, gpu=True, node_pref="ai", note="slot",
                             hold_until=self.now + 60)
        board = json.loads(self.board.read_text())
        self.assertEqual(set(board), {"next_id", "jobs"})
        self.assertEqual(board["next_id"], job_id + 1)
        entry = board["jobs"][0]
        self.assertEqual(set(entry), {"id", "resource", "who", "note", "booked", "eta",
                                      "status", "started", "ended"})
        self.assertEqual((entry["resource"], entry["who"], entry["status"]), ("gpu", "alice", "queued"))
        self.assertEqual(self.heartbeat()["start"], [])
        self.assertEqual(json.loads(self.board.read_text())["jobs"][0]["status"], "running")
        self.post(f"/jobs/{job_id}/renew", {"hold_until": self.now + 120}, "alice")
        self.now += 1919
        self.post("/agent/heartbeat", {"stats": {}}, "ai")
        self.assertEqual(self.job(job_id)["state"], "running")
        self.now += 1
        self.post("/agent/heartbeat", {"stats": {}}, "ai")
        self.assertEqual(self.job(job_id)["state"], "running")
        self.now += 1
        self.post("/agent/heartbeat", {"stats": {}}, "ai")
        self.assertEqual(self.job(job_id)["state_note"], "hold expired")
        self.assertEqual(json.loads(self.board.read_text())["jobs"], [])
        self.assertEqual(len(self.rows("SELECT * FROM audit WHERE action='hold_expired'")), 1)
        other = self.submit(argv=[], hold=True)
        self.post(f"/jobs/{other}/finish", {}, "alice")
        self.assertEqual(self.job(other)["state"], "done")

    def test_board_mac_cloud_and_monotonic_ids(self):
        self.board.write_text(json.dumps({"next_id": 91, "jobs": [
            {"id": 90, "resource": "cloud", "who": "remote"}]}))
        mac = self.submit(node_pref="mac")
        self.submit(node_pref="other", gpu=True)
        self.submit(node_pref="ai", gpu=False)
        board = json.loads(self.board.read_text())
        self.assertEqual(board["next_id"], 91)
        self.assertEqual([(j["id"], j["resource"]) for j in board["jobs"]], [(mac, "mac"), (90, "cloud")])
        self.assertFalse(self.board.with_suffix(".lock").exists())

    def test_idle_hold_expiry_writes_board_and_audit(self):
        job_id = self.submit(argv=[], hold=True, gpu=True, hold_until=self.now)
        self.now += 1800
        self.server.service_actions()
        self.assertEqual(self.job(job_id)["state"], "queued")
        self.assertEqual(self.rows("SELECT * FROM audit WHERE action='hold_expired'"), [])
        self.now += 1
        self.server.service_actions()
        self.assertEqual(self.job(job_id)["state_note"], "hold expired")
        self.assertEqual(json.loads(self.board.read_text())["jobs"], [])
        self.assertEqual(len(self.rows("SELECT * FROM audit WHERE action='hold_expired'")), 1)

    def test_preempt_is_cooperative_and_repeated_until_report(self):
        job_id = self.submit(gpu=True, leaseable=True)
        self.heartbeat()
        self.post(f"/jobs/{job_id}/preempt", {"reason": "make room"}, "ops")
        self.assertEqual(self.heartbeat()["preempt"], [job_id])
        self.assertEqual(self.heartbeat()["preempt"], [job_id])
        self.assertEqual(self.report(job_id, 6)["state"], "queued")

    def test_concurrent_heartbeats_never_duplicate_gpu_start(self):
        ids = [self.submit(gpu=True) for _ in range(4)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.heartbeat(), range(4)))
        starts = [j["id"] for result in results for j in result["start"]]
        self.assertEqual(starts, ids[:1])
        self.assertEqual(len(self.rows("SELECT * FROM jobs WHERE state='running'")), 1)

    def test_reconciliation_grace_live_allocations_and_node_scope(self):
        ids = [self.submit(kind=state) for state in model.ACTIVE]
        live = self.submit(kind="live")
        other = self.submit(kind="other", node_pref="mac")
        hold = self.submit(argv=[], hold=True, gpu=True, hold_until=self.now + 10000)
        self.heartbeat()
        with closing(model.connect(self.server.db_path)) as db, db:
            for job_id, state in zip(ids, model.ACTIVE):
                db.execute("UPDATE jobs SET state=? WHERE id=?", (state, job_id))
            db.execute("UPDATE jobs SET state='running',node='mac',started=? WHERE id=?",
                       (self.now, other))
        positions = [self.job(job_id)["position"] for job_id in ids]
        self.now += 60
        self.heartbeat(allocs=[{"job_id": live, "attempt": 0}], mem_available=0)
        self.assertEqual([self.job(j)["state"] for j in ids], list(model.ACTIVE))
        self.now += 1
        reply = self.heartbeat(allocs=[{"job_id": live, "attempt": 0}], mem_available=0)
        self.assertEqual(reply, {"start": [], "preempt": [], "cancel": []})
        for job_id, position, state in zip(ids, positions, model.ACTIVE):
            job = self.job(job_id)
            if state == "cancelling":
                self.assertEqual((job["state"], job["attempt"], job["ended"], job["state_note"]),
                                 ("cancelled", 0, self.now, "cancelled; lost on node"))
                self.assertEqual((job["node"], job["started"], job["position"]),
                                 ("ai", self.now - 61, position))
                continue
            self.assertEqual((job["state"], job["attempt"], job["state_note"], job["position"]),
                             ("queued", 1, "lost on node", position))
            self.assertIsNone(job["node"])
            self.assertIsNone(job["started"])
            self.assertIsNone(job["ended"])
        for job_id in (live, other, hold):
            self.assertEqual((self.job(job_id)["state"], self.job(job_id)["attempt"]),
                             ("running", 0))
        audits = self.rows("SELECT * FROM audit WHERE action='reconcile' ORDER BY job_id")
        self.assertEqual([r["job_id"] for r in audits], ids)
        self.assertTrue(all(r["principal"] == "ai" for r in audits))
        self.assertEqual([json.loads(r["detail"])["reason"] for r in audits],
                         ["lost on node", "lost on node", "cancelled; lost on node"])
        self.heartbeat(allocs=[{"job_id": live, "attempt": 0}], mem_available=0)
        self.assertEqual(len(self.rows("SELECT * FROM audit WHERE action='reconcile'")), 3)

    def test_reconciliation_can_restart_in_same_heartbeat(self):
        job_id = self.submit(gpu=True)
        self.heartbeat()
        self.now += 61
        start = self.heartbeat()["start"]
        self.assertEqual([j["id"] for j in start], [job_id])
        self.assertEqual((start[0]["attempt"], start[0]["started"]), (1, self.now))

    def test_lost_cancelling_never_restarts_with_available_resources(self):
        job_id = self.submit(gpu=True)
        self.heartbeat()
        self.post(f"/jobs/{job_id}/cancel", {}, "alice")
        self.now += 61
        self.assertEqual(self.heartbeat(), {"start": [], "preempt": [], "cancel": []})
        self.assertEqual(self.job(job_id)["state"], "cancelled")
        self.assertEqual(self.heartbeat()["start"], [])
        self.post("/agent/report", {"job_id": job_id, "attempt": 0, "rc": 6},
                  "ai", status=409)
        self.assertEqual(self.job(job_id)["state"], "cancelled")

    def test_reconciliation_matches_job_and_attempt(self):
        job_id = self.submit()
        self.heartbeat()
        self.now += 61
        self.heartbeat(allocs=[{"job_id": job_id, "attempt": 1}], mem_available=0)
        self.assertEqual((self.job(job_id)["state"], self.job(job_id)["attempt"]),
                         ("queued", 1))
        self.assertEqual(self.heartbeat()["start"][0]["attempt"], 1)
        self.now += 61
        self.assertEqual(self.heartbeat(allocs=[{"job_id": job_id, "attempt": 1},
                                               {"job_id": job_id, "attempt": 0}])["start"], [])
        self.assertEqual((self.job(job_id)["state"], self.job(job_id)["attempt"]),
                         ("running", 1))

    def test_stale_reports_conflict_without_side_effects(self):
        job_id = self.submit()
        self.heartbeat()
        self.report(job_id, 6)
        stale = {"job_id": job_id, "attempt": 0, "rc": 0,
                 "peaks": {"peak_ram": 99*GB}, "fault": "kernel"}
        for restart in (False, True):
            with self.subTest(restart=restart):
                if restart:
                    self.heartbeat()
                before = self.job(job_id)
                audits = self.rows("SELECT * FROM audit")
                profiles = self.rows("SELECT * FROM profiles")
                self.post("/agent/report", stale, "ai", status=409)
                self.assertEqual(self.job(job_id), before)
                self.assertEqual(self.rows("SELECT * FROM audit"), audits)
                self.assertEqual(self.rows("SELECT * FROM profiles"), profiles)
                self.assertEqual(self.rows("SELECT status FROM nodes WHERE name='ai'")[0]["status"],
                                 "active")
        self.assertEqual(self.report(job_id, 0, attempt=1)["state"], "done")

    def test_attempt_input_validation_and_report_roles(self):
        job_id = self.submit()
        self.post("/agent/report", {"job_id": job_id, "attempt": 0, "rc": 0},
                  "ai", status=409)
        self.heartbeat()
        for who in ("owner", "ops", "alice"):
            self.post("/agent/report", {"job_id": job_id, "attempt": 0, "rc": 0},
                      who, status=403)
        for attempt in (None, True, -1, 0.0, "0"):
            with self.subTest(attempt=attempt):
                alloc = {"job_id": job_id}
                if attempt is not None:
                    alloc["attempt"] = attempt
                audits = self.rows("SELECT * FROM audit")
                nodes = self.rows("SELECT * FROM nodes")
                self.post("/agent/heartbeat", {"stats": {}, "allocs": [alloc]}, "ai", status=400)
                self.post("/agent/report", dict(alloc, rc=0), "ai", status=400)
                self.assertEqual(self.rows("SELECT * FROM audit"), audits)
                self.assertEqual(self.rows("SELECT * FROM nodes"), nodes)
                self.assertEqual(self.job(job_id)["state"], "running")

    def test_board_failures_do_not_fail_committed_requests(self):
        for error in (OSError("board unavailable"), ValueError("corrupt board"),
                      RuntimeError("board failure")):
            with self.subTest(error=error), patch("cq.coordinator.write_board", side_effect=error):
                with self.assertLogs(level="ERROR") as logs:
                    job_id = self.submit()
                    self.assertIn(job_id, [j["id"] for j in self.heartbeat()["start"]])
                self.assertTrue(any("board update failed" in line for line in logs.output))
                self.assertEqual(self.job(job_id)["state"], "running")
        self.board.write_text("{broken JSON")
        with self.assertLogs(level="ERROR"):
            job_id = self.submit()
            self.assertIn(job_id, [j["id"] for j in self.heartbeat()["start"]])

    def test_submit_requires_ram_or_profile_and_holds_are_exempt(self):
        for data in ({"argv": ["true"]}, {"argv": ["true"], "ram_req": 0, "kind": "new"}):
            self.post("/jobs", data, "alice", status=400)
        self.assertEqual(self.rows("SELECT * FROM jobs"), [])
        self.assertEqual(self.rows("SELECT * FROM audit"), [])
        hold = self.submit(argv=[], hold=True, ram_req=0, kind="new")
        self.assertEqual(self.job(hold)["ram_req"], 0)
        profiled = self.submit(kind="zero")
        self.heartbeat()
        self.post("/agent/report", {"job_id": profiled, "attempt": 0, "rc": 0, "peaks": {}}, "ai")
        zero = self.submit(kind="zero", ram_req=0)
        start = self.heartbeat()["start"]
        self.assertEqual([(j["id"], j["effective_ram"]) for j in start], [(zero, 256 * 2**20)])

    def test_hold_occupies_slot_but_never_reaches_agent(self):
        hold = self.submit(argv=[], hold=True, gpu=True, ram_req=0, hold_until=self.now + 10000)
        waiting = self.submit(gpu=True)
        cpu = self.submit()
        self.assertEqual([j["id"] for j in self.heartbeat()["start"]], [cpu])
        self.assertEqual((self.job(hold)["state"], self.job(hold)["node"]), ("running", "ai"))
        self.now += 61
        reply = self.heartbeat(allocs=[{"job_id": cpu, "attempt": 0}])
        self.assertTrue(all(hold not in reply[key] for key in ("preempt", "cancel")))
        self.assertEqual(reply["start"], [])
        self.assertEqual(self.job(hold)["attempt"], 0)
        self.post(f"/jobs/{hold}/finish", {}, "alice")
        self.assertEqual([j["id"] for j in self.heartbeat(allocs=[{"job_id": cpu, "attempt": 0}])["start"]],
                         [waiting])

    def test_unsolicited_cancelled_uses_rc_in_running_and_preempting(self):
        for state in ("running", "preempting"):
            for rc, expected in ((0, "done"), (6, "queued"), (125, "failed")):
                with self.subTest(state=state, rc=rc):
                    job_id = self.submit(kind=f"{state}-{rc}")
                    self.heartbeat()
                    with closing(model.connect(self.server.db_path)) as db, db:
                        db.execute("UPDATE jobs SET state=? WHERE id=?", (state, job_id))
                    self.assertEqual(self.report(job_id, rc, cancelled=True)["state"], expected)

    def test_cancelled_reports_require_cancelling(self):
        for rc in (0, 6, 125):
            for flag in (False, True):
                with self.subTest(rc=rc, cancelled=flag):
                    job_id = self.submit(kind=f"cancel-{rc}-{flag}")
                    self.heartbeat()
                    self.post(f"/jobs/{job_id}/cancel", {}, "alice")
                    self.assertEqual(self.report(job_id, rc, cancelled=flag)["state"], "cancelled")

    def test_submitter_priority_limits_on_submit_and_update(self):
        for priority in (-10, 0):
            job_id = self.submit(priority=priority)
            self.post(f"/jobs/{job_id}/priority", {"priority": -10 - priority}, "alice")
        for priority in (-11, 1):
            count = len(self.rows("SELECT * FROM audit"))
            self.post("/jobs", {"ram_req": GB, "priority": priority}, "alice", status=403)
            self.post(f"/jobs/{job_id}/priority", {"priority": priority}, "alice", status=403)
            self.assertEqual(len(self.rows("SELECT * FROM audit")), count)
        for who in ("owner", "ops"):
            unrestricted = self.submit(who=who, priority=20)
            self.post(f"/jobs/{unrestricted}/priority", {"priority": -20}, who)
        self.post(f"/jobs/{job_id}/priority", {"priority": 0}, "bob", status=403)

    def test_submitter_hold_limit_on_submit_default_and_renew(self):
        limit = self.now + 12 * 3600
        hold = self.submit(argv=[], hold=True, hold_until=limit)
        default = self.submit(argv=[], hold=True, hold_until=None)
        self.assertEqual(self.job(default)["hold_until"], limit)
        omitted = self.submit(argv=[], hold=True)
        self.assertEqual(self.job(omitted)["hold_until"], limit)
        self.post("/jobs", {"hold": True, "hold_until": limit + 1}, "alice", status=400)
        self.post(f"/jobs/{hold}/renew", {"hold_until": limit + 1}, "alice", status=400)
        self.assertEqual(self.job(hold)["hold_until"], limit)
        self.now += 60
        self.post(f"/jobs/{hold}/renew", {"hold_until": limit + 60}, "alice")
        for who in ("owner", "ops"):
            unlimited = self.submit(who=who, argv=[], hold=True, hold_until=limit + 86400)
            self.post(f"/jobs/{unlimited}/renew", {"hold_until": limit + 172800}, who)

    def test_bad_inputs_rollback_without_audit(self):
        for data in ({"ram_req": -1}, {"gpu": "yes"}, {"argv": "echo"},
                     {"deps": [999]}, {"env": {"X": 1}}, {"extra": 1},
                     {"ram_req": float("inf")}, {"priority": True}):
            self.post("/jobs", data, status=400)
        self.assertEqual(self.rows("SELECT * FROM jobs"), [])
        self.assertEqual(self.rows("SELECT * FROM audit"), [])
        self.post("/agent/heartbeat", {"stats": {"mem_available": -1}}, "ai", status=400)

    def test_cli_token_initialization_prints_once(self):
        state = self.root / "cli-state"
        state.mkdir(mode=0o755)
        base = [sys.executable, "-m", "cq.coordinator", "--state", str(state),
                "--board", str(self.root / "cli-board.json")]
        result = subprocess.run(base + ["--init-owner", "first"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(state.stat().st_mode & 0o777, 0o700)
        self.assertEqual((state / "queue.sqlite3").stat().st_mode & 0o777, 0o600)
        token = result.stdout.strip()
        self.assertGreater(len(token), 32)
        again = subprocess.run(base + ["--init-owner", "second"], capture_output=True, text=True)
        self.assertNotEqual(again.returncode, 0)
        self.assertEqual(again.stdout, "")
        added = subprocess.run(base + ["--add-token", "helper", "operator"], capture_output=True, text=True)
        self.assertEqual(added.returncode, 0, added.stderr)
        with closing(model.connect(state / "queue.sqlite3")) as db:
            row = db.execute("SELECT * FROM tokens WHERE principal='first'").fetchone()
            self.assertEqual(row["hash"], hashlib.sha256(token.encode()).hexdigest())
            self.assertNotIn(token, str(dict(row)))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit").fetchone()[0], 2)


class CoordinatorInProcess(CoordinatorAPI):
    """Exercise the same request/transaction path without loopback sockets."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 2_000_000_000
        self.board = self.root / "board.json"
        with patch("cq.coordinator.ThreadingHTTPServer.__init__", return_value=None):
            self.server = make_server({"state": self.root / "state", "board": self.board,
                                       "clock": lambda: self.now})
        with closing(model.connect(self.server.db_path)) as db, db:
            self.tokens = {name: add_token(db, name, role) for name, role in (
                ("owner", "owner"), ("ops", "operator"),
                ("alice", "submitter"), ("bob", "submitter"))}
            model.register_node(db, "ai", "linux", {"ram_total": 64*GB, "vram_total": 24*GB},
                                status="active", ram_floor=4*GB)
            self.tokens["ai"] = add_token(db, "ai", "node")

    def request(self, method, path, data=None, who="owner", token=None):
        handler = Handler.__new__(Handler)
        handler.server = self.server
        handler.path = path
        body = json.dumps(data).encode() if data is not None else b""
        handler.headers = {"Content-Length": str(len(body))}
        if who is not None:
            handler.headers["Authorization"] = "Bearer " + (token or self.tokens[who])
        handler.rfile = io.BytesIO(body)
        response = []
        handler.respond = lambda status, payload: response.append((status, payload))
        handler.handle_api(method == "POST")
        return response[0]


class BoardCompatibility(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.board = Path(self.temp.name) / "board.json"
        self.db = model.connect(Path(self.temp.name) / "queue.sqlite3")
        self.addCleanup(self.db.close)

    def test_replacement_preserves_previous_mode(self):
        self.board.write_text('{"next_id": 1, "jobs": []}')
        for mode in (0o600, 0o640, 0o644, 0o660):
            with self.subTest(mode=oct(mode)):
                self.board.chmod(mode)
                write_board(self.db, self.board)
                self.assertEqual(self.board.stat().st_mode & 0o7777, mode)
                write_board(self.db, self.board)
                self.assertEqual(self.board.stat().st_mode & 0o7777, mode)

    def test_new_board_mode_is_0644_under_restrictive_umask(self):
        previous = os.umask(0o077)
        try:
            write_board(self.db, self.board)
        finally:
            os.umask(previous)
        self.assertEqual(self.board.stat().st_mode & 0o7777, 0o644)

    def test_atomic_replacement_cloud_entries_and_numeric_times(self):
        cloud = {"id": 90, "resource": "cloud", "who": "remote"}
        self.board.write_text(json.dumps({"next_id": 91, "jobs": [cloud],
                                         "obsolete": "x" * 10000}))
        job_id = model.create_job(self.db, "alice", gpu=True, hold=True,
                                  node_pref="ai", hold_until=2000, now=1000)
        with self.board.open("a+") as legacy:
            inode = self.board.stat().st_ino
            write_board(self.db, self.board)
            self.assertNotEqual(self.board.stat().st_ino, inode)
            legacy.seek(0)
            self.assertIn("obsolete", json.load(legacy))
            board = json.loads(self.board.read_text())
            self.assertEqual(board["next_id"], 91)
            entry, preserved = board["jobs"]
            self.assertEqual(preserved, cloud)
            self.assertEqual((entry["id"], entry["booked"], entry["eta"]),
                             (job_id, 1000, 2000))
            model.expire_holds(self.db, 3801)
            write_board(self.db, self.board)
            self.assertEqual(json.loads(self.board.read_text())["jobs"], [cloud])
        self.assertFalse(self.board.with_suffix(".lock").exists())

    def test_failed_replace_keeps_previous_board_and_cleans_temp(self):
        original = '{"next_id": 3, "jobs": []}\n'
        self.board.write_text(original)
        real_replace = os.replace

        def fail_replace(source, target):
            self.assertEqual(Path(source).parent, self.board.parent)
            self.assertEqual(Path(target), self.board)
            self.assertEqual(self.board.read_text(), original)
            self.assertEqual(json.loads(Path(source).read_text())["next_id"], 3)
            raise OSError("rename failed")

        with patch("cq.coordinator.os.replace", side_effect=fail_replace):
            with self.assertRaises(OSError):
                write_board(self.db, self.board)
        self.assertEqual(self.board.read_text(), original)
        self.assertEqual(list(self.board.parent.glob("board.json.*")), [])
        with patch("cq.coordinator.os.replace", wraps=real_replace) as replace:
            write_board(self.db, self.board)
            replace.assert_called_once()

    def test_board_symlink_is_not_followed(self):
        target = self.board.parent / "target.json"
        target.write_text('{"secret": true}')
        self.board.symlink_to(target)
        with self.assertRaises(OSError):
            write_board(self.db, self.board)
        self.assertEqual(target.read_text(), '{"secret": true}')

    def test_writer_holds_legacy_board_lock(self):
        real_flock = fcntl.flock
        probe = """import fcntl, sys
with open(sys.argv[1], 'a+') as stream:
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(0)
    sys.exit(1)
"""

        def lock_and_probe(stream, operation):
            real_flock(stream, operation)
            result = subprocess.run([sys.executable, "-c", probe, str(self.board)],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)

        with patch("cq.coordinator.fcntl.flock", side_effect=lock_and_probe) as lock:
            write_board(self.db, self.board)
            lock.assert_called_once()


class StoragePermissions(unittest.TestCase):
    def test_every_start_and_connection_tightens_existing_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            state.mkdir(mode=0o755)
            config = {"state": state, "board": Path(directory) / "board.json"}
            with patch("cq.coordinator.ThreadingHTTPServer.__init__", return_value=None):
                server = make_server(config)
                with closing(model.connect(server.db_path)) as db:
                    paths = [Path(str(server.db_path) + suffix) for suffix in ("", "-wal", "-shm")]
                    for path in paths:
                        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                        path.chmod(0o666)
                    state.chmod(0o777)
                    make_server(config)
                    self.assertEqual(state.stat().st_mode & 0o777, 0o700)
                    for path in paths:
                        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                        path.chmod(0o666)
                    with closing(model.connect(server.db_path)):
                        for path in paths:
                            self.assertEqual(path.stat().st_mode & 0o777, 0o600)


class SchedulerModel(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = model.connect(Path(self.temp.name) / "queue.sqlite3")
        self.addCleanup(self.db.close)
        self.now = 2000
        model.register_node(self.db, "ai", "linux", {"ram_total": 64*GB, "vram_total": 24*GB},
                            status="active", ram_floor=4*GB)
        self.stats()

    def stats(self, **extra):
        stats = {"mem_available": 20*GB, "vram_used": 0, "outside_gpu_busy": False}
        stats.update(extra)
        model.record_heartbeat(self.db, "ai", stats, self.now)

    def create(self, **extra):
        return model.create_job(self.db, "alice", now=self.now, **extra)

    def plan(self):
        return model.plan_for_node(self.db, "ai", self.now)

    def job(self, job_id):
        return model.job_dict(self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def test_schema_wal_defaults_and_permissions(self):
        self.assertIsInstance(self.db.execute("SELECT * FROM nodes").fetchone(), sqlite3.Row)
        self.assertEqual(self.db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        node = self.db.execute("SELECT * FROM nodes").fetchone()
        self.assertEqual(node["vram_cap"], 19.5*GB)
        model.register_node(self.db, "mac", "darwin", {"ram_total": 32*GB})
        mac = self.db.execute("SELECT * FROM nodes WHERE name='mac'").fetchone()
        self.assertEqual((mac["vram_total"], mac["ram_floor"], mac["status"]),
                         (0, 4*2**30, "pending"))
        own, other = {"submitter": "alice"}, {"submitter": "bob"}
        self.assertTrue(model.allowed("submitter", "alice", "cancel", own))
        self.assertFalse(model.allowed("operator", "alice", "cancel", other))
        self.assertTrue(model.allowed("operator", "alice", "move", other))
        self.assertFalse(model.allowed("submitter", "alice", "urgent", own))
        self.assertTrue(model.allowed("node", "ai", "heartbeat", {"node": "ai"}))
        self.assertFalse(model.allowed("node", "ai", "heartbeat", {"node": "mac"}))

    def test_queue_order_node_preference_and_dependencies(self):
        dep = self.create(node_pref="mac")
        wait = self.create(deps=[dep], urgent=True)
        low = self.create(priority=0)
        high = self.create(priority=4)
        urgent = self.create(urgent=True, priority=-1)
        plan = self.plan()
        self.assertEqual([j["id"] for j in plan["start"]], [urgent, high, low])
        self.assertEqual(self.job(wait)["state"], "queued")
        self.db.execute("UPDATE jobs SET state='failed' WHERE id=?", (dep,))
        self.assertEqual(self.plan()["blocked"], [wait])
        self.assertIn(str(dep), self.job(wait)["state_note"])

    def test_head_backfill_and_resource_deduction(self):
        head = self.create(ram_req=20*GB)
        long = self.create(ram_req=GB, est_s=1801)
        short = self.create(ram_req=2*GB, est_s=1800)
        lease = self.create(ram_req=GB, leaseable=True)
        unknown = self.create(ram_req=GB)
        self.assertEqual([j["id"] for j in self.plan()["start"]], [short, lease])
        self.assertTrue(all(self.job(j)["state"] == "queued" for j in (head, long, unknown)))

    def test_pending_growth_and_gpu_outside_load(self):
        first = self.create(ram_req=10*GB)
        self.assertEqual(self.plan()["start"][0]["id"], first)
        second = self.create(ram_req=4*GB)
        self.stats(mem_available=16*GB, allocs=[{"job_id": first, "current_ram": 2*GB}])
        self.assertEqual(self.plan()["start"], [])
        model.report(self.db, first, 0, {"peak_ram": 10*GB, "peak_vram": 0}, 1, self.now)
        self.stats(outside_gpu_busy=True)
        gpu = self.create(gpu=True, vram_req=GB, kind="gpu")
        self.assertEqual([j["id"] for j in self.plan()["start"]], [second])
        self.assertEqual(self.job(gpu)["state"], "queued")

    def test_pending_growth_matches_allocation_attempt(self):
        first = self.create(ram_req=10*GB)
        self.plan()
        self.db.execute("UPDATE jobs SET attempt=1 WHERE id=?", (first,))
        waiting = self.create(ram_req=4*GB)
        self.stats(mem_available=14*GB, allocs=[
            {"job_id": first, "attempt": 0, "current_ram": 13*GB}])
        self.assertEqual(self.plan()["start"], [])
        self.stats(mem_available=14*GB, allocs=[
            {"job_id": first, "attempt": 1, "current_ram": 13*GB},
            {"job_id": first, "attempt": 0, "current_ram": 0}])
        self.assertEqual([j["id"] for j in self.plan()["start"]], [waiting])

    def test_independent_jobs_preserve_head_ram(self):
        self.stats(outside_gpu_busy=True)
        head = self.create(gpu=True, ram_req=8*GB)
        unknown = self.create(ram_req=GB)
        long = self.create(ram_req=GB, est_s=7200)
        drains_head_ram = self.create(ram_req=3*GB)
        self.assertEqual([j["id"] for j in self.plan()["start"]], [unknown, long])
        self.assertEqual(self.job(head)["state"], "queued")
        self.assertEqual(self.job(drains_head_ram)["state"], "queued")

    def test_unknown_gpu_cannot_backfill_gpu_head(self):
        self.stats(vram_used=15*GB)
        head = self.create(gpu=True, vram_req=6*GB)
        unknown = self.create(gpu=True, vram_req=2*GB)
        short = self.create(gpu=True, vram_req=2*GB, est_s=1800)
        self.assertEqual([j["id"] for j in self.plan()["start"]], [short])
        self.assertEqual(self.job(head)["state"], "queued")
        self.assertEqual(self.job(unknown)["state"], "queued")

    def test_vram_cap_and_one_gpu_allocation(self):
        too_big = self.create(gpu=True, vram_req=20*GB)
        short = self.create(gpu=True, vram_req=GB, est_s=100)
        another = self.create(gpu=True, vram_req=GB, leaseable=True)
        self.assertEqual([j["id"] for j in self.plan()["start"]], [short])
        self.assertEqual(self.job(too_big)["state"], "queued")
        self.assertEqual(self.job(another)["state"], "queued")

    def test_profile_uses_last_five_and_default_wall_time(self):
        for i in range(7):
            job_id = self.create(kind="profile", ram_req=1)
            self.plan()
            model.report(self.db, job_id, 0 if i % 2 else 6,
                         {"peak_ram": 100-i, "peak_vram": 200-i}, i+1, self.now)
            if i % 2 == 0:
                self.db.execute("UPDATE jobs SET state='cancelled' WHERE id=?", (job_id,))
        runs = json.loads(self.db.execute("SELECT runs FROM profiles WHERE kind='profile'").fetchone()[0])
        self.assertEqual(len(runs), 5)
        job = self.job(self.create(kind="profile", ram_req=1, vram_req=1))
        self.assertEqual(model.effective(self.db, job), (256 * 2**20, 198 + .5*GB, 7))
        job["est_s"] = 10
        self.assertEqual(model.effective(self.db, job)[2], 10)

    def test_memory_cap_floor_request_and_profile_maximum(self):
        for peaks, request, expected in (([], 0, 256 * 2**20),
                                         ([0, 0], 0, 256 * 2**20),
                                         ([0], GB, 1.3 * GB),
                                         ([0, GB, 2*GB], GB, 2.6 * GB),
                                         ([GB], 4*GB, 5.2 * GB)):
            with self.subTest(peaks=peaks, request=request):
                self.db.execute("DELETE FROM profiles")
                if peaks:
                    runs = [{"peak_ram": p, "peak_vram": 0, "wall_s": 10} for p in peaks]
                    self.db.execute("INSERT INTO profiles VALUES (?,?)", ("cap", json.dumps(runs)))
                job_id = self.create(kind="cap", ram_req=request)
                self.assertEqual(model.effective(self.db, self.job(job_id))[0], expected)

    def test_running_holds_reserve_ram_without_start_specs(self):
        hold = self.create(hold=True, ram_req=10*GB)
        waiting = self.create(ram_req=4*GB)
        self.assertEqual(self.plan()["start"], [])
        self.assertEqual(self.job(hold)["state"], "running")
        self.assertEqual(self.job(waiting)["state"], "queued")
        self.assertEqual(self.plan()["start"], [])

    def test_urgent_and_aged_preemption_respects_holds_and_priority(self):
        lease = self.create(gpu=True, leaseable=True, ram_req=GB)
        self.plan()
        self.stats(vram_used=GB, allocs=[{"job_id": lease, "current_ram": GB,
                                          "current_vram": GB}])
        head = self.create(gpu=True, urgent=True, priority=1, ram_req=GB)
        plan = self.plan()
        self.assertEqual(plan["preempt"], [lease])
        self.assertEqual(self.job(head)["state"], "queued")
        model.report(self.db, lease, 6, {}, 1, self.now)
        self.assertEqual(self.plan()["start"][0]["id"], head)
        model.report(self.db, head, 0, {}, 1, self.now)
        self.db.execute("UPDATE jobs SET state='cancelled' WHERE id=?", (lease,))
        hold = self.create(gpu=True, hold=True, leaseable=True)
        self.plan()
        self.create(gpu=True, urgent=True, priority=99)
        self.assertEqual(self.plan()["preempt"], [])
        self.assertEqual(self.job(hold)["state"], "running")

    def test_waited_ten_minutes_preemption(self):
        lease = self.create(gpu=True, leaseable=True)
        self.plan()
        self.create(gpu=True, priority=1)
        self.now += 600
        self.assertEqual(self.plan()["preempt"], [])
        self.now += 1
        self.assertEqual(self.plan()["preempt"], [lease])

    def test_preemption_reclaims_measured_ram_above_profile(self):
        first = self.create(ram_req=GB, leaseable=True)
        second = self.create(ram_req=GB, leaseable=True)
        self.plan()
        self.stats(mem_available=4*GB, allocs=[
            {"job_id": first, "current_ram": 8*GB},
            {"job_id": second, "current_ram": GB}])
        self.create(ram_req=5*GB, priority=1, urgent=True)
        self.assertEqual(self.plan()["preempt"], [first])

    def test_preemption_ignores_measured_ram_from_stale_attempt(self):
        first = self.create(ram_req=GB, leaseable=True)
        second = self.create(ram_req=GB, leaseable=True)
        self.plan()
        self.db.execute("UPDATE jobs SET attempt=1 WHERE id=?", (first,))
        self.stats(mem_available=4*GB, allocs=[
            {"job_id": first, "attempt": 0, "current_ram": 8*GB},
            {"job_id": second, "attempt": 0, "current_ram": GB}])
        self.create(ram_req=5*GB, priority=1, urgent=True)
        self.assertEqual(self.plan()["preempt"], [first, second])

    def test_inactive_nodes_and_hold_grace(self):
        job_id = self.create(hold=True, hold_until=self.now)
        model.set_node_status(self.db, "ai", "faulted")
        self.assertEqual(self.plan(), {"start": [], "preempt": [], "blocked": []})
        self.assertEqual(model.expire_holds(self.db, self.now+1799), [])
        self.assertEqual(model.expire_holds(self.db, self.now+1800), [])
        self.assertEqual(model.expire_holds(self.db, self.now+1801), [job_id])
        self.assertEqual(model.expire_holds(self.db, self.now+1900), [])


if __name__ == "__main__":
    unittest.main()
