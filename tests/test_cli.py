"""CLI contract tests against a loopback Coordinator stub."""

import contextlib
import http.server
import io
import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from cq.cli import computeboard_main, main, parse_eta


class FakeCoordinator:
    def __init__(self):
        self.requests = []
        self.jobs = [{
            "id": 7, "state": "queued", "submitter": "alice", "node_pref": "ai",
            "gpu": True, "kind": "test", "note": "queued job", "hold_until": time.time() + 1800,
        }]
        self.nodes = [{
            "name": "ai", "status": "active",
            "stats": {"mem_available": 12 * 2**30, "vram_used": 3 * 2**30,
                      "outside_gpu_busy": False},
        }]
        parent = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def _body(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                return json.loads(raw) if raw else None

            def _send(self, value, status=200):
                raw = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _record(self):
                body = self._body() if self.command != "GET" else None
                parent.requests.append((self.command, self.path, body,
                                        self.headers.get("Authorization")))
                return body

            def do_GET(self):
                self._record()
                if self.path == "/jobs":
                    self._send({"jobs": parent.jobs})
                elif self.path == "/nodes":
                    self._send({"nodes": parent.nodes})
                elif self.path.startswith("/jobs/"):
                    job_id = int(self.path.rsplit("/", 1)[1])
                    job = next((item for item in parent.jobs if item["id"] == job_id),
                               {"id": job_id, "state": "queued"})
                    self._send(job)
                else:
                    self._send({})

            def do_POST(self):
                body = self._record()
                if self.path == "/jobs":
                    self._send({"id": 44, "state": "queued"})
                elif self.path.startswith("/jobs/") and self.path.endswith("/finish"):
                    self._send({"state": "done"})
                elif self.path == "/nodes/ai/approve":
                    self._send({"status": "active"})
                else:
                    self._send({"ok": True, **(body or {})})

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = Path(self.temp.name)
        config_dir = self.home / ".config" / "compute-queue"
        config_dir.mkdir(parents=True)
        (config_dir / "token").write_text("test-token\n")
        (config_dir / "cli.json").write_text(json.dumps({"coordinator": "http://invalid"}))
        os.chmod(config_dir / "token", 0o600)
        self.server = FakeCoordinator()
        self.patch_home = mock.patch("pathlib.Path.home", return_value=self.home)
        self.patch_home.start()
        self.patch_env = mock.patch.dict(os.environ, {
            "CQ_URL": self.server.url,
            "CQ_CLOUD_BOARD": str(self.home / "cloud-board.json"),
        })
        self.patch_env.start()

    def tearDown(self):
        self.patch_env.stop()
        self.patch_home.stop()
        self.server.close()
        self.temp.cleanup()

    def invoke(self, function, args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = function(args)
        self.assertEqual(result, 0)
        return output.getvalue().strip()

    def test_selected_profile_uses_its_token_and_coordinator(self):
        profile = self.home / "profiles" / "operator"
        profile.mkdir(parents=True)
        (profile / "token").write_text("operator-token")
        (profile / "token").chmod(0o600)
        (profile / "cli.json").write_text(json.dumps({"coordinator": self.server.url}))
        with mock.patch.dict(os.environ, {"CQ_CONFIG_DIR": str(profile), "CQ_URL": ""}):
            self.invoke(main, ["ls"])
        self.assertEqual(self.server.requests[-1][3], "Bearer operator-token")

    def test_missing_selected_token_never_falls_back_to_owner(self):
        with mock.patch.dict(os.environ, {"CQ_CONFIG_DIR": str(self.home / "missing")}), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["ls"]), 2)
        self.assertEqual(self.server.requests, [])

    def test_parse_eta_rolls_past_clock_time_to_tomorrow(self):
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 10, 20, 16, 0)

        now = FrozenDateTime.now().timestamp()
        with mock.patch("cq.cli.datetime", FrozenDateTime), \
                mock.patch("cq.cli.time.time", return_value=now):
            eta = parse_eta("15:59")

        self.assertEqual(eta, FrozenDateTime(2026, 10, 21, 15, 59).timestamp())

    def test_cq_commands_send_contract_requests(self):
        out = self.invoke(main, [
            "submit", "--gpu", "--ram", "6G", "--after", "3", "--kind", "render",
            "--note", "sample", "--", "python3", "-c", "print('ok')",
        ])
        self.assertEqual(out, "submitted #44")
        method, path, body, auth = self.server.requests[-1]
        self.assertEqual((method, path), ("POST", "/jobs"))
        self.assertEqual(auth, "Bearer test-token")
        self.assertEqual(body, {
            "argv": ["python3", "-c", "print('ok')"], "node_pref": "any", "gpu": True,
            "ram_req": 6 * 2**30, "vram_req": 0, "priority": 0, "kind": "render",
            "note": "sample", "deps": [3], "leaseable": False, "est_s": None,
            "cwd": os.getcwd(), "env": {},
        })

        out = self.invoke(main, ["ls"])
        self.assertEqual(out, "#7 queued ai test queued job")
        self.assertEqual(self.server.requests[-1][:2], ("GET", "/jobs"))

        out = self.invoke(main, ["cancel", "7", "--reason", "finished with it"])
        self.assertEqual(out, "#7 -> cancelled")
        self.assertEqual(self.server.requests[-1][:3],
                         ("POST", "/jobs/7/cancel", {"reason": "finished with it"}))

        out = self.invoke(main, ["move", "7", "--before", "4"])
        self.assertEqual(out, "#7 moved before #4")
        self.assertEqual(self.server.requests[-1][:3],
                         ("POST", "/jobs/7/move", {"before": 4}))

        out = self.invoke(main, ["prio", "7", "9", "--urgent"])
        self.assertEqual(out, "#7 priority 9 urgent")
        self.assertEqual(self.server.requests[-1][:3],
                         ("POST", "/jobs/7/priority", {"priority": 9, "urgent": True}))

        out = self.invoke(main, ["node", "approve", "ai"])
        self.assertEqual(out, "approved ai")
        self.assertEqual(self.server.requests[-1][:3], ("POST", "/nodes/ai/approve", {}))

    def test_submit_requires_ram_or_kind_before_request(self):
        output = io.StringIO()
        error = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
            result = main(["submit", "--", "python3", "-c", "pass"])

        self.assertEqual(result, 2)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("submit requires --ram when --kind is not provided", error.getvalue())
        self.assertEqual(self.server.requests, [])

        self.invoke(main, ["submit", "--kind", "render", "--", "python3", "-c", "pass"])
        self.assertEqual(self.server.requests[-1][2]["ram_req"], 0)

    def test_token_file_wider_than_0600_warns(self):
        token_path = self.home / ".config" / "compute-queue" / "token"
        os.chmod(token_path, 0o644)
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            self.invoke(main, ["ls"])

        self.assertIn("warning: token file", error.getvalue())
        self.assertIn("0644, wider than 0600", error.getvalue())

    def test_computeboard_gpu_book_done_eta_and_status(self):
        before = time.time()
        out = self.invoke(computeboard_main,
                          ["book", "gpu", "alice", "--eta", "30m", "--note", "test hold"])
        self.assertRegex(out, r"^booked #44 gpu queued eta \d\d:\d\d$")
        method, path, body, auth = self.server.requests[-1]
        self.assertEqual((method, path), ("POST", "/jobs"))
        self.assertEqual(auth, "Bearer test-token")
        self.assertNotIn("submitter", body)
        self.assertEqual(body["node_pref"], "ai")
        self.assertTrue(body["gpu"])
        self.assertTrue(body["hold"])
        self.assertEqual(body["note"], "who=alice test hold")
        self.assertGreaterEqual(body["hold_until"], before + 30 * 60)

        out = self.invoke(computeboard_main, ["done", "44"])
        self.assertEqual(out, "#44 -> done")
        self.assertEqual(self.server.requests[-1][:3], ("POST", "/jobs/44/finish", {}))

        out = self.invoke(computeboard_main, ["eta", "44", "45m"])
        self.assertEqual(out, "#44 -> queued")
        self.assertIn(("POST", "/jobs/44/renew"),
                      [(method, path) for method, path, _body, _auth in self.server.requests])
        renew = next(item for item in reversed(self.server.requests) if item[1] == "/jobs/44/renew")
        self.assertGreaterEqual(renew[2]["hold_until"], time.time() + 45 * 60 - 2)

        out = self.invoke(computeboard_main, ["status"])
        self.assertIn("ai GPU lock: free", out)
        self.assertIn("ai VRAM used: 3.0 GB", out)
        self.assertIn("ai MemAvailable: 12.0 GB", out)
        self.assertIn("[gpu]", out)
        expected_job_line = (
            f"  #7 queued  alice  eta "
            f"{time.strftime('%H:%M', time.localtime(self.server.jobs[0]['hold_until']))}"
            "  queued job"
        )
        self.assertIn(expected_job_line, out)
        paths = [path for _method, path, _body, _auth in self.server.requests]
        self.assertEqual(paths[-2:], ["/jobs", "/nodes"])

    def test_computeboard_mac_book_holds_the_gpu(self):
        out = self.invoke(computeboard_main, ["book", "mac", "alice", "--eta", "30m"])
        self.assertRegex(out, r"^booked #44 mac ")
        body = self.server.requests[-1][2]
        self.assertEqual(body["node_pref"], "mac")
        self.assertTrue(body["gpu"])
        self.assertTrue(body["hold"])

    def test_computeboard_start_never_sets_urgent(self):
        out = self.invoke(computeboard_main, ["start", "7"])
        self.assertEqual(out, "#7 -> queued")
        self.assertEqual(self.server.requests[-2][0:3],
                         ("POST", "/jobs/7/priority", {"priority": 0}))
        self.assertNotIn("urgent", self.server.requests[-2][2])
        self.assertEqual(self.server.requests[-1][:2], ("GET", "/jobs/7"))

    def test_computeboard_book_start_respects_submitter_priority_limit(self):
        self.invoke(computeboard_main,
                    ["book", "gpu", "alice", "--eta", "30m", "--start"])
        body = self.server.requests[-1][2]
        self.assertNotIn("submitter", body)
        self.assertEqual(body["priority"], 0)
        self.assertEqual(body["note"], "who=alice")


if __name__ == "__main__":
    unittest.main()
