"""Command line clients for the compute queue Coordinator."""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_URL = "http://127.0.0.1:8765"
ACTIVE_STATES = {"queued", "running", "preempting", "cancelling"}
RESOURCES = ("gpu", "mac", "cloud")
GPU_LOCK = Path(os.environ.get("CQ_GPU_LOCK") or "/tmp/compute-queue-gpu.lock")


class CliError(Exception):
    """An expected CLI or Coordinator error."""


def _config_dir():
    return Path.home() / ".config" / "compute-queue"


class Coordinator:
    """Send authenticated JSON requests to the Coordinator."""

    def __init__(self):
        config_dir = _config_dir()
        token_path = config_dir / "token"
        try:
            self.token = token_path.read_text().strip()
        except OSError as exc:
            raise CliError(f"cannot read token at {token_path}: {exc.strerror}") from exc
        if not self.token:
            raise CliError(f"empty token at {token_path}")
        token_mode = token_path.stat().st_mode & 0o777
        if token_mode & 0o077:
            print(f"cq: warning: token file {token_path} has permissions "
                  f"{token_mode:04o}, wider than 0600", file=sys.stderr)

        url = os.environ.get("CQ_URL")
        if not url:
            try:
                config = json.loads((config_dir / "cli.json").read_text())
            except FileNotFoundError:
                config = {}
            except (OSError, json.JSONDecodeError) as exc:
                raise CliError(f"cannot read {config_dir / 'cli.json'}: {exc}") from exc
            url = config.get("coordinator", config.get("url", DEFAULT_URL))
        self.url = url.rstrip("/")

    def request(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": f"Bearer {self.token}"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.url + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace").strip()
            raise CliError(f"HTTP {exc.code}: {detail or exc.reason}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise CliError(f"Coordinator request failed: {exc}") from exc
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CliError("Coordinator returned invalid JSON") from exc


def parse_size(value):
    """Parse a byte count with optional binary units."""
    text = value.strip().upper()
    units = {"": 1, "B": 1, "K": 2**10, "KB": 2**10, "M": 2**20,
             "MB": 2**20, "G": 2**30, "GB": 2**30, "T": 2**40, "TB": 2**40}
    number = text
    unit = ""
    while number and number[-1].isalpha():
        unit = number[-1] + unit
        number = number[:-1]
    if unit not in units:
        raise argparse.ArgumentTypeError(f"invalid size: {value}")
    try:
        result = float(number) * units[unit]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid size: {value}") from exc
    if result < 0:
        raise argparse.ArgumentTypeError("size must be non-negative")
    return int(result)


def parse_duration(value):
    """Parse a duration such as 30m or 2h into seconds."""
    text = value.strip().lower()
    factors = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text and text[-1] in factors:
        try:
            seconds = float(text[:-1]) * factors[text[-1]]
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid duration: {value}") from exc
    else:
        try:
            seconds = float(text)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid duration: {value}") from exc
    if seconds < 0:
        raise argparse.ArgumentTypeError("duration must be non-negative")
    return int(seconds)


def parse_eta(value):
    """Return an ETA timestamp; past clock values roll over to tomorrow."""
    text = value.strip()
    if text.lower().endswith("m"):
        try:
            return time.time() + int(text[:-1]) * 60
        except ValueError as exc:
            raise CliError(f"invalid ETA: {value}") from exc
    try:
        hour, minute = map(int, text.split(":"))
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError
        eta = datetime.now().replace(hour=hour, minute=minute, second=0,
                                     microsecond=0)
        if eta.timestamp() < time.time():
            eta += timedelta(days=1)
        return eta.timestamp()
    except ValueError as exc:
        raise CliError(f"invalid ETA (use HH:MM or Nm): {value}") from exc


def format_eta(timestamp):
    return datetime.fromtimestamp(float(timestamp)).strftime("%H:%M") if timestamp else "-"


def _env_pairs(values):
    result = {}
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise CliError(f"invalid environment entry {item!r}; use KEY=VALUE")
        result[key] = value
    return result


def _items(payload, key):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        value = payload.get(key, [])
        return value if isinstance(value, list) else []
    return []


def _id(payload):
    if isinstance(payload, dict) and "id" in payload:
        return payload["id"]
    raise CliError("Coordinator response did not include an id")


def _job_line(job):
    node = job.get("node") or job.get("node_pref") or "any"
    fields = [f"#{job.get('id', '?')}", str(job.get("state", "?")), str(node)]
    if job.get("kind"):
        fields.append(str(job["kind"]))
    if job.get("note"):
        fields.append(str(job["note"]))
    return " ".join(fields)


def _make_parser():
    parser = argparse.ArgumentParser(prog="cq")
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit")
    submit.add_argument("--node", default="any")
    submit.add_argument("--gpu", action="store_true")
    submit.add_argument("--ram", type=parse_size)
    submit.add_argument("--vram", type=parse_size, default=0)
    submit.add_argument("--priority", type=int, default=0)
    submit.add_argument("--kind", default="")
    submit.add_argument("--note", default="")
    submit.add_argument("--after", action="append", type=int, default=[])
    submit.add_argument("--leaseable", action="store_true")
    submit.add_argument("--est", type=parse_duration)
    submit.add_argument("--cwd", default=os.getcwd())
    submit.add_argument("--env", action="append", default=[])
    submit.add_argument("argv", nargs=argparse.REMAINDER)

    ls = sub.add_parser("ls")
    ls.add_argument("--all", action="store_true")
    show = sub.add_parser("show")
    show.add_argument("id", type=int)
    cancel = sub.add_parser("cancel")
    cancel.add_argument("id", type=int)
    cancel.add_argument("--reason", default="cancelled via cq")
    move = sub.add_parser("move")
    move.add_argument("id", type=int)
    move.add_argument("--before", type=int, required=True)
    prio = sub.add_parser("prio")
    prio.add_argument("id", type=int)
    prio.add_argument("priority", type=int)
    prio.add_argument("--urgent", action="store_true")
    preempt = sub.add_parser("preempt")
    preempt.add_argument("id", type=int)
    preempt.add_argument("--reason", required=True)

    node = sub.add_parser("node")
    node_sub = node.add_subparsers(dest="node_command", required=True)
    node_sub.add_parser("ls")
    for name in ("invite", "approve", "disable", "clear-fault"):
        command = node_sub.add_parser(name)
        command.add_argument("name")
    profile = sub.add_parser("profile")
    profile_sub = profile.add_subparsers(dest="profile_command", required=True)
    profile_sub.add_parser("ls")
    return parser


def main(argv=None):
    args = _make_parser().parse_args(argv)
    try:
        command = args.command
        if command == "submit" and args.ram is None and not args.kind:
            raise CliError("submit requires --ram when --kind is not provided")
        api = Coordinator()
        if command == "submit":
            job_argv = list(args.argv)
            if job_argv[:1] == ["--"]:
                job_argv.pop(0)
            if not job_argv:
                raise CliError("submit requires a command after --")
            payload = {
                "argv": job_argv,
                "node_pref": args.node,
                "gpu": args.gpu,
                "ram_req": args.ram if args.ram is not None else 0,
                "vram_req": args.vram,
                "priority": args.priority,
                "kind": args.kind,
                "note": args.note,
                "deps": args.after,
                "leaseable": args.leaseable,
                "est_s": args.est,
                "cwd": args.cwd,
                "env": _env_pairs(args.env),
            }
            print(f"submitted #{_id(api.request('POST', '/jobs', payload))}")
        elif command == "ls":
            jobs = _items(api.request("GET", "/jobs"), "jobs")
            if not args.all:
                jobs = [job for job in jobs if job.get("state") in ACTIVE_STATES]
            for job in jobs:
                print(_job_line(job))
        elif command == "show":
            payload = api.request("GET", f"/jobs/{args.id}")
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        elif command == "cancel":
            api.request("POST", f"/jobs/{args.id}/cancel", {"reason": args.reason})
            print(f"#{args.id} -> cancelled")
        elif command == "move":
            api.request("POST", f"/jobs/{args.id}/move", {"before": args.before})
            print(f"#{args.id} moved before #{args.before}")
        elif command == "prio":
            body = {"priority": args.priority}
            if args.urgent:
                body["urgent"] = True
            api.request("POST", f"/jobs/{args.id}/priority", body)
            suffix = " urgent" if args.urgent else ""
            print(f"#{args.id} priority {args.priority}{suffix}")
        elif command == "preempt":
            api.request("POST", f"/jobs/{args.id}/preempt", {"reason": args.reason})
            print(f"#{args.id} preempt requested")
        elif command == "node":
            if args.node_command == "ls":
                payload = api.request("GET", "/nodes")
                for item in _items(payload, "nodes"):
                    print(f"{item.get('name', '?')} {item.get('status', '?')}")
            elif args.node_command == "invite":
                payload = api.request("POST", "/nodes/invite", {"name": args.name})
                print(payload.get("code", ""))
            else:
                endpoint = {"approve": "approve", "disable": "disable",
                            "clear-fault": "clear-fault"}[args.node_command]
                api.request("POST", f"/nodes/{urllib.parse.quote(args.name, safe='')}/{endpoint}", {})
                print(f"{args.node_command}d {args.name}" if args.node_command != "clear-fault"
                      else f"cleared fault on {args.name}")
        elif command == "profile":
            payload = api.request("GET", "/profiles")
            for item in _items(payload, "profiles"):
                print(json.dumps(item, ensure_ascii=False, sort_keys=True))
    except CliError as exc:
        print(f"cq: {exc}", file=sys.stderr)
        return 2
    return 0


def _ram_gb(node):
    stats = node.get("stats") or {}
    value = stats.get("mem_available", node.get("mem_available", 0))
    return float(value or 0) / 2**30


def _vram_gb(node):
    stats = node.get("stats") or {}
    value = stats.get("vram_used", node.get("vram_used", 0))
    return float(value or 0) / 2**30


def _route_mac_state():
    """Read the Mac battery state and probe its mllock lock."""
    probe = ("pmset -g batt | head -1; python3 -c \"import fcntl,pathlib;f=open(pathlib.Path.home()/'.ml-queue.lock','a+');"
             "\ntry:\n fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB); print('LOCKFREE')\n"
             "except OSError: print('LOCKHELD')\"")
    target = os.environ.get("CQ_MAC_SSH")
    if not target:
        return "down", "CQ_MAC_SSH not set"
    try:
        out = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=6", "-o", "BatchMode=yes",
             target, probe], capture_output=True, text=True, timeout=20
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return "down", "ssh failed"
    if "AC Power" not in out:
        return ("down", "no answer") if not out.strip() else ("battery", "on battery")
    return ("free", "AC, mllock free") if "LOCKFREE" in out else ("busy", "mllock held")


def route(kind, ram_gb):
    """Choose a lane now using the legacy computeboard rules."""
    try:
        available = next((int(line.split()[1]) / 2**20
                          for line in open("/proc/meminfo")
                          if line.startswith("MemAvailable")), 0)
    except OSError:
        available = 0
    lock = GPU_LOCK.read_text().strip() if GPU_LOCK.exists() else ""
    if kind == "light":
        return ("ai", f"MemAvailable {available:.1f} GB") if available >= 6 else (
            "wait", f"ai MemAvailable {available:.1f} GB < 6")
    if kind == "cpu":
        if available >= max(12, ram_gb + 6):
            return "ai", f"MemAvailable {available:.1f} GB"
        state, why = _route_mac_state()
        if state == "free" and ram_gb <= 20:
            return "mac", f"ai RAM {available:.1f} GB too low; Mac {why} (use mllock --wait, book computeboard mac)"
        return "wait", f"ai RAM {available:.1f} GB; Mac {state}: {why}"
    if kind == "gpu-train":
        if not lock and available >= 12:
            return "ai-gpu", f"lock free, MemAvailable {available:.1f} GB"
        state, why = _route_mac_state()
        if state == "free":
            return "mac", (f"ai GPU busy ({lock or 'RAM low'}); Mac {why} — MPS training "
                           "(T28b proved 800 LoRA steps), chunks <=2-3 h, mllock --wait, book computeboard mac")
        return "wait", f"ai GPU: {lock or f'RAM {available:.1f} GB < 12'}; Mac {state}: {why} (book computeboard gpu queued; recheck in 10 min)"
    if kind == "gpu-infer":
        if not lock and available >= 12:
            return "ai-gpu", f"lock free, no training (take {GPU_LOCK})"
        state, why = _route_mac_state()
        if state == "free":
            return "mac", f"ai GPU busy ({lock or 'RAM low'}); Mac {why} (MPS, mllock --wait)"
        return "cloud", f"ai GPU busy ({lock or 'RAM low'}); Mac {state}; use Modal/Kaggle per CLOUD-USAGE.md ($0 guard)"
    raise CliError(f"unknown kind {kind}")


def _cloud_path():
    return Path(os.environ.get("CQ_CLOUD_BOARD", "/tmp/compute-queue-cloud-board.json"))


def _load_cloud_board():
    try:
        data = json.loads(_cloud_path().read_text())
    except FileNotFoundError:
        data = {"next_id": 1, "jobs": []}
    except (OSError, json.JSONDecodeError) as exc:
        raise CliError(f"cannot read local cloud board: {exc}") from exc
    data.setdefault("next_id", 1)
    data.setdefault("jobs", [])
    cutoff = time.time() - 86400
    data["jobs"] = [job for job in data["jobs"]
                    if job.get("status") in ("queued", "running") or job.get("ended", 0) > cutoff]
    return data


def _save_cloud_board(data):
    path = _cloud_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1))


def _cloud_job(data, job_id):
    return next((job for job in data["jobs"] if job.get("id") == job_id), None)


def _computeboard_parser():
    parser = argparse.ArgumentParser(prog="computeboard")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    book = sub.add_parser("book")
    book.add_argument("resource", choices=RESOURCES)
    book.add_argument("who")
    book.add_argument("--eta", required=True)
    book.add_argument("--note", default="")
    book.add_argument("--start", action="store_true")
    route_parser = sub.add_parser("route")
    route_parser.add_argument("kind", choices=("light", "cpu", "gpu-infer", "gpu-train"))
    route_parser.add_argument("--ram-gb", type=float, default=8)
    for action in ("start", "done", "cancel"):
        sub.add_parser(action).add_argument("id", type=int)
    eta = sub.add_parser("eta")
    eta.add_argument("id", type=int)
    eta.add_argument("eta")
    return parser


def _board_jobs(api):
    jobs = _items(api.request("GET", "/jobs"), "jobs")
    nodes = _items(api.request("GET", "/nodes"), "nodes")
    return jobs, nodes


def _legacy_resource(job):
    if job.get("node") == "mac" or job.get("node_pref") == "mac":
        return "mac"
    if job.get("gpu"):
        return "gpu"
    return None


def _print_status(api, cloud):
    jobs, nodes = _board_jobs(api)
    ai = next((node for node in nodes if node.get("name") == "ai"), {})
    stats = ai.get("stats") or {}
    gpu_busy = any(_legacy_resource(job) == "gpu" and
                   job.get("state") in {"running", "preempting", "cancelling"}
                   for job in jobs)
    lock_state = "busy" if stats.get("outside_gpu_busy") or gpu_busy else "free"
    print(f"ai GPU lock: {lock_state}")
    if "vram_used" in stats or "vram_used" in ai:
        print(f"ai VRAM used: {_vram_gb(ai):.1f} GB")
    print(f"ai MemAvailable: {_ram_gb(ai):.1f} GB (start new jobs at >=12 GB)")
    all_jobs = list(jobs) + [
        {"id": item.get("id"), "resource": "cloud", "who": item.get("who"),
         "note": item.get("note", ""), "eta": item.get("eta"),
         "status": item.get("status"), "booked": item.get("booked", 0),
         "started": item.get("started"), "ended": item.get("ended"), "_cloud": True}
        for item in cloud["jobs"]
    ]
    now = time.time()
    for resource in RESOURCES:
        if resource == "cloud":
            selected = [job for job in all_jobs if job.get("_cloud") and
                        job.get("status") in ("running", "queued")]
        else:
            selected = [job for job in all_jobs if not job.get("_cloud") and
                        _legacy_resource(job) == resource and
                        job.get("state") in ACTIVE_STATES]
        selected.sort(key=lambda job: (job.get("state", job.get("status")) != "running",
                                       job.get("created", job.get("booked", 0))))
        print(f"\n[{resource}]" + ("" if selected else " no bookings"))
        for job in selected:
            state = job.get("state", job.get("status", "?"))
            who = job.get("submitter", job.get("who", "?"))
            eta = job.get("hold_until", job.get("eta"))
            eta_text = format_eta(eta) if eta else "-"
            late = " LATE" if state == "running" and eta and float(eta) < now else ""
            note = job.get("note", "")
            print(f"  #{job.get('id')} {state:7} {who}  eta {eta_text}{late}  {note}")


def computeboard_main(argv=None):
    args = _computeboard_parser().parse_args(argv)
    if args.command == "route":
        try:
            lane, why = route(args.kind, args.ram_gb)
        except CliError as exc:
            print(f"computeboard: {exc}", file=sys.stderr)
            return 2
        print(f"{lane}\t{why}")
        return 0

    try:
        cloud = _load_cloud_board()
        if args.command == "book" and args.resource == "cloud":
            now = time.time()
            job = {"id": cloud["next_id"], "resource": "cloud", "who": args.who,
                   "note": args.note, "booked": now, "eta": parse_eta(args.eta),
                   "status": "running" if args.start else "queued",
                   "started": now if args.start else None}
            cloud["next_id"] += 1
            cloud["jobs"].append(job)
            _save_cloud_board(cloud)
            print(f"booked #{job['id']} cloud {job['status']} eta {format_eta(job['eta'])}")
            return 0

        api = Coordinator()
        if args.command == "status":
            _print_status(api, cloud)
            return 0
        if args.command == "book":
            eta = parse_eta(args.eta)
            payload = {
                "argv": [],
                "node_pref": "ai" if args.resource == "gpu" else "mac",
                "gpu": args.resource == "gpu",
                "ram_req": 0,
                "vram_req": 0,
                "priority": 0,
                "kind": "computeboard",
                "note": f"who={args.who}" + (f" {args.note}" if args.note else ""),
                "deps": [],
                "leaseable": False,
                "est_s": None,
                "cwd": os.getcwd(),
                "env": {},
                "hold": True,
                "hold_until": eta,
            }
            created = api.request("POST", "/jobs", payload)
            job_id = _id(created)
            state = created.get("state", "queued")
            print(f"booked #{job_id} {args.resource} {state} eta {format_eta(eta)}")
            return 0

        local = _cloud_job(cloud, args.id) if args.command in ("start", "done", "cancel", "eta") else None
        if local is not None:
            if args.command == "eta":
                local["eta"] = parse_eta(args.eta)
            elif args.command == "start":
                local.update(status="running", started=time.time())
            else:
                local.update(status="cancel" if args.command == "cancel" else "done",
                             ended=time.time())
            _save_cloud_board(cloud)
            print(f"#{args.id} -> {local['status']}")
            return 0

        if args.command == "done":
            api.request("POST", f"/jobs/{args.id}/finish", {})
            print(f"#{args.id} -> done")
        elif args.command == "cancel":
            api.request("POST", f"/jobs/{args.id}/cancel", {"reason": "computeboard cancel"})
            print(f"#{args.id} -> cancel")
        elif args.command == "eta":
            before = api.request("GET", f"/jobs/{args.id}")
            api.request("POST", f"/jobs/{args.id}/renew", {"hold_until": parse_eta(args.eta)})
            print(f"#{args.id} -> {before.get('state', '?')}")
        elif args.command == "start":
            api.request("POST", f"/jobs/{args.id}/priority",
                        {"priority": 0})
            after = api.request("GET", f"/jobs/{args.id}")
            print(f"#{args.id} -> {after.get('state', '?')}")
    except CliError as exc:
        print(f"computeboard: {exc}", file=sys.stderr)
        return 2
    return 0

