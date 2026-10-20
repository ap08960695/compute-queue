# compute-queue v1 specification (accepted 2026-10-20)

Terms follow `GLOSSARY.md`. Python 3.12 stdlib only. No third-party packages.

## Files and owners

| File | Purpose |
|---|---|
| `cq/model.py` | SQLite schema, data access, pure scheduler `plan_for_node()`, permissions |
| `cq/coordinator.py` | HTTP API (ThreadingHTTPServer), auth, audit, board writer, hold expiry |
| `cq/agent.py` | Node Agent: heartbeat loop, launch/monitor/stop Allocations, guards, spool, `--hw` |
| `cq/cli.py` | `cq` CLI (Owner/Operator/Submitter) |
| `bin/cq` | thin launcher for `cq/cli.py` |
| `bin/computeboard` | compatibility shim with the old computeboard CLI, backed by the Coordinator |
| `install/install.sh` | Node installer (Linux systemd user unit, macOS launchd agent) |
| `tests/test_*.py` | unittest suites, runnable with `python3 -m unittest discover -s tests` |

## Data model (SQLite, WAL, one file)

`nodes(name PK, os, status, ram_total, vram_total, vram_cap, ram_floor, gpu_locks JSON, last_seen, stats JSON, created)`
- `status`: `pending` (registered, not approved) | `active` | `faulted` (no new GPU work until Owner clears) | `disabled`.
- `stats` (latest heartbeat): `mem_available`, `vram_used`, `outside_gpu_busy` (bool), `allocs` (see heartbeat).
- Defaults: `ram_floor` 4 GiB; `vram_cap` 19.5e9 on Linux GPU nodes; Mac `vram_total` 0 (unified memory, RAM only).

`jobs(id INTEGER PK, submitter, kind, note, argv JSON, cwd, env JSON, node_pref, gpu BOOL, ram_req, vram_req, priority INT, position REAL, deps JSON, leaseable BOOL, est_s, urgent BOOL, hold BOOL, hold_until, state, node, attempt, rc, peaks JSON, created, started, ended, state_note)`
- `state` closed set: `queued`, `running`, `preempting`, `cancelling`, `done`, `failed`, `blocked`, `cancelled`.
- `ram_req`/`vram_req` in bytes; RAM means unreclaimable (anon+kernel+shmem on Linux, phys footprint on macOS).
- `hold` jobs have no argv: they only occupy a slot (used by the computeboard shim). They expire when `now > hold_until + 1800` (30 min grace, boundary not expired) unless renewed; expiry sets `done` with `state_note='hold expired'` and is audited.

`profiles(kind PK, runs JSON)` - last 5 finished runs: `{peak_ram, peak_vram, wall_s, at}`.

`tokens(hash PK, principal, role, node)` - sha256 of token; role `owner` | `operator` | `submitter` | `node`. Raw tokens are never stored.

`invites(code_hash PK, node_name, expires)` - one-time join codes created by Owner/Operator.

`audit(id, at, principal, action, job_id, detail JSON)`.

## Request sizing (Profile first)

`effective_ram(job) = 1.3 * max(profile peak_ram over runs)` if the kind has a Profile, else `1.3 * job.ram_req`. Same for VRAM (factor 1.0 + 0.5 GB margin). `est_s` defaults to the Profile max `wall_s`.

## Scheduler: `plan_for_node(db, node_name, now) -> {"start": [job...], "preempt": [job_id...], "blocked": [job_id...]}`

Pure function over DB state (no I/O besides the DB). Called on every heartbeat from that node.

1. Node must be `active`. Otherwise return empty.
2. Candidate queue: jobs in `queued`, ordered by `(urgent DESC, priority DESC, position ASC, id ASC)`, whose `node_pref` is this node or `any`.
3. Dependencies: if any dep is `failed`/`cancelled`/`blocked`, mark job `blocked` (state_note names the dep) and skip. If any dep is not `done`, skip (not blocked).
4. Headroom: `ram_free = mem_available - ram_floor - pending_growth`, where `pending_growth = sum over running allocs on node of max(0, effective_ram - current_ram)`. GPU free iff no running/preempting GPU alloc on node, `outside_gpu_busy` false, and node not `faulted`. VRAM free = `vram_cap - vram_used`.
5. Walk the queue. A job fits if `effective_ram <= ram_free` and (not gpu, or GPU free and `effective_vram <= vram_free`). Fit -> add to start, deduct resources, continue.
6. The first job that does not fit is the head. After the head, a job that fits may start if either (a) it is Backfill-eligible (`est_s <= 1800` or `leaseable`), or (b) it does not take what the head is waiting for: it is not a GPU job when the head is a GPU job, and after deducting it `ram_free` still covers the head's `effective_ram`. Unknown `est_s` is not eligible under (a).
7. Preemption: if the head is `urgent`, or the head has waited more than 10 min behind running Backfill allocations, request `preempt` of running leaseable allocations on this node with lower `(urgent, priority)` until the head would fit. Never preempt non-leaseable or hold allocations.
8. Hold jobs consume their declared slot like any other job.

## `cq/model.py` public functions (tests target these)

- `connect(path) -> sqlite3.Connection` (creates schema, WAL, `row_factory=sqlite3.Row`)
- `register_node(db, name, os, hw, status='pending', ram_floor=4*2**30, vram_cap=None, gpu_locks=())`
- `set_node_status(db, name, status)`
- `record_heartbeat(db, name, stats, now)`
- `create_job(db, submitter, argv=(), node_pref='any', gpu=False, ram_req=0, vram_req=0, priority=0, kind='', note='', deps=(), leaseable=False, est_s=None, urgent=False, hold=False, hold_until=None, cwd='', env=None, now=0) -> int`
- `effective(db, job) -> (ram_bytes, vram_bytes, est_s)`
- `plan_for_node(db, node_name, now) -> dict` (applies the starts/blocked transitions it returns)
- `report(db, job_id, rc, peaks, wall_s, now, fault=None, cancelled=False)`
- `allowed(role, principal, action, job) -> bool` (`action` in `submit, view, cancel, move, priority, urgent, preempt, node_admin, heartbeat`)
- `expire_holds(db, now) -> [job_id]`

## Permissions

| Action | owner | operator | submitter | node |
|---|---|---|---|---|
| submit, list, show | yes | yes | yes | no |
| cancel own job | yes | yes | yes | no |
| cancel other's job | yes | no (403 "needs owner") | no | no |
| move/priority own job | yes | yes | yes | no |
| move/priority other's job | yes | yes | no | no |
| set urgent, preempt | yes | yes | no | no |
| node invite/approve/disable/clear-fault | yes | yes | no | no |
| heartbeat, report | no | no | no | own node only |

Every mutating call writes one audit row with principal and `reason` (required for operator actions on others' jobs).

## HTTP API (JSON, `Authorization: Bearer <token>`)

- `POST /jobs` -> `{id}`; `GET /jobs?state=` ; `GET /jobs/<id>`
- `POST /jobs/<id>/cancel {reason}` ; `/move {before: id | position}` ; `/priority {priority, urgent?}` ; `/preempt {reason}` ; `/renew {hold_until}` (hold) ; `/finish` (hold -> done)
- `POST /nodes/invite {name}` -> `{code}` (valid 24 h) ; `POST /nodes/register {name, code, hw}` -> `{token}` (node becomes `pending`) ; `POST /nodes/<n>/approve|disable|clear-fault` ; `GET /nodes`
- `POST /agent/heartbeat {stats, allocs:[{job_id, state:"running", current_ram, current_vram}]}` -> `{start:[job specs], preempt:[ids], cancel:[ids]}`. Starting jobs move to `running` with `node` and `started` set inside the same transaction.
- `POST /agent/report {job_id, rc, peaks:{peak_ram, peak_vram}, wall_s, fault?}` -> rc 0 `done`; rc 6 back to `queued` at same position, `attempt+1`; other rc `failed`; cancelled-by-request `cancelled`. Writes Profile for rc 0 and rc 6. `fault` sets node `faulted`.
- Errors: 400 bad input, 401 bad token, 403 role, 404, 409 state conflict.

Coordinator binds to a configured address (default `127.0.0.1:8765`; LAN address set in config). State dir default `~/.local/share/compute-queue/`. After each mutation it rewrites the compatibility board file (`--board`, default `~/.local/share/compute-queue/compute-board.json`) (old schema `{next_id, jobs:[{id, resource, who, note, booked, eta, status, started, ended}]}`, under the same fcntl lock path the old tool used) for running/queued GPU and Mac allocations: `resource` = `gpu` for GPU jobs on `ai`, `mac` for jobs on `mac`; `who` = submitter; `id` = job id.

## Agent behaviour

- Config `~/.config/compute-queue/agent.json` (mode 0600): `{coordinator, token, node, os}`. Local spool `~/.local/share/compute-queue/agent/` holds `allocs.json` (running allocations, for reattach) and `outbox.jsonl` (unsent reports).
- Every 10 s: sample stats, send heartbeat, act on reply. If the Coordinator is unreachable: keep monitoring, queue reports in outbox, start nothing. Resend outbox first on reconnect.
- Linux launch: `systemd-run --user --unit cq-job-<id> --property=MemoryMax=<effective_ram> --property=MemorySwapMax=0 --property=KillMode=control-group --property=RemainAfterExit=yes --property=Type=exec --working-directory=<cwd>` with env `CQ_JOB_ID`, `CQ_SIGNAL_DIR` plus job env. GPU jobs on ai are wrapped in `flock -x <first gpu_lock>`. Never pass `--collect`. Read RAM from the unit cgroup `memory.stat` (anon+kernel+shmem). Finished = `ActiveState` inactive/failed or `SubState=exited`; rc from `ExecMainStatus`; then `systemctl --user stop` the unit.
- macOS launch: `subprocess.Popen(start_new_session=True)` under `caffeinate -i`; RAM from `footprint -p`; kill (SIGTERM, 20 s, SIGKILL on the process group) when footprint > effective_ram cap or `kern.memorystatus_vm_pressure_level >= 4`.
- Preempt: create `$CQ_SIGNAL_DIR/LEASE-END-REQUESTED`; never kill. Cancel: stop the unit / kill the process group.
- Guards (Linux GPU node): VRAM total > `vram_cap` -> stop the GPU allocation and report `fault: "vram"`; new kernel lines matching `amdgpu.*(page fault|reset|timeout|hang)|HSA_STATUS_ERROR|oom-kill` since agent start (`sudo -n journalctl -k -b`, skipped with a logged warning if sudo is unavailable) -> stop GPU allocations, report `fault: "kernel"`.
- The Agent stops only its own units/process groups. It never signals any other process (always-on services, other agents, other sessions).
- `agent.py --hw` prints JSON `{os, ram_total, cpu, gpus:[{name, vram_total}]}` (Linux: /proc/meminfo, /sys/class/drm/card*/device/mem_info_vram_total; macOS: sysctl hw.memsize, machdep.cpu.brand_string).

## CLI

`cq submit [--node N|any] [--gpu] [--ram 6G] [--vram 9G] [--priority P] [--kind K] [--note T] [--after ID]... [--leaseable] [--est 30m] [--cwd D] [--env K=V]... -- argv...`
`cq ls [--all]`, `cq show ID`, `cq cancel ID [--reason R]`, `cq move ID --before ID2`, `cq prio ID P [--urgent]`, `cq preempt ID --reason R`, `cq node ls|invite NAME|approve NAME|disable NAME|clear-fault NAME`, `cq profile ls`.
Token from `~/.config/compute-queue/token` (0600); coordinator URL from `~/.config/compute-queue/cli.json` or `CQ_URL`.

## computeboard shim

Same commands and output shape as the old tool: `status`, `book <gpu|mac|cloud> <who> --eta HH:MM|Nm [--note] [--start]`, `start`, `done ID`, `cancel ID`, `eta ID T`, `route ...`.
`book` creates a hold Job (`gpu` -> the Linux GPU node; `mac` -> the macOS node) with `hold_until` from eta; prints `booked #<id> <res> <status> eta HH:MM`. `done` -> `/finish`. `eta` -> `/renew`. `cloud` stays a local board entry (no Node). `route` keeps the old logic.

## Installer

`install.sh <coordinator_url> <invite_code> <node_name>`: check python3 >= 3.11, copy `cq/agent.py` to `~/.local/share/compute-queue/agent/`, run `agent.py --hw`, POST `/nodes/register`, write agent.json 0600, install and start the service (systemd user unit `cq-agent.service` with Restart=always, or launchd `com.compute-queue.agent` with KeepAlive). Print: "registered <name> as pending; ask the Owner to run: cq node approve <name>".

## v1.1 amendments (after review R01, 2026-10-20) - these override earlier sections

- **Reconciliation (B1).** Each heartbeat lists the Agent's live allocations. A job the Coordinator holds as `running`/`preempting`/`cancelling` on that node, started more than 60 s ago and absent from the list, goes back to `queued` (`attempt+1`, `state_note='lost on node'`, audited). A board write failure never fails the heartbeat or any other request; it is logged. Agent HTTP timeout is 35 s (above the 30 s SQLite busy timeout). One failing start never skips the remaining starts in a reply; a launch failure is reported with rc 125.
- **Mandatory memory cap (B2).** `effective_ram = max(1.3 * max(profile peak_ram), 1.3 * ram_req, 256 MiB)`; profile peaks of 0 are ignored. `POST /jobs` (non-hold) is rejected with 400 when `ram_req` is 0 and the kind has no Profile. The Agent refuses to launch any job whose cap is 0 or missing (rc 125); it never sets `MemoryMax=infinity`.
- **Holds never reach Agents (B3).** The scheduler marks a fitting hold `running` on the node but never includes it in the `start` list; Agents never see holds. `/finish` and expiry only change Coordinator state.
- **Node reports (S2).** A report's `cancelled: true` is honoured only when the job is `cancelling`; otherwise the rc decides. Operators may still invite and approve Nodes (Owner decision).
- **GPU lock (S3).** `agent.json` may carry `gpu_locks` (installer option `--gpu-lock PATH`, default `/tmp/compute-queue-gpu.lock` on Linux GPU nodes). The Agent wraps GPU jobs in `flock -x <first lock>` and reports `outside_gpu_busy` when a non-blocking probe of any listed lock fails and it holds no GPU allocation itself.
- **macOS stop (S1).** After SIGTERM to the process group, the Agent waits up to 20 s for the whole group to disappear (`os.killpg(pgid, 0)`), then SIGKILLs the group; the allocation is kept and reported only once the group is gone.
- **Submitter limits (N1, N3).** Submitters may set priority only in `[-10, 0]` on their own jobs; higher needs operator/owner. A submitter's hold `hold_until`/renew is capped at now + 12 h.
- **Files (N6, N7).** The state directory is chmod 0700 on every start; DB, `-wal`, `-shm` 0600. The board is written to a temp file in the same directory and renamed into place.
- **Agent outbox (N8).** Reports are dropped only on 400/404/409; 401/403/5xx/timeouts keep them queued. `systemctl stop` uses `--no-block` and finish is detected on later ticks (N10).
- **Shim (N2).** `computeboard book` never sends `submitter` (the token principal is the submitter; the legacy `who` goes into `note` as `who=<name>`). `start` does not set `urgent`.

## v1.2 amendments (after delta review R02) - override earlier sections

- **Lost cancelling job (NB1).** Reconciliation requeues only `running`/`preempting` jobs. A lost `cancelling` job becomes `cancelled` (`ended=now`, `state_note='cancelled; lost on node'`), audited.
- **Attempt-scoped allocations (S-new-1, N-new-2, N-new-3).** Start specs carry `attempt`. Unit name is `cq-job-<id>-a<attempt>` (macOS: same id in allocs). Heartbeat alloc entries and reports carry `attempt`. The Coordinator answers 409 to a report whose job is not assigned to this node or whose attempt differs; the Agent drops 409 reports (already in its drop set). Before launching, the Agent writes an intent entry `{job_id, attempt, unit, state:"launching"}` to `allocs.json`; on startup it adopts every `cq-job-*` unit (Linux) that systemd still lists and that matches an intent/alloc entry, and stops (`--no-block`) any `cq-job-*` unit with no entry.
- **GPU locks (N-new-1).** The Agent reads `gpu_locks` from `agent.json` only; any value in a start spec is ignored.
- **Board mode (S-new-2).** The board replacement keeps the previous file's mode, or 0644 when new. At cutover the legacy computeboard is replaced by the shim, so no legacy writer remains; until then the Coordinator must not run against the production board.
