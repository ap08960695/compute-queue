# compute-queue

A small job queue for a handful of shared machines (Linux GPU workstations and Macs). It places each Job on a Node so RAM and VRAM stay busy without overloading any machine.

Pure Python 3.11+ standard library. No third-party packages.

- One **Coordinator** holds the queue in SQLite and serves a token-authenticated HTTP API.
- Each **Node** runs an **Agent** that pulls work, limits memory (systemd `MemoryMax` with no swap on Linux, a sampled footprint watchdog on macOS), and reports back. The Mac watchdog can overshoot between samples; it is not a kernel-enforced cap.
- Sizing uses measured **Profiles** (peak unreclaimable RAM, VRAM and wall time of earlier runs), not guesses.
- Queue features: priorities, dependencies, resource-aware backfill, lease-boundary preemption (exit code 6 = checkpointed, resume later), per-node GPU exclusivity and VRAM cap, kernel GPU-fault guard.
- Roles: Owner, Operator, Submitter, Node. Every mutating call is audited.

Read [GLOSSARY.md](GLOSSARY.md) for the vocabulary, [docs/SPEC.md](docs/SPEC.md) for the full contract, and [docs/adr/](docs/adr/) for the design decisions.

## Quick start (one machine, loopback)

```bash
# Coordinator
python3 -m cq.coordinator --state ~/.local/share/compute-queue --init-owner me > owner-token.txt
python3 -m cq.coordinator --state ~/.local/share/compute-queue --bind 127.0.0.1:8765 &

# CLI token
mkdir -p ~/.config/compute-queue && tail -1 owner-token.txt > ~/.config/compute-queue/token
chmod 600 ~/.config/compute-queue/token
export CQ_URL=http://127.0.0.1:8765

# Add this machine as a Node
CODE=$(bin/cq node invite box1 | tail -1)
install/install.sh http://127.0.0.1:8765 "$CODE" box1
bin/cq node approve box1

# Submit work
bin/cq submit --node box1 --ram 2G --est 10m -- python3 train.py
bin/cq ls
```

## Configuration

| Setting | Where |
|---|---|
| Coordinator address, state dir, board file | `cq.coordinator --bind/--state/--board` |
| CLI token and URL | `~/.config/compute-queue/token` (0600), `CQ_URL` or `~/.config/compute-queue/cli.json` |
| Separate CLI identities | `CQ_CONFIG_DIR=/path/to/profile`, containing its own `token` and `cli.json`; a missing token fails without falling back to another profile |
| Agent | `~/.config/compute-queue/agent.json` (0600), written by `install/install.sh` |
| GPU lock shared with non-queue GPU users | `install.sh --gpu-lock PATH` (default `/tmp/compute-queue-gpu.lock`); CLI route check: `CQ_GPU_LOCK` |
| Mac lane probe for `computeboard route` | `CQ_MAC_SSH=user@host` |

Bearer tokens travel over plain HTTP. Keep the Coordinator on a trusted LAN or put it behind TLS.

Use distinct Operator tokens for coding assistants and a Submitter token for Hermes.
The `who` text on a compatibility booking is a note; the authenticated token determines
ownership and permissions. Operators cannot cancel another identity's jobs. Keep the
Owner profile for explicitly authorized administrative actions, including node approval
and fault clearance. On the coordinator host,
`python3 -m cq.coordinator --add-token NAME operator` creates an Operator token;
redirect it to a private file rather than a shared log.

GPU lock paths are node configuration. The first path is the allocation's exclusive
lock; additional paths are probes for legacy workloads. A workload taking an inner
lock must use a different path from the first one to avoid locking itself out.
Route all cluster jobs through the coordinator; probes alone cannot serialize
independent legacy launchers.

## Tests

```bash
python3 -m unittest discover -s tests
```

### Per-job RAM admission

`cq submit --ram-floor 8G --ram-start-min 19G` adds optional RAM admission
constraints to an ordinary job. Both sizes use binary units (`G` = GiB).
The corresponding `POST /jobs` fields are `ram_floor` and `ram_start_min`,
in bytes, defaulting to zero. Both must be finite nonnegative numbers.
Existing SQLite queues are migrated additively with zero defaults.

`ram_floor` is the MemAvailable reserve required **after** accounting for the
job's effective RAM cap. The scheduler uses the maximum of the node floor,
the candidate's floor, and the floors of all active allocations (including holds,
preempting and cancelling jobs). It reserves pending growth and jobs planned in
the same heartbeat. Backfill preserves these floors; preemption only projects
released resources, and does not permit starts until allocations actually finish.

`ram_start_min` is an absolute prelaunch MemAvailable threshold, in addition to
the floor plus effective RAM requirement. Scheduling tests it against available
RAM after reservations, without subtracting the floor a second time. A start
spec includes `effective_ram_floor`, the combined floor for that allocation.
For jobs opting in with either nonzero constraint, the agent samples RAM again
before launching and checks both thresholds using that combined floor. Low RAM,
invalid constraints, or missing/invalid telemetry produce launch-failed rc 125
without starting the workload. Jobs with both constraints zero retain legacy
agent behavior.

These fields do not implement a stability timer. A controller requiring
MemAvailable >= 19 GiB continuously for 300 seconds must enforce that before
submission. Diagnostic admission still uses the exact queued job ID and keeps
the node disabled; its grant is consumed only once the RAM constraints fit.

### One-job diagnostic on a disabled node

An Owner may authorize one already-queued, exact-node job without opening the
node's general queue:

```sh
cq node diagnostic ai JOB_ID --reason "One approved regression; no retry"
```

The job must be attempt 0, executable, never previously started, and the node must
have no allocations. The scheduler atomically consumes the durable grant when it
starts that job. Other jobs, including CPU jobs, stay queued. The node stays
disabled after completion, failure, or refusal. Exit 6 and a lost diagnostic
allocation fail without requeueing. Cancelling a pending job revokes its grant.
Only an Owner can create or inspect grants (`GET /nodes/NAME/diagnostic`);
operators cannot use this route to enable a node.

On Linux, a newly observed kernel fault remains latched in the agent's
`KERNEL-FAULT.json` across restarts. It blocks subsequent GPU launches and stops
owned GPU allocations; it never stops an outside process. This file is evidence
for explicit recovery review, not an automatically cleared retry flag.
