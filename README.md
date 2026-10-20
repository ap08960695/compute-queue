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
