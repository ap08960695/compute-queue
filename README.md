# compute-queue

A small job queue for a handful of shared machines (Linux GPU workstations and Macs). It places each Job on a Node so RAM and VRAM stay busy without overloading any machine.

Pure Python 3.11+ standard library. No third-party packages.

- One **Coordinator** holds the queue in SQLite and serves a token-authenticated HTTP API.
- Each **Node** runs an **Agent** that pulls work, runs every Job under a hard memory cap (systemd `MemoryMax` with no swap on Linux, footprint watch on macOS), and reports back.
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
| Agent | `~/.config/compute-queue/agent.json` (0600), written by `install/install.sh` |
| GPU lock shared with non-queue GPU users | `install.sh --gpu-lock PATH` (default `/tmp/compute-queue-gpu.lock`); CLI route check: `CQ_GPU_LOCK` |
| Mac lane probe for `computeboard route` | `CQ_MAC_SSH=user@host` |

Bearer tokens travel over plain HTTP. Keep the Coordinator on a trusted LAN or put it behind TLS.

## Tests

```bash
python3 -m unittest discover -s tests
```
