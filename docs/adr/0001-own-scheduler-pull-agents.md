# Own scheduler with pull agents, not Nomad or Slurm

We write our own small Coordinator plus per-Node Agents instead of adopting Nomad or Slurm. Slurm does not run usefully on macOS. Nomad cannot see AMD VRAM or Mac unified memory well, and the hard part for us is the host-safety rules learned from real freezes (one GPU job per card, 19.5 GB VRAM cap, 12 GiB RAM start floor, never touch always-on services, stop on kernel GPU faults), which a generic scheduler would not enforce.

Agents pull from the Coordinator over LAN HTTP with a per-Node token and open no listening port. If the Coordinator is down, Agents finish running work, keep results locally, and start nothing new, so no work is lost and no Node is double-allocated.

## Consequences

- Revisit Nomad if the cluster grows past about five Nodes.
- Other bots' worker processes stay outside the queue for now; they are measured as Outside load, not scheduled.
