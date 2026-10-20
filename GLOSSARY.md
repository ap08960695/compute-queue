# Compute Queue

One queue that places work on the shared machines (for example a Linux GPU workstation, a Mac, and future nodes), so RAM and VRAM stay fully used without overloading any machine.

## Work

**Job**:
A request to run one piece of work, stating the resources it needs, the Node it wants (or any), and its priority.
_Avoid_: Booking, task, card

**Allocation**:
A Job that has been placed on a Node and currently holds that Node's RAM and VRAM.
_Avoid_: Booking, reservation, lease

**Profile**:
The measured resource use (unreclaimable RAM, VRAM, wall time) of earlier runs of the same kind of Job, used as that kind's default request.
_Avoid_: Estimate, sizing

**Dependency**:
A rule that a Job may start only after another named Job has finished successfully.
_Avoid_: Chain, sentinel

## Scheduling

**Preemption**:
Asking a running Allocation to stop at its next Lease boundary so a more urgent Job runs first, then resuming it.
_Avoid_: Kill, break

**Lease**:
A bounded slice of time an Allocation holds a GPU before it must checkpoint and give the GPU back.

**Backfill**:
Starting a smaller Job that fits now while a larger Job waits for resources it cannot yet get.

**Headroom**:
The RAM and VRAM a Node can still give, after its own system use and work outside the queue.

## People and roles

**Owner**:
The person who runs this cluster and has final say over every Job, Node and cancellation.

**Operator**:
An agent the Owner has granted rights to reorder, preempt and approve Nodes for all Jobs; it cannot cancel another submitter's Job without the Owner.
_Avoid_: Admin, manager

**Submitter**:
Whoever sent a Job (the Owner, a coding agent or a bot); it may reorder and cancel only its own Jobs.
_Avoid_: Owner (of a job), who

## Machines

**Node**:
A machine that runs Allocations through its Agent.
_Avoid_: Host, worker, box

**Coordinator**:
The single place that holds the queue and decides which Job gets which Node.
_Avoid_: Master, server, scheduler

**Agent**:
The program on each Node that pulls work from the Coordinator, runs it within limits, and reports back.
_Avoid_: Daemon, worker

**Outside load**:
Work on a Node that the queue does not run, such as other bots' workers or an always-on model server.
