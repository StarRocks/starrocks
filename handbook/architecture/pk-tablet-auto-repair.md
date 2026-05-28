# Auto-Repair of Corrupted Primary-Key Tablets

- Status: landed
- Scope: BE tablet init, FE replica state, cluster scheduler

## Purpose

Describe the design and end-to-end workflow for automatic recovery of
primary-key (PK) tablets whose local replica data is unusable (for example,
missing segment files or corrupted rowset metadata discovered during init).
The goal is to keep the cluster online and self-healing when a single BE
replica is corrupted, instead of stalling the tablet or crashing the BE.

This document describes **what** each actor does and **why**; it does not
reference specific code paths.

## Background: the problem

A PK tablet opens (initializes) when the BE starts, when a tablet is cloned
in, or when the tablet is first loaded from meta. Before this change, if
that init step hit a corruption (for example, a rowset referenced a segment
file that no longer existed on disk), the failure was surfaced as an error
from the load path. The tablet stayed in a half-loaded state, any write to
that tablet kept failing, and in some deployments the process would even
hard-fail. Operators had to manually identify the bad replica, drop it from
the FE metadata, and wait for the scheduler to clone a fresh copy.

The cluster already tracks a per-replica flag called the **error state** and
the BE already knew how to declare that state at runtime when a write
operation discovered corruption. What was missing:

1. A graceful path for **init-time** corruption so the BE does not stall or
   crash and so the FE can still see the tablet.
2. Treating error-state replicas as "abnormal" everywhere the FE picks
   replicas (not just on the query path).
3. A scheduler action that **drops** an error-state replica so a clean
   replica can be cloned in its place, subject to safety checks and rate
   limiting.

## High-level picture

```
    +------------------+              +------------------+
    |     BE node      |   reports    |        FE        |
    | (holds a PK      |  tablet      | (TabletChecker + |
    |  tablet replica) |  error       |  TabletScheduler)|
    +------------------+ -----------> +------------------+
            ^                                   |
            | clone task                        | "drop then clone"
            |                                   v
            |                           +-----------------+
            +---------------------------| repair decision |
                                        +-----------------+
```

An error-state replica is no longer hidden or terminal. It is a signal that
FE can act on with a controlled, rate-limited drop-then-clone flow.

## BE-side design change

### Tablet init states (before / after)

```
 before (init-time corruption)                after (init-time corruption)
 -----------------------------                ----------------------------

         init                                         init
          |                                            |
          v                                            v
    [corruption]                                 [corruption]
          |                                            |
          v                                            v
  + - - - - - - - -+                         + - - - - - - - - - - - +
  | error returned |                         | tablet enters ERROR   |
  | tablet stalls  |                         | state inside BE,      |
  | or BE may FATAL|                         | init reports OK so    |
  + - - - - - - - -+                         | the tablet is loaded  |
                                             | and reportable to FE  |
                                             + - - - - - - - - - - - +
```

The BE behavior change is narrowly scoped to PK tablets and to the specific
**corruption** flavor of init failure. Any other init failure (permission
error, truly missing meta, etc.) still bubbles up unchanged. Writes to a
replica in the ERROR state are refused by the BE; the FE-side repair flow is
what eventually removes the replica from the tablet.

Key invariants:

- ERROR state is set **once** during init on a corruption signal.
- A replica in ERROR state is visible to the FE's report protocol.
- A replica in ERROR state refuses writes at the BE boundary; reads that
  would need its data fall back to healthy replicas via the FE query path.

### What the BE reports

The existing heartbeat / tablet-report channel now carries the ERROR flag
for a PK replica that entered ERROR during init. FE already knew how to
mark the corresponding `Replica` metadata as "in error state"; no wire
format change was needed.

## FE-side design change

The FE change has three parts:

1. **Classification** - treat error-state replicas as abnormal everywhere
   they matter.
2. **Scheduling** - add an explicit "drop the error-state replica" action
   to the repair scheduler, for both the general and the colocate paths.
3. **Safety** - do not drop if it would destroy the last usable copy, and
   rate-limit drops cluster-wide.

### Classification changes

Before this change, several FE helpers that pick replicas only filtered out
the pre-existing `isBad()` flag. The query path had already been extended
to also filter out error-state replicas, but the write-path, the colocate
health checker, and the empty-tablet-recovery predicate still treated an
error-state replica as "healthy enough." That asymmetry caused writes to
be routed to replicas the BE would then reject.

After the change, the following paths all treat `isErrorState` identically
to `isBad`:

```
   scope                                   before         after
   ----------------------------------      ---------      --------
   query replica selection                 excluded       excluded
   write / load replica selection          INCLUDED       excluded
   generic replica health check            abnormal       abnormal
   colocate replica health check           healthy        abnormal
   "data is totally lost" predicate        not lost       lost
```

The net effect: once BE marks a replica as error-state, FE stops routing
work to it, the checker flags the tablet as needing repair, and the
scheduler picks it up.

### Scheduling changes

The tablet scheduler's redundant-replica handler already had a checklist of
reasons to drop a replica (bad, backend down, wrong location, stale
version, etc.). A new reason is added near the top of that list:

```
  handleRedundantReplica (decision order):

     1. backend of replica was dropped            (existing)
     2. replica is marked BAD                     (existing)
     3. replica is in ERROR state                 (new)
     4. backend of replica is unavailable         (existing)
     5. replica is CLONE or DECOMMISSION          (existing)
     6. location mismatch                         (existing)
     7. failed / lower version                    (existing)
     ...
```

A symmetric change is made on the **colocate** redundant-replica path so
colocate tables are not a second-class citizen for this feature.

### Safety guard

Dropping the only usable copy of a tablet converts a recoverable situation
into data loss. Therefore the scheduler enforces:

```
  drop error-state replica
    requires: at least one OTHER replica is
              - not bad
              - not in error state
              - on an alive backend
              - in a state that can serve loads
```

If that condition is not met, the drop is skipped for this round and the
error-state replica is **preserved** so the operator or a follow-up round
still has something to clone from (or to manually repair).

### Rate limiting

Error-state deletions pass through a cluster-local token bucket
(Guava `RateLimiter`) so a cluster-wide incident cannot trigger a rapid
cascade of drops. When a token is not available, the drop is skipped and
the scheduler moves on to other repair work this round; the replica stays
error-state and is retried on a future round.

The rate is a mutable config. The scheduler re-syncs the bucket's rate at
the start of every check loop, so operators can tune the rate at runtime
without restarting FE.

## End-to-end workflow

### Sequence (happy path: single corrupted replica, 3-replica tablet)

```
  BE (holder)    BE (other)    FE (report)    FE (checker)    FE (scheduler)
      |               |             |               |                |
      | init finds    |             |               |                |
      | corruption    |             |               |                |
      |-- set ERROR ->|             |               |                |
      |     state     |             |               |                |
      |               |             |               |                |
      |---- tablet report with ERROR flag --------->|                |
      |                             |-- mark replica isErrorState    |
      |                             |               |                |
      |                             |               |-- classify as  |
      |                             |               |   abnormal ->  |
      |                             |               |   needs repair |
      |                             |               |-------- enqueue|
      |                             |               |                |
      |                             |               |     handleRedundantReplica:
      |                             |               |      * other healthy replicas? yes
      |                             |               |      * rate limiter token? yes
      |                             |               |      * DROP the error-state replica
      |                             |               |                |
      |<--- delete replica task ------------------------------------ |
      | drop data                   |               |                |
      |                             |               |                |
      |                             |               |     next round: checker
      |                             |               |     sees REPLICA_MISSING,
      |                             |               |     scheduler issues clone
      |                             |               |                |
      |                  <----- clone task from healthy replica -----|
      |                  | copy data                |                |
      |                  |                          |                |
      v                  v                          v                v
  (empty /        (source for         (replica count       (healthy state)
   recycled)        clone)              restored)
```

### Replica state transitions (FE view)

```
                 +-----------+     BE reports error      +----------------+
        init --> |  NORMAL   |-------------------------->|  ERROR_STATE   |
                 +-----------+                           +----------------+
                     |   ^                                      |
                     |   | clone completes                      |
                     |   |                                      | scheduler drops
                     |   +--------- [REPLICA_MISSING ---+       | (safety guard +
                     |              then re-created]    |       |  rate limited)
                     |                                  |       v
                     |                                  |  +-----------+
                     +----- other pre-existing paths ---+  | (removed) |
                        (bad, decommission, location, ...)  +-----------+
```

Key points:

- ERROR_STATE is not a dead end: it is a transient state that funnels into
  the existing drop-then-clone repair pipeline.
- A single tablet can cycle through ERROR_STATE multiple times (e.g., on
  repeated disk faults); each cycle is rate-limited.

### Scheduler decision flow (drop-then-clone, simplified)

```
             tablet flagged unhealthy by checker
                          |
                          v
              +-----------------------+
              | handleRedundantReplica|
              +-----------------------+
                          |
                    for each reason:
                   backend dropped? bad? ERROR STATE? ...
                          |
                  matches "ERROR STATE"?
                          |
                          v
                +-------------------+
                | safety guard:     |
                | is there ANOTHER  |
                | healthy replica?  |
                +-------------------+
                   |              |
                 yes             no
                   |              |
                   v              v
          +----------------+   +--------+
          | rate limiter   |   | SKIP,  |
          | token avail?   |   | keep   |
          +----------------+   | replica|
            |         |        +--------+
          yes         no
            |          \--> SKIP this round, retry later
            v
     DROP error-state replica
            |
            v
     ...next round: REPLICA_MISSING -> clone task to fresh BE
```

## Configuration and observability

### Operator knobs

- `tablet_sched_delete_error_state_replica_permits_per_second`
  mutable. Number of error-state replica drops allowed per second across
  the whole scheduler. Changes take effect on the next scheduler round; no
  restart needed. Default is tuned for typical clusters; lower it during an
  incident to slow the repair pipeline, raise it to accelerate recovery.

### What operators should see

- **BE logs**: a PK tablet entering error state during init logs a clear
  "corruption -> ERROR state for FE-driven repair" line with the tablet id.
  The BE then stays up and serves other tablets normally.
- **FE logs**: replicas reported as error-state are logged when the
  scheduler decides to skip (throttled or safety-guard) or when it drops
  them. Both skip paths log the replica and tablet id so incidents are
  traceable across rounds.
- **SHOW TABLETS / replica listing**: the error-state flag is already
  surfaced on the replica row, so operators can spot an affected replica
  before (and during) the repair cycle.

## Non-goals / explicit limits

- **Not a general corruption detector.** Only the specific init-time
  corruption path on PK tablets has been re-classified. Other failure
  types still fail loudly.
- **Not a write-side fix.** A BE that discovers corruption during a write
  still raises an error to the writer; this change is about graceful load
  + FE-driven repair, not silent write-time recovery.
- **Not a substitute for meta repair.** If the meta itself is gone (for
  example, a missing tablet record), this flow does not help.
- **RF=1 tablets cannot be auto-repaired.** The safety guard keeps the
  error-state replica in place (rather than destroying the last copy),
  which matches the pre-existing behavior for bad replicas.

## Rollout and compatibility

- No protocol or metadata format change. The error-state flag already
  existed on FE and BE before this change.
- Safe to roll in mixed versions: a BE without the change continues to
  behave as before; the FE-side classification changes still work because
  they key off a flag that pre-dated this work.
- Safe to disable by setting the rate-limit config to a very small number;
  the feature is effectively throttled off without a restart.
