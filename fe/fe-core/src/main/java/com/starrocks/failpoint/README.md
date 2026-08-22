# What is failpoint?
Failpoint is a fault injection testing framework that can precisely inject faults at any location within a function, helping to test system fault tolerance and stability.  
Writing failpoints does not require modifying the source code of the system under test (SUT), making it suitable for both R&D engineers (RD) and quality assurance (QA).

# How to use failpoint?
## Defining a failpoint
Defining a failpoint is very simple. Here's an example:  

```text
RULE bdb_ha_get_leader_exception
CLASS com.starrocks.ha.BDBHA
METHOD getLeader()
HELPER com.starrocks.failpoint.FailPointHelper
IF shouldTrigger("bdb_ha_get_leader_exception")
DO throw new RuntimeException("failpoint triggered");
ENDRULE
```

This is a failpoint written in Byteman script that triggers an exception when calling `com.starrocks.ha.BDBHA.getLeader`.

- `RULE`: The name of the rule.
- `CLASS`: The class name where the fault is injected.
- `METHOD`: The method name where the fault is injected. You can also specify parameter types for overloaded methods, e.g., `getLeader(int)`.
    - Below `METHOD`, you can define the injection location, such as:
        - `AT ENTRY`: At the beginning of the method.
        - `AT EXIT`: At the end of the method.
        - If not specified, the default is the beginning of the method.
    - For more location specifiers, refer to:  
      https://downloads.jboss.org/byteman/latest/byteman-programmers-guide.html#location-specifiers
- `HELPER`: The helper class. All functions in this class can be used in the `IF` and `DO` blocks below.
- `IF`: The trigger condition. Here, `shouldTrigger` is fixed, with the parameter being the `RULE` name.
- `DO`: The fault action. In the example, it throws an exception. Other options include:
    - Returning a result directly: `DO return null`.
    - Executing a block of code: `DO sleep(1000)`.
    - Byteman supports powerful execution logic, allowing access to any variables in the context. For complex logic, refer to:  
      https://downloads.jboss.org/byteman/latest/byteman-programmers-guide.html#rule-bindings
- `ENDRULE`: Marks the end of the rule definition.

## Using failpoints
1. `conf/failpoint.btm` ships with the build and already contains the rules listed under
   [Range-distribution reshard failpoints](#range-distribution-reshard-failpoints); add the startup
   option `--failpoint` to load it. Put your own rules in that same file. A restart is required for a
   new rule: Byteman loads the script once, at agent init, so `ADMIN ENABLE FAILPOINT` can only arm a
   rule that is already in the file.

2. Use admin commands to trigger failpoints:  

```text
// Enable permanently
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND;

// Disable after 10 executions
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' WITH 10 TIMES ON FRONTEND;

// Trigger with 10% probability
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' WITH 0.1 PROBABILITY ON FRONTEND;

// Pause every thread that reaches the failpoint, until it is disabled
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' WITH PAUSE ON FRONTEND;

// Disable (also releases a pause)
ADMIN DISABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND;
```

All of these require the `OPERATE` system privilege.

## Pausing at a failpoint

`WITH PAUSE` blocks every thread that reaches the failpoint instead of injecting a fault. It exists
for the fault pattern that a fail-only failpoint cannot express:

> stop at phase X -> act externally (kill a node, switch the leader) -> release -> assert

```sql
ADMIN ENABLE FAILPOINT 'some_failpoint' WITH PAUSE ON BACKEND '10.0.0.2:9060';
-- ... poll SHOW FAILPOINTS until PausedThreads > 0, then do the external action ...
ADMIN DISABLE FAILPOINT 'some_failpoint' ON BACKEND '10.0.0.2:9060';
```

Points worth knowing:

- **A released pause never injects.** Once released, the trigger evaluates to false and the flow
  continues normally, so arming an existing fail-style failpoint `WITH PAUSE` turns it into a pure
  stop point. To pause *and* fail, arm a second failpoint downstream.
- **Any mode change releases**, not just `ADMIN DISABLE FAILPOINT`.
- **A forgotten disable self-heals.** On timeout the failpoint is **disarmed**, not merely stepped
  past, so later arrivals are not parked again; a `pause timed out` WARNING is logged. If the failpoint
  was re-armed while the pause was expiring, the new mode is kept and left alone. The timeout comes
  from `failpoint_pause_timeout_second` (FE config, default 300, mutable), snapshotted when the
  failpoint is armed and sent to every frontend and backend with the arming request, so all nodes
  share one value even if the config changes afterwards.
- **Observability.** `SHOW FAILPOINTS` reports `TriggerCount` (cumulative fires) and `PausedThreads`
  (threads parked right now) for backends. FE failpoints are not listed by `SHOW FAILPOINTS`; an FE
  pause logs `failpoint <name> paused, waiting for ADMIN DISABLE FAILPOINT` in `fe.log`, and the
  effect on a reshard job is visible in `information_schema.tablet_reshard_jobs`.
- **Mixed versions are safe but not useful.** A pause is sent as `DISABLE` plus a request-level pause
  flag, so a node that predates this feature simply disables the failpoint rather than arming it.
  Nothing is injected, but nothing pauses either, and such a node reports `DISABLE` rather than
  `PAUSE`. Always confirm a pause with `PausedThreads > 0`, which is the only signal that proves a
  thread actually parked.
- **A pause blocks the thread it parks, and that thread stays blocked.** On the backend the wait
  deliberately blocks the pthread rather than yielding a bthread: `shouldFail()` runs inside libfiu's
  `fiu_fail()`, which holds a thread-local recursion counter and a read lock across the callback, so a
  pause that migrated to another worker would corrupt both and silently disable every failpoint on the
  original worker. The trade-off is that a paused failpoint occupies its thread, so parking more brpc
  handlers than the worker pool has threads can delay `ADMIN DISABLE FAILPOINT` until the pause times
  out. Pause a handful of handlers, not all of them.
- On the frontend, `TabletReshardJobMgr` runs every reshard job on one daemon thread, so a pause
  inside a job also freezes the other reshard jobs on that frontend. A node shutdown while a thread is
  parked waits out that thread's pause timeout.

## Build requirement for BE failpoints

FE failpoints need `--failpoint` at FE startup. **BE/CN failpoints exist only in a backend compiled
with `ENABLE_FAULT_INJECTION=ON`** (`ENABLE_FAULT_INJECTION=ON ./build.sh --be`); the default build
has them compiled out and `ADMIN ENABLE FAILPOINT ... ON BACKEND` returns
`FailPoint is not supported, need re-compile BE with ENABLE_FAULT_INJECTION`.

**A fault-injection build is a test-only build.** The SQL statements require the `OPERATE` privilege,
but the backend's `update_fail_point_status` RPC has no authorization of its own, and this document
describes driving it directly over HTTP. So on a node built with `ENABLE_FAULT_INJECTION=ON`, anyone
who can reach the internal BE port can arm any failpoint, bypassing the SQL privilege check entirely
— and with the reshard hooks below that includes parking the publish thread pool. Deploy such builds
only to test clusters whose internal ports are network-isolated, never to production. (This is a
property of the failpoint framework as a whole, not of any individual hook.)

## Range-distribution reshard failpoints

A forced tablet split finishes in well under a second, while the fastest external fault lever — an FE
restart — takes about 17 seconds. So no externally injected fault can land inside a reshard's `RUNNING`
sub-phases; only `WITH PAUSE` on one of the hooks below can stop the job there.

### Backend hooks (`ENABLE_FAULT_INJECTION=ON` only)

Every hook returns an `InternalError` when armed `ENABLE` (the reshard publish task fails and the
frontend retries) and parks the thread when armed `WITH PAUSE`. The "reached when" column matters:
only three are unconditional, and each of those is unconditional **within its own path**, not on every
reshard.

| Failpoint | Phase it stops at | Reached when |
|---|---|---|
| `tablet_reshard_between_metadata_writes` | inside the loop that persists the new tablet metadatas, after one has been written | every reshard publish (split, merge, identical) |
| `tablet_merge_after_rssid_reassign` | merge phase 1 done: per-source rowset-id offsets and the merged range are computed, nothing projected yet | every merge |
| `tablet_reshard_after_identical_pk_flush` | identical reshard, right after the PK-index flush wrote its sstables and before any metadata references them | every identical reshard |
| `tablet_merge_before_delete_predicate_range` | a delete-predicate rowset has been copied into the merged metadata but not yet confined to its source tablet's range | a merge where some source rowset carries a delete predicate, i.e. a `DELETE` ran on a source tablet (DUP / AGG / UNIQUE; primary-key tables use delvecs instead) |
| `tablet_merge_after_write_delvec` | the merged delvec file is written, metadata not yet updated | primary-key table with delete/update history (the phase is skipped when there is no source delvec and no synthesized gap) |
| `tablet_merge_after_write_dcg_cols` | a rebuilt `.cols` segment is written, metadata not yet updated | two delta-column-group entries claim the **same** column id for the same segment, i.e. a partial-column update on both merge sources touching one column |
| `tablet_merge_after_write_sstable` | a rebuilt persistent-index sstable is written, metadata not yet updated | primary-key table with a cloud-native persistent index **and** a legacy-form shared sstable or a remap that disagrees with the natural offset — in practice a multi-generation split/merge history with compaction on only some of the old tablets |

The three `after_write_*` hooks are the orphan-file windows: the file is durable and unreferenced.
They differ in what an armed `ENABLE` leaves behind, and the difference matters if you are counting
orphan files.

- **`tablet_merge_after_write_sstable` cleans up its own output.** Both callers arm a cleanup guard
  *before* the call and cancel it only after the output metadata is built, so an injected error
  deletes the rebuilt sstable.
- **`tablet_merge_after_write_dcg_cols` does not.** Its caller records the rebuilt path only *after*
  the rebuild returns successfully, so an injected error returns before the caller learns the
  filename and the `.cols` file is left for ordinary orphan-file vacuum.
- **`tablet_merge_after_write_delvec` does not either.** Nothing arms a cleanup guard over the merged
  delvec file, so an injected error leaves it for vacuum as well.

Do not expect a whole-tablet garbage-file check to read zero straight after an armed `ENABLE`, even
for the sstable hook. A merge runs the `.cols` phase, then the delvec phase, then the sstable phase,
so a merge that wrote either of the first two and then failed at the sstable hook discards the
metadata referencing them and leaves those files for vacuum — the sstable itself is gone, but the
earlier outputs are not. An immediate zero is only expected from a fixture that reaches the sstable
phase without writing a `.cols` or delvec file first.

### Frontend rules (in `conf/failpoint.btm`)

| Rule | Phase it stops at |
|---|---|
| `tablet_reshard_job_run` | job entry — holds the job at whatever state it is in. The general-purpose amplifier for making an external fault land in flight |
| `split_before_metadata_switch` / `merge_before_metadata_switch` | split points computed and the transaction published, catalog **not** yet switched |
| `split_after_metadata_switch` / `merge_after_metadata_switch` | catalog switched and the `CLEANING` transition already journalled, job not yet `FINISHED` |
| `colocate_mid_align_table` | after the first table of a colocate group has been processed and before the next — partial orchestration, not a half-aligned tablet layout |

### Where a pause parks

The operational question is whether a pause wedges the cluster. It does not, but the blast radius is
worth knowing.

**Backend.** Each parked task occupies one thread of the `publish_version` pool. brpc workers are not
affected: the RPC handler waits on a bthread latch and yields its worker. The only StarRocks
serialization state a parked thread holds is that reshard's publish token, so a concurrent publish on
the same source tablet is told to retry rather than blocked — no data, index, or metacache mutex is
held. The parked thread does still hold libfiu's own read lock and thread-local recursion counter for
the duration of the pause, which is why the wait must never migrate threads, plus whatever buffers,
open readers, and cleanup guards its site had live.

**The bound to respect:** the publish pool has roughly as many threads as the node has cores, and it
serves *every* publish including ordinary loads. Park more reshard tasks than that and all publishing
on that node stalls until release or timeout. Pause a handful of tablets, not a whole table.
`ADMIN DISABLE FAILPOINT` still gets through regardless — `update_fail_point_status` is served inline
on a brpc worker with no thread-pool handoff.

**Frontend.** All six rules park the single `TabletReshardJobMgr` daemon thread, which runs the
colocate checker, the reshard-candidate drain, and the reshard jobs in sequence. So any one of them
freezes every reshard job *and* colocate convergence on that frontend, and only one frontend pause can
be in effect at a time. The daemon loop has no watchdog, so a park simply means missed ticks.

**One hard prohibition:** never place a rule inside `SplitTabletJob.addNewMaterializedIndexes` or its
merge peer. Those hold the table WRITE lock, so a pause there makes the table unavailable to queries
and DDL for the whole pause. The `*_before_metadata_switch` rules deliberately sit at the *call* to
that method, which is outside every lock.
