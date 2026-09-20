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

## Keeping the script honest

A Byteman rule that cannot be installed does not announce itself. Byteman skips it, and
`ADMIN ENABLE FAILPOINT` still reports success, because the frontend tracks the point by name and
never checks that anything is listening. The rule is enabled, never fires, and whatever test relies
on it passes while testing nothing. A rename during an ordinary refactor is enough to cause it.

`FailPointBtmRuleTest` (FE unit tests) is the guard, in two layers:

- **Reflection over `CLASS` and `METHOD`** — the class must load and must declare a method of that
  name; when the rule writes a parameter list, some overload must match it exactly. Needs nothing
  but the JDK, so it runs on every rule and its failure message names the rule and the drift.
- **Byteman's own rule checker** (`RuleCheck`, the engine behind `bmcheck`) — loads the target's
  bytecode, runs the real transform, then type checks and compiles the rule. This is the same work
  the agent does at startup, so it covers what reflection cannot see: a `HELPER` that does not
  declare the method `IF` calls, a `DO` action that does not type check against the method it was
  injected into (`DO return;` where a value is expected), and a location specifier naming an
  injection point that is not there (`AT INVOKE foo` when nothing in the body calls `foo`).

Rules over third-party classes are checked but not asserted on: they are not on this repo's compile
path, and a red build from a classpath difference would only teach people to ignore the test. The
practical consequence for rule authors is the one in
[Iceberg connector failpoints](#iceberg-connector-failpoints): **prefer naming code in this repo.**
A rule over a library class gets neither layer's protection.

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

The two `after_write_*` hooks are the orphan-file windows: the file is durable and unreferenced.
They differ in what an armed `ENABLE` leaves behind, and the difference matters if you are counting
orphan files.

- **`tablet_merge_after_write_dcg_cols` does not.** Its caller records the rebuilt path only *after*
  the rebuild returns successfully, so an injected error returns before the caller learns the
  filename and the `.cols` file is left for ordinary orphan-file vacuum.
- **`tablet_merge_after_write_delvec` does not either.** Nothing arms a cleanup guard over the merged
  delvec file, so an injected error leaves it for vacuum as well.

Do not expect a whole-tablet garbage-file check to read zero straight after an armed `ENABLE`: an
error at either hook leaves its newly written file for ordinary orphan-file vacuum.

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

## Iceberg connector failpoints

Nothing injectable existed on the Iceberg path before. Every backend point whose name sits in
Iceberg-adjacent code was on the parquet **write** side, and no frontend rule touched the
connector at all, so the failure families that dominate Iceberg bug reports -- commit conflicts,
caches handing out paths an external engine already expired, partition-spec evolution racing
with DML -- could only be reproduced by running enough concurrency to get lucky. Measured on one
of them: a 2-way concurrent repro never hit, 6-way hit 3 times in 10.

### Backend hooks (`ENABLE_FAULT_INJECTION=ON` only)

| Point | Where it fires |
|---|---|
| `iceberg_delete_file_read_failed` | position-delete file is open, not one row applied to the deletion bitmap yet |
| `iceberg_delete_file_read_slow` | same site, sleeps 2s — widens the window where a data file is readable but its deletes are not applied |
| `iceberg_deletion_vector_read_failed` | v3 deletion-vector blob, after every descriptor check, before the Puffin read |
| `hive_scanner_open_file_failed` | data file's `FileSystem` resolved, scanner has read nothing |

`hive_scanner_open_file_failed` sits in `HiveDataSource`, which hive, iceberg, hudi and delta
scans all open their data files through — it is not Iceberg-only. The three delete-path points
are Iceberg-only.

The correctness question these ask is the same one in every case: **a delete file or data file
that cannot be read must fail the scan, never return the data file with its deletes silently
skipped.** A query that succeeds with an armed delete-path point has returned rows that should
have been deleted.

Note the asymmetry with equality deletes: there is no backend point for them because there is no
backend merge path to put one on. `IcebergEqualityDeleteRewriteRule` splits the scan in the
frontend and the equality deletes come back as an anti-join, so only position deletes and
deletion vectors are merged in the backend.

### Frontend rules (in `conf/failpoint.btm`)

| Rule | Phase it stops at |
|---|---|
| `iceberg_commit_before_metadata_swap` | `<op>.commit()` has staged the snapshot into the transaction, `commitTransaction()` has not run — the window a competing commit lands in |
| `iceberg_scan_files_planned` | file list resolved and about to go to the backends, no backend has opened anything — **only under `set enable_connector_incremental_scan_ranges = false`** |
| `iceberg_scan_source_built` | the default scan path: the lazy split source is built, not one `FileScanTask` produced yet |
| `iceberg_cache_refresh_before_swap` | cached table known stale (metadata file location differs), swap to the new one not done |
| `connector_alter_clauses_applied` | every ALTER clause applied to the transaction, transaction not committed — for Iceberg, a new `PartitionSpec` staged but not published |
| `iceberg_alter_published_before_refresh` | the same ALTER one step later: the new metadata is committed and the frontend cache has not been refreshed to it yet |
| `hms_client_pool_exhausted` | acquiring a metastore client — throws as if the pool were exhausted / the connect were refused — **Hive connector only, not Iceberg** |
| `hms_client_acquire_slow` | same site, sleeps 5s — the metastore is reachable but slow to hand out a connection — **Hive connector only, not Iceberg** |

Two deliberate choices in that table:

- `iceberg_commit_before_metadata_swap` sits on `IcebergMetadata.publishStagedTransaction`, a
  one-line method that exists only to name this window. Its three callers all do
  `<op>.commit(); publishStagedTransaction(tx);` inside the lambda they hand to
  `commitWithCleanup`, so the staging and the publication are two adjacent statements with
  nothing between them to arm. The rule originally sat on `commitWithCleanup`'s own entry, which
  runs *before* `commitAction.run()` and therefore before anything is staged — it documented one
  race and stopped inside another.
  The two frames that would be tighter are both unusable: iceberg's
  `Transaction.commitTransaction` is library code, and the statements' enclosing
  `lambda$…$N` is a synthetic name that shifts with any edit to the file.
  **A rule naming a class that does not resolve, or a signature that drifted with the library
  version, is silently dead** — `ADMIN ENABLE FAILPOINT` reports success for a rule that will
  never fire, so the test it guards passes while testing nothing. Only rules over named code in
  this repo can be kept honest by a compile, and `FailPointBtmRuleTest` compares this rule's
  full signature.
- `connector_alter_clauses_applied` and `iceberg_alter_published_before_refresh` are the two
  sides of one ALTER: the first stops before `transaction.commitTransaction()`, the second at
  the exit of `IcebergAlterTableExecutor.applyClauses` after it returned. Partition-spec
  evolution racing with DML is the family they exist for, and it is a millisecond window with
  neither of them armed. `iceberg_alter_published_before_refresh` does not fire when the ALTER
  threw: `AT EXIT` triggers on return, not on athrow, and an ALTER that never committed has
  nothing published to go stale against.
- `connector_alter_clauses_applied` is on the shared `ConnectorAlterTableExecutor`, so it fires
  for any connector ALTER, not only Iceberg. There is no Iceberg-only method at this phase; the
  name says so rather than implying otherwise.

### Where an Iceberg pause parks

**`iceberg_cache_refresh_before_swap` holds a lock. The others do not.** It fires inside
`synchronized (lock)` on the per-table monitor in `CachingIcebergCatalog.refreshTable`, so a
second refresh of *the same table* blocks for the pause. That is the point of the rule — that
monitor is exactly what a concurrent refresh has to contend with — but it means the pause is not
free: `REFRESH EXTERNAL TABLE` and the post-ALTER cache refresh for that table both queue behind
it. It is per-table, so other tables and the rest of the frontend are unaffected.

`iceberg_commit_before_metadata_swap`, `connector_alter_clauses_applied` and
`iceberg_alter_published_before_refresh` park the statement's own thread with no catalog lock
held — the ALTER executor runs *outside* the `synchronized (this)` block in
`IcebergMetadata.alterTable`, which only covers the cache refresh that follows the commit. That
is precisely why the third one is at the executor's exit rather than at the entry of the refresh
it precedes: the same window, but on the near side of the monitor.

`iceberg_scan_files_planned` and `iceberg_scan_source_built` both park a query-planning thread.
Arm either with a probability rather than permanently: planning threads are a shared pool, and
pausing every scan at once starves it.

**Which of the two scan rules fires depends on a session variable, and the default is not the one
you might assume.** `enable_connector_incremental_scan_ranges` defaults to **true**, so
`IcebergScanNode.setupScanRangeLocations` calls `IcebergMetadata.getRemoteFilesAsync` — a separate
implementation that does *not* delegate to the synchronous `getRemoteFiles`. So:

- default configuration → `iceberg_scan_source_built` is the one that fires.
- `set enable_connector_incremental_scan_ranges = false` → `iceberg_scan_files_planned` fires.

They are not the same window reached two ways. The async path is lazy: `buildRemoteInfoSource`
wraps a `CloseableIterator<FileScanTask>` and produces files one at a time as the backends consume
them, so "the whole list is resolved" is a state that never exists there. `iceberg_scan_source_built`
means the source is built and nothing has been produced yet — enough to race an expire or rewrite
against planning, but not the stronger guarantee `iceberg_scan_files_planned` gives.

Note what kind of mistake this was: both rules name a real method with a real signature, so Byteman
installs them and `FailPointBtmRuleTest` passes on them. **Neither the signature check nor Byteman's
rule checker can tell you that nothing calls the method you hooked.** That gap closes only by arming
the point and observing a hit.

The two `hms_client_*` rules sit on `HiveMetaClient.getClient`, and they are the most dangerous
rules in this file. **Read this before arming either of them.**

`getClient`'s first statement is `BlockingCallValidator.validateNotUnderLock("hive-metastore", ...)`.
That is a **detector, not a guarantee**. It reports when the calling thread holds an FE metadata
lock; it never prevents the call, and under `Mode.OFF` it does not even report. Its presence is
evidence that this path *can* be reached under a lock — that is why someone mounted a guard here.
`LockInvariantViolations` says so outright: the lock-target rule "has a known-empty violation set
and is enforced", while **the blocking-call rule "is still *collecting* one"**. Sites that call HMS
under a metadata lock are known to exist and are not all fixed.

So the cost of a pause here is not "one parked thread":

> An FE metadata lock must not be held across a request to an external system: the lock's hold time
> becomes that system's round-trip time, and every waiter pays it — transaction publish takes the
> table lock with a **1000ms `tryLock`**, so one slow call inside a critical section **fails a
> load**.
>
> — `BlockingCallValidator`

`hms_client_acquire_slow` sleeps 5s. If it fires on a path that holds a metadata lock, it holds
that lock for 5s and every load whose publish waits on it fails, five times over the tryLock
budget. `hms_client_pool_exhausted` is the same exposure: it throws from inside the critical
section rather than returning to it.

**Before arming either on anything you care about**, check whether this path is currently reached
under a lock: grep the FE log for `blocking_call_under_lock` with `transport=hive-metastore`. An
empty result for your workload is the evidence the old wording here assumed without checking.

One diagnostic wrinkle: both rules are `AT ENTRY`, which puts the pause **before** the validator
runs, so the `LOCK_INVARIANT_VIOLATION` line that would explain the stall appears 5s after the
stall starts, not at its beginning. When reading logs, the violation report is the *cause* of what
you saw five seconds earlier, not a consequence of it.

Within the Hive connector these rules do park on every path to the metastore (`getClient` is
private and every caller is inside `HiveMetaClient`, covering both the `callRPC` and
`getPartitionsByNames` families), so arm with a probability; at 100% the Hive catalog stops
answering entirely, which is `hms_client_pool_exhausted`'s job, not this one's.

These two also cover something the network layer cannot. Blocking `:9083` from outside produces
"cannot reach the metastore"; a pool that is exhausted or a connect the metastore itself refuses
happens **inside the frontend process** and looks different to the code above it. Both are real,
and only one of them is reachable with iptables.

### Known gap: Iceberg's own HMS connection acquisition

The `hms_client_*` rules do **not** reach Iceberg. `IcebergHiveCatalog` holds an
`org.apache.iceberg.hive.HiveCatalog` as its delegate and routes every metadata operation through
it, so table loads and commits go via iceberg's own client pool and never touch
`com.starrocks.connector.hive.HiveMetaClient`. Arming these two and then running an Iceberg query
injects nothing, however much the query talks to HMS.

This is left as a gap rather than papered over, and the reason is the one in the header of
`conf/failpoint.btm`: the only frame that would cover it is inside iceberg's `CachedClientPool`,
which is library code. A rule naming it would be silently dead the moment the signature drifts
with the iceberg version, and `ADMIN ENABLE FAILPOINT` would keep reporting success. The
alternative — hooking `IcebergHiveCatalog.loadTable` — is available and compile-checked, but it
injects "this metadata operation failed", not "acquiring a connection failed". Those are different
faults, and labelling one as the other is exactly the mismatch between a rule's documented window
and its actual one that the rest of this file exists to prevent.

To fault-inject Iceberg's metastore path today, work at the network layer (block `:9083`) and
accept that it reproduces "cannot reach the metastore" rather than "the pool is exhausted".
