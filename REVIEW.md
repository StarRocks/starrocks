# Severe-Risk Review Profile

This optional profile adds repository-specific depth to one ordinary code
review. Preserve the ordinary diff review and every finding it would normally
report. Use this profile only to deepen investigation of severe risks
reachable from the change. Do not run a separate pass, repeat generic checks,
or suppress an otherwise valid finding.

## Evidence Threshold

Spend the additional investigation on wrong results, data or metadata loss,
process crashes, concurrency failures, security bypasses, incompatible
upgrades, stuck jobs, or material resource regressions.

A profile-generated finding must connect:

1. the contract or invariant that must hold;
2. the changed path that violates it;
3. a reachable input, state transition, deployment state, or interleaving;
4. the resulting user or system impact.

Historical defects and this document generate hypotheses only. Validate every
candidate against the current diff, discard unreachable speculation, merge
manifestations of one root cause, and retain every independent defect that
clears the threshold.

## Severe-Risk Extension

For each changed invariant:

1. **Classify the promise.** A fix closes every reachable form of the failure; a
   refactor preserves behavior; persisted or protocol changes support upgrades.
2. **Trace it.** Follow only reachable callers, representations, nodes,
   versions, lifecycle transitions, retry, replay, recovery, and no-op paths.
3. **Challenge it.** Construct the smallest concrete input, sequence, scale,
   deployment state, or interleaving, then disprove or validate it with code,
   tests, old-state behavior, size arithmetic, failure injection, or measurement.

Do not sacrifice local diff correctness for broader reasoning, or stop locally
when the changed invariant crosses a module or lifecycle boundary.

## Failure Models

- **No-op lifecycle progress.** An unchanged, already-latest, deduplicated,
  skipped, or reused object may still require lease renewal, heartbeat, epoch
  advancement, ownership validation, checkpointing, or deadline refresh.
  Data-plane no-op does not imply control-plane no-op.
- **Partial coverage.** One subclass, encoding, nesting level, job type,
  execution mode, or storage mode changes while a sibling or base default does
  not.
- **Identity and ownership drift.** State uses a transient or physical identity
  where a durable logical identity is required, or the reverse. Check split,
  reshard, rollup, schema change, retry, replay, publish, and deletion.
- **Stage or batch drift.** Parse- or analysis-time state is treated as a runtime
  guarantee after forwarding, rewriting, null widening, or physical conversion;
  or per-item state violates a batch-wide consistency requirement.
- **Lifecycle and durability gaps.** Cancel, timeout, close, failure, shutdown,
  duplicate callbacks, retry, replay, and partial progress must converge once.
  Persist before publication or destruction, wait for async work before
  releasing state, and revalidate current ownership before deleting.
- **Mixed-version handoff.** For persisted state, RPC fields, identities,
  storage paths, or lookup keys, check old writer to new reader, new writer to
  old reader, retry on another FE/BE/CN version, restart from old state, and
  rollback after new state was written. Retain dual-read or dual-write behavior
  until incompatible actors cannot participate.
- **Bulk-path amplification.** A helper acceptable for one online mutation may
  become quadratic or repeatedly publish partial state inside image load,
  journal replay, recovery, batch deletion, or schema reconstruction. Look for
  rebuild, sort, clone, publication, or full-map scans inside such loops.
- **Resource-envelope mismatch.** Prove separate bounds for final output, peak
  scratch memory, cumulative work across rows/pages/partitions/replay entries,
  and aggregate work across concurrent drivers. Bounded output does not imply
  bounded temporary memory or total work.
- **Default activation.** A changed default or feature flag newly exposes every
  existing object and built-in caller. Check no-op and reuse paths, restart,
  persisted old state, rollback, and disabled-era lifecycle assumptions.

## Triggered Module Guidance

Apply a row only when the diff reaches its trigger.

| Trigger | Review focus |
|---|---|
| FE metadata, DDL, persistence, replay | Check edit-log/image symmetry, old-state defaults, convergence, publication after complete reconstruction, bulk amplification, `SHOW CREATE`, job/base defaults, and the correct index or schema source. |
| Parser, analyzer, optimizer, planner | Preserve immutability, quoting, traversal, and forwarded statements. Prove equivalence under NULL, empty input, outer joins, grouping sets, DISTINCT, correlation, ordering, nullable rewrites, and indirect columns. Preserve output IDs/properties and keep query-local state out of shared caches. |
| Coordinator, scheduler, load, transaction, background job | Model retry, callbacks, cancellation, failover, no-op progress, and partial work as one idempotent state machine. Carry semantic inputs through retry/replay and use durable identity. |
| Connector, catalog, format, filesystem, JNI | Preserve NULL, timezone, escaping, precision, and comparison semantics across every path, encoding, and nesting level. Close resources on every exit and cover retry on another node or version. |
| BE execution, expression, column, hot path | Cover constant, nullable, all-NULL, dictionary, empty-batch, scalar, and nested forms; trace logical to physical types; propagate every `Status`/`StatusOr`; bound output, scratch, cumulative, and concurrent resource use separately. |
| Local/lake storage, compaction, index, vacuum | Treat deletion as high risk: stale work cannot authorize deletion and split/shared descendants retain referenced files. Check crash consistency, old-rowset readability, index false negatives, publish order, mode parity, mixed-version retry, and rollback. |
| Protocol, configuration, feature flag, observability | Trace every producer, consumer, copier, forwarder, internal caller, and retry constructor; preserve absent-field and mixed-version behavior. Diagnostics must not alter execution or replace the original error. |

## Evidence and Reporting

- Parser, analyzer, optimizer: focused FE test plus a plan or result at the
  semantic boundary.
- Metadata, persistence, protocol: writer-reader-replay trace including old
  state or a mixed-version case.
- Execution, ownership, storage: focused BE/SQL test, sanitizer, or failure
  injection for lifetime and crash paths.
- External integration: success, remote failure, cancellation, and conversion.
- Performance: use measurements for empirical or constant-factor claims. Size
  arithmetic, asymptotic analysis, or a reachable unbounded allocation path is
  sufficient when it directly proves material impact.

A mixed-version finding must identify old behavior from the base revision,
release code, protocol default, or persisted representation; do not infer it
only from the new implementation. If execution is unavailable, state the code
evidence and unresolved condition rather than manufacturing certainty.

Profile-generated findings should normally warrant P0 or P1 through serious
correctness, availability, durability, compatibility, security, concurrency,
or material resource impact. Severity follows reachable impact, not a checklist
match. Ordinary findings retain their normal severity and output policy.

For each profile-generated finding, identify the smallest useful changed line
range, violated contract, concrete failure path and impact, and narrowest safe
remediation. Do not narrate passed profile checks, report a risk solely because
this document mentions it, or pad an empty review.
