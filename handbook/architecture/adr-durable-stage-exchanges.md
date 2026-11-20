# ADR: Durable Stage Exchanges for Intra-Query Fault Tolerance

- Status: Proposed (RFC under community review — [PR #76056](https://github.com/StarRocks/starrocks/pull/76056))
- Date: 2026-07-09
- Deciders: StarRocks PMC (pending)
- RFC: [Intra-Query Fault Tolerance via Durable Stage Exchanges](../plans/active/2026-07-08-durable-stage-exchanges-rfc-v2.md)

## Context

Long-running batch queries (multi-hour INSERT/CTAS, scheduled ETL) have an all-or-nothing failure model: one compute-node loss cancels the whole query, and whole-query retry (`max_query_retry_time`) discards all partial work and is abandoned once any rows have streamed to the client. Batch clusters increasingly run on spot/preemptible capacity where node loss is routine.

Two properties of the streaming exchange layer constrain any recovery design:

1. `SinkBuffer` frees chunks once acknowledged — a fragment cannot re-send its output without re-running.
2. `DataStreamRecvr` fixes its sender set at creation and has no deduplication — receivers cannot accept replacement senders mid-stream.

Consequently, metadata-only lineage recovery of an interior fragment cascades both upstream (outputs freed) and downstream (consumer state cannot roll back), degenerating to whole-query restart. Any sub-query recovery scheme must make *something* durable.

## Decision

Adopt an **opt-in staged execution mode for shared-data clusters** in which exchange output at recovery boundaries is materialized to shared object storage ("durable stage exchanges"), making every stage a pure function of durable inputs so that only lost fragment instances are replayed on node failure.

Constituent decisions:

1. **Durability target = stage outputs, never operator state.** Batch inputs are bounded and replayable, so outputs can be materialized instead of checkpointing GB–TB operator state (hash tables, sort runs).
2. **Recovery boundary = every exchange edge (initial scope).** Uniform correctness argument, no per-plan case analysis; cost-based boundary placement is deferred until measured per-edge costs exist.
3. **Recovery unit = fragment instance.** What the FE already schedules and tracks; intra-fragment pipelines are not independently reschedulable.
4. **Additive stage engine.** A second `ExecutionSchedule` implementation and a second exchange transport, selected per query; streaming code paths untouched, byte-identical behavior when disabled.
5. **Commit + attempt fencing at report ingress.** An attempt commits when its final report (carrying the output manifest) passes the fence check; late reports from presumed-dead nodes are rejected. At most one attempt per instance ever commits.
6. **Correctness independent of replay determinism.** No consumer observes output before commit, so replayed attempts may diverge byte-wise without affecting results.
7. **Exactly-once DML via transaction scoping.** A failed sink-stage attempt aborts the load transaction; only the sink stage reruns under a fresh transaction over committed spool. No per-file dedup protocol.
8. **Spool on the existing remote-spill block stack.** ≥64 MB blocks, randomized prefix sharding, jittered backoff; storage-volume seam (`fault_tolerant_spool_storage_volume`) for request-optimized tiers. Spool loss costs one bounded replay, never correctness, so it does not need data-lake durability.
9. **Coordinator fault tolerance out of scope.** Lineage/manifests/attempts live in FE memory; the query dies with its FE, exactly as today. Manifest persistence is a recorded extension point.

## Alternatives considered

| Alternative | Rejected because |
| --- | --- |
| Checkpoint operator state (Flink model) | State is GB–TB at exactly the moment it matters; every stateful operator needs checkpoint/restore surgery; built for unbounded streams that cannot be re-run. |
| Fragment replication | Doubles compute always to save it rarely; nondeterminism makes replicas diverge; still needs consumer-side dedup. |
| Metadata-only lineage replay | Upstream outputs already freed and consumer state cannot roll back — cascades to whole-query restart. |
| Write-ahead lineage (Quokka) | Requires engine-wide replay determinism incompatible with morsel stealing and timing-dependent runtime filters; cannot be additive (dedup must enter the live exchange); recovery cost grows with query progress. |

## Consequences

**Positive**

- Recovery cost becomes detection lag plus one cold-cache stage replay, instead of the full query's runtime; committed work is never redone.
- Suitable for spot/preemptible capacity; per-warehouse activation isolates the cost to batch workloads.
- Barriers cap concurrent resource footprint (only ready stages run), and committed manifests carry exact per-partition sizes — enabling adaptive per-stage parallelism later.
- Hardening of the shared spill block stack (block reopen, large-block buffering, lifecycle) benefits spill directly.

**Negative**

- Expected 1.5–3× wall-clock overhead for a first implementation (spool I/O plus loss of pipeline overlap); to be measured, not assumed acceptable.
- A second scheduling mode to maintain and test; a new commit/fencing protocol; spool GC surface (TTL sweep, orphan reaping).
- Global runtime filters are disabled across recovery boundaries in early phases (planner-warned), a real performance cliff until merge dedup lands.
- Spool storage cost and object-store request-rate exposure, mitigated but not eliminated by block sizing and prefix sharding.

**Neutral**

- FE failure semantics unchanged: IQFT never makes it worse, does not make it better.
- With the feature disabled, behavior is byte-identical to today — the default posture for all interactive workloads.

## Follow-ups (deferred, recorded in the RFC)

- Cost-based recovery-boundary placement (restores streaming overlap on non-boundary edges).
- Additional recovery primitives (bounded chunk-level replay, small-state checkpoints) behind the same invariants.
- Manifest persistence for FE-failover adoption; result-set retry after partial streaming; proactive spot-drain.
