# Paimon Global Index P1 regression and acceptance plan

Prepared on 2026-11-10 before independent Docker verification.

- PR: https://github.com/StarRocks/starrocks/pull/79483
- Reviewed baseline: `e33101f319737c09d3318dcb403f1208c15069be`
- Scope: schema-unsafe SDK fallback and policy-rewritten internal index results.
- Implementation owner: coding agent. Acceptance owner: independent Docker verification agent.
- Final code review, source/artifact identity checks, and submission owner: primary agent.

## Fix contracts

### P1-A: unsafe historical index encoding

Retain the current read schema when pinning a data snapshot. Before SDK index
planning, apply the historical scalar-key compatibility guard to indexes that
could be used for this query. An incompatible or unverifiable index must not be
used by either stage 1 or stage 2 fallback. Prefer disabling index evaluation for
the affected scan over returning potentially incomplete candidates. Do not rely
on the final SQL filter or FULL search mode to recover false negatives.

Reuse the existing metadata cache and historical-field analysis where possible.
Do not perform unnecessary schema-history I/O for tables without relevant
indexes. Retain safe-field, rename, nullability, and explicitly disabled-index
behavior. Preserve existing diagnostics; record an intelligible fallback reason.

### P1-B: policy changes to index-result rows

Continue checking the caller's privileges on the base and virtual tables.
Reject the optional internal index execution when row-access or column-masking
policies would rewrite the virtual table. Let the parent query use its ordinary
authorized SDK path. Do not bypass policies or infer complete candidates from
one surviving bitmap. Keep direct virtual-table access forbidden, including ROOT.

Use the existing policy-analysis boundary, and avoid duplicate policy lookups or
changes to unrelated table types. Keep ordinary base-table filtering/masking
semantics unchanged. A policy-free eligible query must still use stage 2.

## Environment and provenance

Record host architecture, Docker server version, container/image IDs, CPU/memory
limits, JDK/Maven/Paimon versions, source HEAD and dirty diff hash, and FE/BE
artifact identities. Use isolated container names, ports, catalogs and data paths.
Do not change or stop unrelated containers. Bound every potentially blocking
Docker/network/build command. Do not expose credentials in logs.

Use local Docker for the requested simulation. Prefer a real FE plus two BE
processes and Paimon data/index files reachable by all processes. A matching
baseline BE may be reused for FE-only fixes, but its commit/build identity and
Global Index capability must be verified. A successful query that silently
falls back is not proof that distributed execution works.

If a full cluster cannot be started, retain the exact blocker and run all
independent integration checks that remain possible. Label SDK-only probes,
in-process FE integration tests, mocked scans, and real FE/BE SQL E2E separately.
Never report a lower layer as a completed full-cluster E2E.

## Required regression matrix

| ID | Scenario | Required result and path evidence |
| --- | --- | --- |
| A01 | INT BTree, rows 1 and 2, schema-only INT to STRING, predicate id='1' | Stage 0/1/2 all return exactly '1'; stage 1 and stage 2 fallback do not use the unsafe index. |
| A02 | INT to BIGINT, values spanning a 32-bit boundary after evolution | Stages agree with index-disabled reference; no key-decoding exception or lost rows. |
| A03 | Same-schema BTree, selective predicate, multiple physical index ranges | Stage 2 is actually applied and equals stage 0; ordinary safe index eligibility is retained. |
| A04 | Rename, nullability-only change, and name reuse with a new field ID | Stable IDs remain correct; safe changes retain eligibility; reused names do not select an old field's index. |
| A05 | Missing schema history / metadata-read failure | No unsafe-index candidates are accepted; either a safe data scan or an explicit query failure, never silent partial results. |
| A06 | Unindexed appended rows and partial coverage | Complete results equal stage 0; unsupported distributed coverage safely falls back. |
| A07 | Explicit stage 0 and table-level global-index.enabled=false | No SDK or distributed index execution. |
| B01 | Virtual-table row policy that would remove some shard rows | Optional child fails before candidate consumption; parent returns the complete authorized base-table result through fallback. |
| B02 | Virtual-table masking of index_result, including a valid alternative serialized bitmap | Same fallback requirement; legal serialization cannot disguise tampering. |
| B03 | Policy-free virtual table | Eligible stage 2 execution still applies; no blanket disablement. |
| B04 | Base-table row filter or masking only | Original base-table policy remains enforced; no extra rows or unmasked values. |
| B05 | Base-only privilege, virtual-only privilege, both privileges, and direct virtual access | Base-only falls back; virtual-only cannot read base; both permit eligible execution; direct virtual access is denied. |
| C01 | Cancellation during optional index work | Cancellation propagates; no new fallback query begins after cancellation. |
| C02 | Empty matches, multiple shards, IN/range/NULL predicates | Correct complete results, including legitimate empty bitmaps. |

For A01 retain a failing baseline control. The independently reproduced baseline
returns `[]` with current schema plus SDK indexes, `[1]` with indexes disabled,
and throws a type exception through the old snapshot-schema copy path.

## Validation layers and gates

1. Focused FE regressions on actual production metadata and analyzer paths,
   including real Paimon BTree fixtures and injected access-controller policies.
2. Relevant existing metadata/cache, plan, authorization, service, cancellation,
   and caller-executor suites; Java Checkstyle and `git diff --check`.
3. Independent Docker simulation and, where the environment supports it, real
   SQL E2E. Capture both result equivalence and selected/fallback execution paths.
4. Primary-agent review of every production hunk, failure path, added test, and
   documentation change. Re-run affected checks after review-driven edits.

A test is PASS only when its actual result and required path evidence match.
FAIL, NOT RUN, and BLOCKED must remain explicit. A mocked or SDK-only result
does not satisfy a real distributed SQL case. Do not infer approval from CI
results on the baseline or from an artifact with an unverified source identity.

## Evidence retention

Keep this plan, reproducible test/fixture commands, final case-by-case acceptance
report, environment manifest, source diff/hash, full test logs, XML summaries,
SQL outputs and relevant EXPLAIN/profile/fallback evidence. Keep baseline and
fixed-run evidence separate. Store bulky generated evidence outside the Git
commit and link it from the final acceptance report. Preserve every acceptance
document after container cleanup; remove only this task's temporary services.

## Submission

Commit only the two fixes, their regression tests, relevant behavior docs, and
reusable acceptance material. Add one signed-off fixup commit to the existing
PR without rewriting its public history. Verify the PR remote HEAD immediately
before a normal push. Record the final commit and post-push verification.
