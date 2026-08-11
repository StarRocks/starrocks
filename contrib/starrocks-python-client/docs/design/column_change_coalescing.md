# Design: Coalescing Column Schema Changes for Alembic Migrations

**Status:** Implemented (initial cut)
**Area:** `starrocks.alembic` (Alembic dialect integration)
**Related code:** `starrocks/alembic/ops.py`, `toimpl.py`, `render.py`, `starrocks.py`, `starrocks/sql/ddl.py`, `starrocks/dialect.py`

---

## 1. Summary

Alembic autogenerate emits one `ALTER TABLE ... ADD/DROP COLUMN` statement per
column. StarRocks executes each such statement as an **asynchronous
schema-change job** and permits **only one in-flight job per table**. As a
result, a migration that adds/removes several columns from the same table fails
on the second statement, because the first job is still running.

This document describes:

1. The root cause and the constraints it imposes.
2. The solution we shipped: an autogenerate **rewriter** that coalesces a
   table's `ADD`/`DROP COLUMN` operations into a single `ALTER TABLE`
   statement, plus an **opt-in wait** that blocks until each schema-change job
   reaches a terminal state.
3. The alternative approaches considered and why they were rejected.
4. Limitations, risks, and future work.

---

## 2. Background & Problem Statement

### 2.1 Customer-reported symptom

> When multiple ADD/REMOVE columns exist in a single autogenerate, they should
> be executed as a single SQL call. Otherwise the first `ALTER` causes the
> remaining ones to fail due to an in-progress schema change, and the pipeline
> must be re-run repeatedly (30 min – hours per table) until all the changes
> eventually land.
>
> Brownie points: have Alembic monitor and wait for the schema-change status,
> returning only when it reaches a terminal state, so the migration pipeline is
> 100% in sync with the cluster at all times.

### 2.2 Why this happens — StarRocks semantics

StarRocks schema changes on columns are **online and asynchronous**:

- `ALTER TABLE t ADD COLUMN ...` returns to the client almost immediately,
  after *submitting* a schema-change job — not after the change is durable.
- A table may have **at most one running schema-change job** at a time.
  Submitting a second job while the first is running is rejected with an error
  along the lines of *"Table already has a running schema change job."*

StarRocks does, however, support **combining multiple column changes into one
statement**, which becomes a single job:

```sql
ALTER TABLE t ADD COLUMN a INT, DROP COLUMN c, ADD COLUMN b INT;
```

### 2.3 Why this happens — Alembic execution model

Each migration directive is executed independently. In
`alembic/ddl/impl.py`, `DefaultImpl.add_column()` and `drop_column()` each call
`self._exec(...)` immediately, so a generated `upgrade()` like:

```python
op.add_column('t', sa.Column('a', INTEGER()))
op.drop_column('t', 'c')
op.add_column('t', sa.Column('b', VARCHAR(50)))
```

produces three separate `ALTER TABLE` statements executed back-to-back:

```sql
ALTER TABLE t ADD COLUMN a INT;   -- job #1 submitted (async, returns)
ALTER TABLE t DROP COLUMN c;      -- ERROR: job #1 still running
ALTER TABLE t ADD COLUMN b INT;   -- never reached
```

There is no built-in Alembic mechanism that groups consecutive operations on
the same table into a single statement at execution time.

### 2.4 The two problems, stated precisely

- **P1 (correctness / usability):** Multiple column changes on one table in one
  revision must be emitted as one statement (one job) to avoid the
  in-progress-schema-change failure.
- **P2 (synchronization):** Even a single `ALTER TABLE` returns before the job
  finishes. To keep the migration pipeline in lock-step with the cluster (so a
  *subsequent* migration step, or a later property/distribution change on the
  same table, does not collide), the tool should be able to wait for a terminal
  job state.

---

## 3. Goals & Non-Goals

### Goals

- **G1:** Autogenerate produces migrations that apply cleanly when a table has
  several `ADD`/`DROP COLUMN` changes in one revision.
- **G2:** The generated migration is explicit and reviewable (a human can read
  the combined operation before applying it).
- **G3:** Optionally guarantee cluster synchronization (P2), off by default so
  existing behavior is unchanged.
- **G4:** Follow existing dialect conventions (custom ops registered like
  `alter_table_distribution`, wired through `env.py` like `render_item` /
  `include_object`).

### Non-Goals

- **NG1:** Coalescing `MODIFY COLUMN` (type/nullability changes) into the same
  statement. Deferred (see §7 and §9).
- **NG2:** Cross-table batching or reordering across revisions.
- **NG3:** Transactional/rollback semantics for partially applied migrations —
  StarRocks does not support transactional DDL and this design does not change
  that.

---

## 4. Chosen Design

Two independent pieces, corresponding to P1 and P2.

### 4.1 Part 1 — Coalescing via an autogenerate rewriter (solves P1)

The coalescing is done during **autogenerate**, using an Alembic
[`Rewriter`](https://alembic.sqlalchemy.org/en/latest/api/autogenerate.html#alembic.autogenerate.rewriter.Rewriter)
attached to `process_revision_directives`. Alembic already groups a table's
operations into a `ModifyTableOps` container; the rewriter folds the contiguous
`AddColumnOp` / `DropColumnOp` entries in that container into a single new
operation.

**New components:**

| Component | File | Role |
|---|---|---|
| `StarRocksAlterColumnsOp` | `alembic/ops.py` | Custom Alembic operation holding `adds: [Column]`, `drops: [Column]`, `table_name`, `schema`. Registered as `op.starrocks_alter_columns(...)`. |
| `combine_column_alters` | `alembic/ops.py` | `Rewriter` registered on `ModifyTableOps`; collapses add/drop ops into one `StarRocksAlterColumnsOp`. |
| `_render_starrocks_alter_columns` | `alembic/render.py` | Renders the op to Python (`op.starrocks_alter_columns(...)`) in the generated script. |
| `starrocks_alter_columns` (impl) | `alembic/toimpl.py` | Executes the op: binds add-columns to a table for spec rendering, then emits one `AlterTableColumns` DDL. |
| `AlterTableColumns` | `sql/ddl.py` | DDL element representing the combined statement. |
| `visit_alter_table_columns` | `dialect.py` | Compiles `AlterTableColumns` to `ALTER TABLE t <clause>, <clause>, ...`. |

**Data flow (autogenerate):**

```
alembic revision --autogenerate
        │
        ▼
Alembic diff → ModifyTableOps{ AddColumnOp, DropColumnOp, ... }   (per table)
        │
        ▼  process_revision_directives = combine_column_alters
combine_column_alters rewrites ModifyTableOps
        │   collect AddColumnOp.column  → adds
        │   collect DropColumnOp.to_column() → drops
        │   (leave MODIFY / other ops untouched, in place)
        ▼
ModifyTableOps{ StarRocksAlterColumnsOp(adds, drops), <other ops> }
        │
        ▼  _render_starrocks_alter_columns
op.starrocks_alter_columns('t', adds=[...], drops=[...])   # written to script
```

**Data flow (apply, `alembic upgrade`):**

```
op.starrocks_alter_columns(...)  →  toimpl.starrocks_alter_columns
        │   bind add columns to a lightweight Table (for column spec)
        ▼
AlterTableColumns → visit_alter_table_columns
        ▼
ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b VARCHAR(50), DROP COLUMN c   # one job
```

**User wiring (`env.py`):**

```python
from starrocks.alembic import (
    render_column_type,
    include_object_for_view_mv,
    combine_column_alters,          # new
)

context.configure(
    render_item=render_column_type,
    include_object=include_object_for_view_mv,
    process_revision_directives=combine_column_alters,   # new
)
```

**Why the rewriter approach (vs. execution-time):**

- The combined statement is materialized as an explicit, reviewable
  `op.starrocks_alter_columns(...)` call in the generated script (G2).
- It reuses Alembic's own per-table grouping (`ModifyTableOps`), so there is no
  need to invent a flush boundary at execution time (see §5.2).
- It is idiomatic — the dialect already asks users to wire `render_item` and
  `include_object`; this is one more callback of the same kind.

### 4.2 Part 2 — Opt-in wait for terminal state (solves P2)

`StarRocksImpl` overrides `_exec()`. After executing any column-altering
construct, if the user opted in, it polls `SHOW ALTER TABLE COLUMN` until the
latest job for that table reaches a terminal state.

- **Trigger set:** `AlterTableColumns` (our combined op) plus Alembic's own
  `AddColumn`, `DropColumn`, and `AlterColumn` base constructs. This means the
  wait also benefits users who did **not** enable the rewriter (e.g. plain
  `op.add_column`), and single-column `MODIFY` changes.
- **Terminal states:** `FINISHED` (success) and `CANCELLED` (raise). Any other
  state keeps polling. No matching job row ⇒ treated as done (covers
  metadata-only changes).
- **Gating:** No-op when `as_sql` (offline `--sql` mode) or when there is no
  live connection. Off unless `starrocks_wait_for_schema_change=True`.

**Config (in `context.configure`):**

```python
starrocks_wait_for_schema_change=True,          # default False
starrocks_schema_change_poll_interval=2.0,      # seconds, default 2.0
starrocks_schema_change_timeout=None,           # seconds, None = wait forever
```

Poll query:

```sql
SHOW ALTER TABLE COLUMN [FROM `<schema>`]
WHERE TableName = :table_name
ORDER BY CreateTime DESC LIMIT 1
```

### 4.3 Reversibility / downgrade

The rewriter runs independently over the `UpgradeOps` and `DowngradeOps`
directive trees. Alembic already reverses `AddColumnOp` ↔ `DropColumnOp` (a drop
carries the original column definition via `_reverse`), so the downgrade tree
contains fully-specified re-add operations. The rewriter coalesces those the
same way, producing a correct `downgrade()`:

```python
def downgrade():
    op.starrocks_alter_columns(
        't',
        adds=[sa.Column('c', INTEGER(), nullable=True)],   # re-add the dropped col
        drops=[sa.Column('a'), sa.Column('b')],
    )
```

`StarRocksAlterColumnsOp.reverse()` is also implemented (swap adds/drops) as a
best-effort for programmatic use, but generated scripts always contain an
explicit `downgrade()`, so runtime `reverse()` is not on the critical path.

---

## 5. Alternatives Considered

### 5.1 Doc-only guidance (status quo) — *rejected*

The prior guidance told users to split multi-column changes into separate
revision files and re-run until they land.

- ➖ Pushes the entire burden onto the user; pipelines can take hours.
- ➖ Does not make autogenerate output correct.
- ✅ Zero code.

Rejected: it does not solve P1; it only documents the workaround.

### 5.2 Execution-time buffering in `impl._exec` — *rejected as primary, partially reused for P2*

Override `StarRocksImpl` so that consecutive column DDL constructs targeting the
same table are buffered and flushed as a single `ALTER TABLE`.

- ✅ Works even for already-generated scripts and hand-written migrations; no
  `env.py` change.
- ➖ **Flush-boundary problem:** there is no reliable hook for "end of this
  table's column ops." You would have to flush on (a) a construct targeting a
  different table, (b) a non-column construct, and (c) end-of-migration — and
  Alembic's `DefaultImpl` exposes no clean end-of-run callback
  (`run_migrations` iterates steps with no per-step teardown we can latch onto
  cleanly). This is fragile and easy to get subtly wrong (e.g. a trailing op).
- ➖ The combined statement is invisible in the generated script (fails G2).

Rejected as the coalescing mechanism. The `_exec` override *is* used, but only
for the well-bounded Part 2 wait (post-execution polling), where there is no
buffering and no flush-boundary problem.

### 5.3 Rely on Alembic `batch_alter_table` — *rejected*

Alembic's batch mode groups operations for one table.

- ➖ On non-recreate backends it still emits separate `ALTER TABLE` statements;
  it does not produce StarRocks' single multi-clause `ALTER`.
- ➖ Batch mode is opt-in per-migration and changes authoring ergonomics; it is
  designed primarily for SQLite table-rebuilds.

Rejected: does not produce the required single-statement SQL and adds authoring
friction.

### 5.4 Post-process the emitted SQL string — *rejected*

Intercept compiled SQL and regex/parse-merge consecutive `ALTER TABLE`
statements on the same table.

- ➖ Operating on SQL text is brittle (quoting, schema qualification, comments).
- ➖ Still has the same "which statements may I merge" boundary question, now at
  the string level with less structure than the op tree.

Rejected: strictly worse than operating on the structured op tree.

### 5.5 Coalesce `MODIFY COLUMN` too, in the first cut — *deferred*

Fold type/nullability changes into the same combined `ALTER`.

- ➖ Higher risk: StarRocks `MODIFY COLUMN` restates the **entire** new column
  spec, so the rewriter would have to reconstruct a full column definition by
  merging each `AlterColumnOp`'s `modify_*` and `existing_*` fields. Alembic
  also decomposes a single `alter_column` into multiple sub-directives.
- ➖ More surface area for subtle diffs and incorrect specs.

Deferred to future work (§9). The current cut coalesces `ADD`/`DROP` only —
exactly the customer's reported case — and relies on the Part 2 wait to
serialize any residual `MODIFY` collisions.

### 5.6 Wait via a blocking wrapper only, no coalescing — *rejected as sole solution*

Just poll after every statement, without combining them.

- ➖ Waiting between N separate single-column `ALTER`s means N sequential jobs.
  Each job can take minutes to hours; N of them is far slower than one combined
  job. It "works" but is pathologically slow.
- ✅ Simple.

Rejected as a standalone fix; adopted only as the complementary Part 2.

### 5.7 Alternatives for Part 2 (the wait)

| Option | Verdict |
|---|---|
| Poll `SHOW ALTER TABLE COLUMN` in `_exec` (chosen) | ✅ Uses the documented status surface; localized to the impl. |
| Synchronous ALTER (session variable that blocks) | ➖ Not available in StarRocks; schema changes are inherently async. |
| External orchestration (pipeline polls the cluster itself) | ➖ Pushes the problem back to every user's CI; the customer explicitly asked the tool to do it. |

---

## 6. Testing

Unit tests (`test/unit/test_alter_columns.py`, 10 cases) cover:

- **Rewriter:** N add/drop ops → one op; single op unchanged; non-column ops
  preserved in order; drops-only collapse.
- **Op:** `reverse()` swaps adds/drops; `to_diff_tuple()`.
- **Compiler:** combined `ADD`+`DROP` SQL; adds-only, no schema; empty →
  `CompileError`.
- **Renderer:** valid `op.starrocks_alter_columns(...)` with a real
  `AutogenContext`.

Full unit suite (554 tests) passes. Integration against a live cluster
(add+drop+modify in one revision, with and without the wait flag) is the
recommended pre-merge gate.

---

## 7. Limitations

- **`MODIFY COLUMN` not coalesced.** A revision with both a combined add/drop
  *and* a `MODIFY` on the same table is still two jobs. Mitigation: enable the
  wait flag, or split the revision.
- **Cross-table / cross-revision** collisions are not batched; the wait flag is
  the mitigation for same-run, different-table sequencing.
- **Retroactive:** existing generated scripts are not rewritten; the rewriter
  only affects new `--autogenerate` runs.
- **Wait polling granularity:** picks the most recent job for the table by
  `CreateTime`; assumes the just-submitted job is the latest (true in the
  single-writer migration context).

---

## 8. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Rewriter reorders an interleaved non-column op incorrectly | Low | Non-column ops kept in original relative order; combined op placed at the first column-op position; unit-tested. |
| Column spec rendering differs from `op.add_column` path | Low | `toimpl` binds columns to a table and reuses the same `get_column_specification`; identical code path to `add_column`. |
| Wait loop hangs forever on a stuck job | Med | `starrocks_schema_change_timeout` raises; `CANCELLED` raises. Default (no timeout) is opt-in and documented. |
| Feature changes behavior for users who don't want it | Low | Both parts are opt-in via `env.py` wiring / config flags; default behavior unchanged. |

---

## 9. Future Work

1. **Coalesce `MODIFY COLUMN`** into the combined statement (§5.5), including
   reconstruction of full column specs from `AlterColumnOp`.
2. **Auto-wire the rewriter** by default from `StarRocksImpl` so users need not
   edit `env.py` (with an escape hatch to disable).
3. **Structured job introspection** (job id, progress %, ETA) surfaced in logs
   during the wait.
4. **Coalesce property changes** where StarRocks allows multiple `SET (...)` in
   one statement.

---

## 10. Appendix — Decision at a glance

| Concern | Decision | Key reason |
|---|---|---|
| Where to coalesce | Autogenerate rewriter (`process_revision_directives`) | Reviewable output; reuses `ModifyTableOps`; no flush-boundary problem |
| What to coalesce | `ADD` + `DROP COLUMN` only (initial) | Matches reported case; `MODIFY` is higher risk |
| Cluster sync | Opt-in poll of `SHOW ALTER TABLE COLUMN` in `_exec` | Documented status surface; localized; default off |
| User surface | `combine_column_alters` + config flags, wired in `env.py` | Consistent with existing `render_item`/`include_object` pattern |
