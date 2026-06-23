# StarRocks Shared-Data PK Table · Lightweight Sorted Secondary Index · Full Design

> Scope: shared-data Primary Key tables. Goal: accelerate queries whose predicate falls on **non-primary-key / non-sort-key** columns, without changing the base table's physical layout, without slowing primary-key point lookups, and allowing **multiple** indexes per table.

---

## 0. Goals & Design Trade-offs

- **Problem**: A PK table can only physically cluster on a single dimension (primary key / sort key). A predicate on any other column forces a full scan. `ORDER BY` optimizes only one dimension, penalizes PK point lookups, and cannot be stacked.
- **Positioning**: A **sorted side-car file outside the base table** that stores only `[index columns + row-position pointer]` (not full rows), ordered by the index columns. Many can coexist on one table; the primary key and base layout are untouched.
- **Key trade-offs**:
  - Choose **sorted runs** (binary-search a range inside a run) over Roaring inverted lists — the main use case is high-cardinality range queries, where inverted-list OR over a wide range explodes.
  - Choose **row-precise positions** (enables covering / no read-back) over coarse skip-index granularity — this is the differentiator vs `ORDER BY`.
  - Build via **external-merge style** (sort small batches into runs, merge later) rather than a single full in-memory sort of the whole rowset — avoids OOM, streams.

> **[Diagram 1 — Architecture / positioning]**
> AI prompt: "Clean flat technical comparison diagram, white background, English labels, blue/teal accent. LEFT panel titled 'ORDER BY': one large table box physically re-sorted by a single key 'user_id', a note 'one physical order only; PK point query slowed'. RIGHT panel titled 'Sorted Secondary Index': one unchanged base table box labeled 'base data (sorted by PK)', plus three small separate side-car file boxes labeled 'index on user_id', 'index on create_ts', 'index on town', each shown as a thin sorted strip pointing back to the base table with dashed arrows labeled 'row-position pointer'; a note 'base layout unchanged; many indexes; PK point query unaffected'. Minimal, labeled rounded rectangles, no photorealism."

---

## 1. File Format (`.sidx` run file)

### 1.1 Synthetic schema

Each run file is a small **segment-v2 format** file with schema:

```
[ index_col_1, …, index_col_K, __sidx_pos__ BIGINT ]
```

- Sorted by the **index-column prefix** within the file;
- Index columns are marked key / sort_key in the synthetic schema, with **page-level zone-map + bloom filter** enabled by default;
- `__sidx_pos__` = the row-position pointer:

```
__sidx_pos__ = (segment_ordinal << 32) | rowid_in_segment      // int64
```

`segment_ordinal` is the row's segment ordinal **within this rowset**; the global rssid = `rowset_id + segment_ordinal` (reconstructed at query time, for DelVec). Because the position is absolute within the rowset, **one run file can span multiple segments**.

> **[Diagram 2 — File format & position encoding]**
> AI prompt: "Clean flat technical diagram, white background, monospace labels, blue accent. TOP: a sorted index run file shown as a table with columns 'user_id | town | __sidx_pos__ (int64)' and 3-4 example rows sorted ascending by user_id. BOTTOM-LEFT: a 64-bit integer split into two halves, high 32 bits boxed 'segment_ordinal', low 32 bits boxed 'rowid_in_segment'. An arrow from one __sidx_pos__ value goes to BOTTOM-RIGHT, pointing at a specific row inside a 'base segment' box (rows drawn as horizontal lines). Caption arrows: 'sorted by index column' on top, 'points to exact base row' on the pointer. Flat vector, labeled."

### 1.2 Runs (multiple files) & naming

One `(rowset, index)` may map to **multiple run files** (flushed from the write buffer; **not ordered relative to each other**; each run is **internally** sorted). File names follow the same style as segments — `txn + uuid` for uniqueness, no logical info embedded:

```
<txn_id>_<uuid>.sidx
```

Logical info (which index, which rowset) lives entirely in metadata (§2), not in the file name.

- At write time: number of runs ≈ index-data-size / buffer-size (default 100 MB) → tens for a large rowset;
- After compaction: merged into 1 ~ a few **sorted** files (§4).

### 1.3 File-level pruning info

Because runs are unordered relative to each other, cross-run pruning must use **file-level** info (block-level zone-map is weak on random high-cardinality columns; here we rely on file-level min/max + bloom):

- Each run records: index-prefix `min_key / max_key`, `row_count`, and (optionally) a file-level bloom filter.

---

## 2. Metadata Organization

### 2.1 Rowset metadata (two-level)

In `RowsetMetadataPB`, there is **one `SecondaryIndexPB` per secondary index**, holding that index's **multiple run files**:

```protobuf
message RowsetMetadataPB {
  ...
  repeated SecondaryIndexPB secondary_indexes = 20;   // one per secondary index
}

// A secondary index (logical level): name & columns stored once
message SecondaryIndexPB {
  optional string index_name        = 1;   // logical index name
  repeated string index_col_names   = 2;   // ordered index columns
  repeated SecondaryIndexRunPB runs = 3;   // run files (sorted within a run, unordered across runs)
}

// One run index file (physical level)
message SecondaryIndexRunPB {
  optional string file_name = 1;   // <txn_id>_<uuid>.sidx
  optional int64  file_size = 2;
  optional int64  row_count = 3;
  optional bytes  min_key   = 4;   // file-level zone-map (index prefix)
  optional bytes  max_key   = 5;
  // bloom filter metadata (optional)
}
```

- **Two levels**: logical info (name, columns) stored once in `SecondaryIndexPB`; `runs` is its list of physical files. The read path iterates index → its `runs`, no grouping-by-name needed.
- Proto fields stay optional/repeated; `secondary_indexes` uses tag 20 (tag 19 is taken by upstream `uid`).

### 2.2 Index definition (carried by table schema)

A secondary index definition is **part of the table schema**, written by FE DDL (inline `INDEX … USING SORTED` at create, or `ALTER TABLE … ADD INDEX`), and shipped to BE via `TabletSchemaPB`:

```protobuf
message TabletIndexPB {              // table-level index definition (extended)
  optional string index_name    = 1;
  repeated string col_names     = 2;   // ordered index columns
  optional IndexType index_type = 3;   // new enum value: SORTED
  optional string comment       = 4;
}
```

- The write path reads SORTED index definitions directly from `TabletSchema` to decide which columns to build runs for;
- Fully driven by real DDL — no BE-side config / backdoor.

> **[Diagram 3 — Two-level metadata]**
> AI prompt: "Clean flat tree/hierarchy diagram, white background, blue accent, monospace labels. Root box 'RowsetMetadataPB' connects down to a 'secondary_indexes []' group. Under it two 'SecondaryIndexPB' boxes (e.g. index_name='idx_user_town', cols=[user_id,town]) and (index_name='idx_ts', cols=[create_ts]). Each SecondaryIndexPB branches to a 'runs []' list of several 'SecondaryIndexRunPB' leaf boxes showing 'file=<txn>_<uuid>.sidx, min_key, max_key, row_count'. Emphasize the two levels: logical (name/cols) once, physical (run files) many. Labeled rounded rectangles, vertical layout."

---

## 3. Write Flow (load)

### 3.1 Overview

Loads (with or without load spill) write segments through the delta writer. For **each `(tablet, index)`** there is a **secondary-index write buffer** (default 100 MB, configurable). As the delta writer appends a chunk to the segment, it also writes the **index column values + row positions** into the buffer.

```
DeltaWriter ─append chunk──▶ TabletWriter ──▶ segment
                  │ extract index cols + pos in the same pass
                  ▼
        secondary-index write buffer (per tablet/index, 100MB)
                  │ when full → submit flush task
                  ▼
        secondary-index flush threadpool (async)
                  │ each task: sort the buffer by index columns → write one run file
                  ▼
              run file (<txn_id>_<uuid>.sidx)
```

### 3.2 Flush & run generation

- Buffer fills (or a segment-roll boundary is hit) → **hand off** the accumulated columns to a flush task on the **async threadpool**; the original buffer is cleared and keeps receiving.
- Flush task: **sort this batch (≤100 MB) in place** (`sort_and_tie_columns`, bounded memory) → write one run file via a segment writer → return that run's `SecondaryIndexRunPB` (file_name / min / max / row_count).
- **Bounded memory**: at any moment ≈ `concurrent(load+compact) tablets × indexes × 100MB × (filling + in-flight flush)`.
- **Global memory budget** (important): do not let a fixed 100 MB per buffer scale linearly; use a **global secondary-index memory budget** to drive flushes — when exceeded, flush the largest buffer first, so the peak is capped by one global limit (mirroring how memtable flush is globally governed).

### 3.3 Commit (finish)

- At `finish()`: **drain** all pending flush tasks for this tablet, collect all runs into one `SecondaryIndexPB` per index (its `runs` holds all runs), write into rowset metadata; only then is the rowset visible.
- Any flush failure → the whole load fails and rolls back.
- Async tail latency = the time to flush the last buffer.
- Load-spill vs not is **unified** here: index extraction happens at the delta writer's append point, independent of spill.

---

## 4. Compaction Flow (two-phase: merge index files only + LCRM remap)

Compaction **does not re-extract from base via the write buffer**. After the base segments finish merging, it enters a **second phase: secondary-index merge**, reading only the input rowsets' `.sidx` files, merging + remapping positions.

### 4.1 Why

- Reads only `.sidx` (hundreds of MB), not the base (GBs);
- Input `.sidx` are already sorted runs → a k-way merge yields a **sorted, consolidated output** (solves read amplification);
- Old rowsets' positions are invalidated by the merge → remap via **LCRM (Lake Compaction Rows Mapper)**.

### 4.2 LCRM direction (key)

LCRM records, **in output-row order**, each output row's **source old position** `(rssid<<32|rowid)` → i.e. **new→old**; the new position is implicit from "the i-th entry + output segment row counts".

The index merge needs **old→new**, the opposite direction → we must **invert** LCRM into old→new:

```
inverse[old_pos] = new_pos          // old position → new position
```

- Form: per-input-rssid `new_pos[rowid]` arrays (8 B/row); a missing old position = deleted/overwritten (TOMBSTONE).
- Scale: ≈ input_rows × 8 B (full 100 M ≈ 800 MB; incremental counts only the merged input). Back it with mmap / spill / the compaction memory budget.
- Note: the persistent-PK-index update path updates by PK value in output order and does **not** build this inverse → it is new memory.

### 4.3 Free bonus: delete filtering

LCRM contains only **surviving rows**. Once the inverse is built, an old position **absent from inverse ⟺ that row was deleted** → drop that index entry during merge. **No separate DelVec lookup** — remap + survivor filtering done in one pass.

### 4.4 Flow

```
base segments merged + LCRM written
  ① read LCRM (output order): i → (new_seg, new_rowid); fill inverse[old_pos] = new_pos
  ② k-way merge all runs of this index across input rowsets (sorted by index col, streaming, bounded memory)
  ③ for each (index_val, old_pos):
        inverse hit  → emit (index_val, new_pos)
        inverse miss → drop (deleted)
  ④ write the [sorted + remapped + consolidated] new index file(s) (optionally split by target size)
```

### 4.5 Trigger

Cheap (read `.sidx` + one merge + incidental de-amplification), so compaction can do it **almost always**. Optional gate: `index_file_count > segment_count` AND `overlapped == false` (only invest in settled outputs). Failure → **graceful fallback** (keep the unmerged runs; correctness unaffected).

> **[Diagram 4 — Compaction two-phase LCRM merge]**
> AI prompt: "Clean flat two-phase pipeline diagram, white background, blue/teal accent, English labels. PHASE 1 (top): several 'input rowset' boxes with their base segments flowing into a 'base merge' box, producing 'output rowset (new segments)' and a side artifact box 'LCRM (new→old, output order)'. PHASE 2 (bottom): on the left, several 'input .sidx runs (sorted by index col, OLD positions)' boxes feed a 'k-way merge' funnel; from the right, 'LCRM' is transformed by an 'invert' box into 'inverse: old_pos → new_pos (TOMBSTONE = deleted)' feeding into the merge; the merge emits one 'output .sidx (sorted, NEW positions, deleted dropped, consolidated)' box. Annotate arrows: 'remap old→new', 'drop deleted rows'. Funnel/merge visual, labeled rounded rectangles."

---

## 5. Query Flow (read)

### 5.1 Entry & prefix gate

```
query predicate
 │ ① prefix gate: predicate must hit the index's LEADING column, else skip that index
 ▼
TabletReader::get_segment_iterators
 │ ② iterate each SecondaryIndexPB of the rowset (two-level: index → its runs)
 │ ③ file-level pruning: skip a run whose [min_key,max_key]/bloom doesn't overlap the predicate
 ▼
 two paths: lookup (read-back) / covering (no read-back)
```

### 5.2 Lookup path (primary)

```
for each chosen index:
   per_index_bitmap = empty
   for run in this index's runs (those not pruned):
       push predicate into the run's segment scan (zone-map/bloom) → decode __sidx_pos__
       → one candidate Roaring per segment; UNION into per_index_bitmap     ← union runs of same index
   merged = (first ? per_index_bitmap : merged ∩ per_index_bitmap)          ← intersect across indexes (multi-index AND)
→ feed merged as presupplied_rowid_filter to the base scan, read only candidate rows
(DelVec / residual predicate handled by the existing scan pipeline; lookup result shared across morsels)
```

- Parallelism: the base scan is still split by each morsel's rowid_range; lookup does not reduce its parallelism.

### 5.3 Covering path (no read-back)

When **predicate columns ∪ output columns** are all ⊆ some index's columns, answer directly from `.sidx`, never touching base:

```
for run in matched runs:
   emit the output index columns for the [lo,hi] matched range
   if needed: absolute position (rowset_id+seg, rowid) → DelVec filter
```

Optimizations:
- **No-delete fast path**: when the rowset has no delete vector, skip reading `__sidx_pos__`, skip per-row decode + DelVec; `COUNT(*)` collapses to "count matched rows".
- **Parallelism**:
  - Single sorted file (after compaction merge): split by `.sidx` row positions across morsels (`N_idx == N_base` numeric mapping), parallel with no overlap;
  - Multiple runs (un-merged at write time): use **run as the parallel unit** across morsels.

### 5.4 When to enable (cost gate)

- **Phase 1: explicit hint** `[_USE_SORTED_INDEX_]`.
- **Phase 2: automatic via CBO**. Estimate "index path cost (candidate rows → pages touched on read-back)" vs "full scan (total pages)", take the cheaper; otherwise fall back to full scan.
  - Same idea as ClickHouse choosing a projection via `sum_marks` comparison — "measure the work, take the minimum";
  - **Difference**: ours is scatter read-back, so the cost must account for **read amplification** (how many pages the matched rows touch — page-level, not just candidate-row count); the measured crossover is ~10% selectivity.

> **[Diagram 5 — Query flow]**
> AI prompt: "Clean flat branching flowchart, white background, blue/teal accent, English labels. Start box 'query predicate' → 'prefix gate (must hit leading index column)' → 'file-level prune (min/max + bloom per run)'. Then split into TWO branches. LEFT branch 'Lookup (read-back)': 'scan matched runs → decode positions' → 'union candidate bitmaps across runs' → 'intersect across indexes (AND)' → 'narrow base scan (read only candidates)'. RIGHT branch 'Covering (no read-back)': 'read matched runs → emit output cols directly' → 'optional DelVec filter' → 'result (no base access)'. Small note box at bottom: 'enable via hint (P1) / CBO cost gate (P2)'. Decision diamonds for the gate, rounded rectangles for steps, two clearly labeled columns."

---

## 6. User Interface

### 6.1 Define at table creation

```sql
CREATE TABLE orders (
    order_id BIGINT NOT NULL, create_ts DATETIME NOT NULL, user_id BIGINT NOT NULL,
    town VARCHAR(64), amount DECIMAL(18,2),
    INDEX idx_user_town (user_id, town) USING SORTED COMMENT '...',
    INDEX idx_ts        (create_ts)     USING SORTED
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 16;
```
- Multiple indexes; a composite index accelerates predicates on its **leading prefix**.

### 6.2 Add / drop on an existing table

```sql
ALTER TABLE orders ADD  INDEX idx_ts (create_ts) USING SORTED;   -- async rebuild over existing data; inline for new writes
ALTER TABLE orders DROP INDEX idx_ts;
```

### 6.3 Enable on a query

```sql
-- Phase 1: hint (table-level; works with joins)
SELECT max(amount) FROM orders [_USE_SORTED_INDEX_] WHERE user_id BETWEEN 12345 AND 13444;
-- Phase 2: CBO auto-selection, no hint needed
```

---

## 7. Memory & Concurrency Model

| Dimension | Design |
|---|---|
| Write memory | per-(tablet,index) buffer (default 100 MB) + **global memory budget** driving flush; no full in-memory sort of the whole rowset |
| Flush concurrency | dedicated secondary-index flush threadpool, async; drained at commit |
| Compaction memory | inverse (old→new) ~O(input rows); back with mmap / spill / budget |
| Reader cache | process-wide LRU of opened runs (footer + column readers) to avoid re-opening |
| Lookup cache | candidate bitmap for the same (.sidx, predicate) computed once, shared across morsels |

---

## 8. Roadmap

| Phase | Content |
|---|---|
| **Phase 1** | **Full DDL** (`CREATE/ALTER TABLE … INDEX … USING SORTED`) + **Write** (per-(tablet,index) buffer, async flush, multi-run; incl. compaction's two-phase LCRM merge + old→new remap + delete filtering) + **Read** (lookup + covering; file-level pruning, multi-run union, multi-index AND) + **Hint** (`[_USE_SORTED_INDEX_]`) |
| **Phase 2** | **CBO auto-selection**: choose index vs full scan by cost (read-back pages vs full-scan pages), retire the hint dependency (see §5.4). Not in Phase 1. |

---

## Appendix: Key Invariants

- Position encoding `(seg_ordinal<<32)|rowid`; global rssid = `rowset_id + seg_ordinal` (matches the base scan's `_opts.rowset_id + segment_id()`), keeping DelVec / read-back correct.
- Each `SecondaryIndexPB` holds multiple runs; runs are internally sorted but unordered across each other (cross-run pruning via file-level min/max + bloom); compaction merge converges them to sorted.
- The index is an **optimization, not correctness**: at any stage (build failure / not yet built / over budget) the query can safely fall back to a full scan.
