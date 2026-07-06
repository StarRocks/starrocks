# Secondary index vs ORDER BY — corrected results (100M rows, single tablet, p50 ms)

Two bugs were masking the index in the first sweep:
1. Background lake compaction's index rebuild OOM'd (512 MB limit < 581 MB .idx) →
   compacted rowset lost the index → queries silently full-scanned. Fixed: raise
   `secondary_index_build_mem_limit_mb`, disable compaction, reload.
2. Covering path rescanned the whole matched `.idx` once PER morsel (71x). Fixed:
   one designated morsel scans it once (profile: RawRowsRead 395M → 5.57M).

## THE key result — predicate on a NON-sort-key column (index's purpose)

`WHERE create_ts BETWEEN ...` ; `orders_ob` is ORDER BY(user_id,town) so it CANNOT
prune create_ts and must full-scan. Index uses the parallel lookup path.

| range | rows | idx COUNT(*) | ORDER BY COUNT(*) | speedup |
|---|---|---|---|---|
| 1 day  | 114K  | 41 ms  | 243 ms | ~6x |
| 1 week | 802K  | 66 ms  | 248 ms | ~3.7x |
| 1 month| 3.4M  | 127 ms | 259 ms | ~2x |
| 3 month| 10.5M | 234 ms | 275 ms | ~1.2x |

ORDER BY is a flat ~250 ms (always full-scans a non-sort-key); the index scales with
selectivity and wins by up to 6x. **A table has one physical sort order but many
secondary indexes — this is exactly what the index is for.**

## Predicate on the sort key (index's worst case)

`WHERE user_id BETWEEN ...` ; user_id is ORDER BY's leading key → ORDER BY is optimal
(parallel contiguous scan). The index cannot beat it here:

| sel | idx lookup COUNT(*) | idx covering COUNT(*) | ORDER BY COUNT(*) |
|---|---|---|---|
| 0.55% | 65 | 63 | 43 |
| 5.5%  | 146 | 382 | 64 |
| 27.8% | 353 | 1838 | 119 |

## Covering (no-readback) vs lookup

Covering now correct + no 71x blowup, but it runs single-threaded (one designated
morsel), so it loses to the PARALLEL lookup path except at very high selectivity /
tiny result sets (e.g. create_ts 1-day: covering 31 ms vs lookup 41 ms). To make
covering win in general it needs the `.idx` scan parallelized across morsels (the
morsel framework partitions by base rowid, not by index-key order — that's the next
optimization), plus per-row overhead cuts (skip DelVec when a segment has no deletes).

## Bottom line
- Index BEATS ORDER BY by up to 6x on non-sort-key predicates (its real use case).
- Index ties/loses on the sort key (ORDER BY is optimal there by construction).
- Lookup path (parallel) is currently the best index path; covering needs scan
  parallelization to surpass it.
