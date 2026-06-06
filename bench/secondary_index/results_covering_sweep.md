# No-readback sweep: secondary index (covering) vs ORDER BY

100M rows, single tablet. Two query shapes that need NO base readback:
`cnt` = `SELECT COUNT(*) WHERE user_id <range>`, `town` = `SELECT MAX(town) WHERE user_id <range>`.
p50 ms, hot (warmup dropped). idx = enable_secondary_index_read=true; ob = ORDER BY(user_id,town) table.

## VARCHAR (user_id VARCHAR(10), town VARCHAR(64))

| selectivity | idx cnt | ORDER BY cnt | idx town | ORDER BY town |
|---|---|---|---|---|
| 0.0005% | 32.5 | 10.9 | 32.2 | 10.6 |
| 0.55%   | 283.9 | 30.4 | 67.6 | 31.5 |
| 5.5%    | 2381.7 | 56.9 | 184.2 | 74.5 |
| 11.1%   | 4778.1 | 68.7 | 272.8 | 80.7 |
| 27.8%   | 11743.5 | 117.1 | 481.8 | 127.7 |

## BIGINT (user_id BIGINT, town VARCHAR(64))

| selectivity | idx cnt | ORDER BY cnt | idx town | ORDER BY town |
|---|---|---|---|---|
| 0.0005% | 26.8 | 10.9 | 15.1 | 6.1 |
| 0.55%   | 157.3 | 12.0 | 40.8 | 14.6 |
| 5.5%    | 1285.5 | 14.0 | 448.9 | 21.0 |
| 11.1%   | 4815.8 | 20.4 | 208.8 | 34.8 |
| 27.8%   | 6046.6 | 40.3 | 367.4 | 82.1 |

## Conclusion

ORDER BY wins decisively at every selectivity for both query shapes and both column
types; the gap widens with selectivity. Reason: the predicate is on `user_id`, which is
exactly the ORDER BY leading key, so ORDER BY reads one contiguous physical range with
zero per-row overhead. The secondary index must scan its sorted `.idx` and decode each
`__sidx_pos__`; COUNT(*) currently does not engage the covering fast path (disabled by
the global-dict guard) and falls back to a full base scan (profile: 11 GB read for the
VARCHAR COUNT(*) at 5.5%), inflating its numbers.

IMPORTANT framing: filtering on the ORDER BY sort key is the secondary index's WORST
case -- ORDER BY is purpose-built for its own leading key. The index's real value is
acceleration for columns that are NOT the single physical sort key (you can have many
secondary indexes but only one ORDER BY). This test does not exercise that case.

## Correctness (validated)
- COUNT(*) covering correct at all selectivities after the per-morsel rowid-range fix
  (was inflated x71 by morsel replication).
- DELETE honored via DelVec-by-position (covered COUNT drops exactly by deleted rows).
