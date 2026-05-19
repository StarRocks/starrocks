---
displayed_sidebar: docs
---

# ds_theta_estimate

Scalar function that returns the approximate distinct count summarized by a
serialized Apache DataSketches Theta sketch (`VARBINARY`, compact form). One
output row per input row.

Accepts any sketch written in the standard Apache DataSketches C++ compact theta
format with the default hash seed, including sketches produced by:

- [`ds_theta_accumulate`](./ds_theta_accumulate.md) (build from raw values)
- [`ds_theta_combine`](./ds_theta_combine.md) (union across rows)
- [`ds_theta_union`](./ds_theta_union.md), [`ds_theta_intersect`](./ds_theta_intersect.md),
  [`ds_theta_a_not_b`](./ds_theta_a_not_b.md) (pairwise set ops)
- Externally produced sketches loaded as raw `VARBINARY` from Parquet, Iceberg,
  or another Apache DataSketches consumer

Returns `NULL` for `NULL` input. Returns 0 for an empty sketch.

## Syntax

```Haskell
BIGINT ds_theta_estimate(sketch)
```

- `sketch`: `VARBINARY` compact theta sketch.

## Examples

```SQL
-- Per-row estimate of a sketch column
SELECT ds_theta_estimate(sk) FROM sketches;

-- Compose with set ops
SELECT ds_theta_estimate(ds_theta_union(a.sk, b.sk)) AS u,
       ds_theta_estimate(ds_theta_intersect(a.sk, b.sk)) AS i
FROM cohort_a a JOIN cohort_b b USING (day);

-- Estimate of a daily-rolled union
SELECT ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches;
```

## Keywords

DS_THETA_ESTIMATE, DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_COUNT_DISTINCT
