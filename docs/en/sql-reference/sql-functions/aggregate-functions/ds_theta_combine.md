---
displayed_sidebar: docs
---

# ds_theta_combine

Aggregate function that unions serialized Apache DataSketches Theta sketches
(`VARBINARY`) across rows and returns a single serialized compact sketch.

Input is a column of compact theta sketches from any source: built via
[`ds_theta_accumulate`](./ds_theta_accumulate.md), produced by the pairwise
scalar set ops [`ds_theta_union`](./ds_theta_union.md) /
[`ds_theta_intersect`](./ds_theta_intersect.md) /
[`ds_theta_a_not_b`](./ds_theta_a_not_b.md), or loaded externally as
`VARBINARY` from Parquet/Iceberg.

The on-wire format is the standard Apache DataSketches C++ compact theta
serialization, so result sketches interoperate with any other Apache
DataSketches implementation that uses the default hash seed.

## Syntax

```Haskell
VARBINARY ds_theta_combine(sketch)
```

- `sketch`: a `VARBINARY` column of compact theta sketches.

## Examples

```SQL
-- Roll daily sketches up to the year.
SELECT year(day), ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches
GROUP BY year(day);
```

## Keywords

DS_THETA_COMBINE, DS_THETA_ACCUMULATE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
