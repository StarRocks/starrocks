---
displayed_sidebar: docs
---

# ds_theta_combine

Aggregates serialized Apache DataSketches Theta sketches (`VARBINARY`) by
union-merging them, returning a single serialized compact sketch. Inverse of
[ds_theta_accumulate](./ds_theta_accumulate.md) at the aggregate layer.

The on-wire format is the standard Apache DataSketches C++ compact theta
serialization, so sketches produced by any conforming Apache DataSketches
implementation can be combined here without translation.

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
