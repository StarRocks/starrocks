---
displayed_sidebar: docs
---

# ds_theta_accumulate

Builds an Apache DataSketches Theta sketch over `expr` and returns the sketch
serialized as `VARBINARY` (compact form). Pair with [ds_theta_combine](./ds_theta_combine.md)
and [ds_theta_estimate](./ds_theta_estimate.md) to materialize and reuse sketches.

The output uses the standard Apache DataSketches C++ compact serialization, so
sketches written by StarRocks can be consumed by any Apache DataSketches
implementation that uses the default hash seed, and vice versa.

## Syntax

```Haskell
VARBINARY ds_theta_accumulate(expr)
```

- `expr`: column whose distinct values are summarized.

## Examples

```SQL
-- Persist sketches per group, then estimate distinct count from them.
CREATE TABLE sketches AS
SELECT grp, ds_theta_accumulate(id) AS sk FROM t GROUP BY grp;

SELECT grp, ds_theta_estimate(sk) FROM sketches;
```

## Keywords

DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
