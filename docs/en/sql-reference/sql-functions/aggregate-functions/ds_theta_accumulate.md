---
displayed_sidebar: docs
description: "Builds an Apache DataSketches Theta sketch from column values and returns a serialized compact sketch as VARBINARY."
---

# ds_theta_accumulate

Builds an Apache DataSketches Theta sketch over `expr` and returns the sketch
serialized as `VARBINARY` (compact form). Pair with [ds_theta_combine](./ds_theta_combine.md)
and [ds_theta_estimate](../scalar-functions/ds_theta_estimate.md) to materialize and reuse sketches.

The output uses the standard Apache DataSketches C++ compact serialization format,
compatible with any Apache DataSketches consumer.

:::note
`ds_theta_accumulate` pre-hashes input values before passing them to DataSketches.
Set operations between StarRocks-accumulated sketches and sketches built externally
from the same raw values will not produce correct results due to this hash difference.
Use `ds_theta_combine`, `ds_theta_intersect`, and `ds_theta_a_not_b` to operate on
sketches that were all produced by the same accumulation path.
:::

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
