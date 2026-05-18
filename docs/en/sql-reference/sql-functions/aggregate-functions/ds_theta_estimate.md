---
displayed_sidebar: docs
---

# ds_theta_estimate

Returns the approximate distinct count summarized by a serialized Apache
DataSketches Theta sketch (`VARBINARY`, compact form). Inverse of
[ds_theta_accumulate](./ds_theta_accumulate.md).

Accepts any sketch written in the standard Apache DataSketches C++ compact theta
format with the default hash seed.

## Syntax

```Haskell
BIGINT ds_theta_estimate(sketch)
```

- `sketch`: `VARBINARY` compact theta sketch.

## Examples

```SQL
SELECT ds_theta_estimate(sk) FROM sketches;
```

## Keywords

DS_THETA_ESTIMATE, DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_COUNT_DISTINCT
