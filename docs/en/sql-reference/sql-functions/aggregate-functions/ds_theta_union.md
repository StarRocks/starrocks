---
displayed_sidebar: docs
---

# ds_theta_union

Scalar pairwise union of two serialized Apache DataSketches Theta sketches.
Returns a single serialized compact sketch whose distinct count estimates
`|A ∪ B|`. Returns `NULL` if either input is `NULL`.

For an aggregate-style union over many sketches at once, use
[ds_theta_combine](./ds_theta_combine.md).

## Syntax

```Haskell
VARBINARY ds_theta_union(sketch_a, sketch_b)
```

- `sketch_a`, `sketch_b`: `VARBINARY` compact theta sketches.

## Examples

```SQL
SELECT ds_theta_estimate(ds_theta_union(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## Keywords

DS_THETA_UNION, DS_THETA_INTERSECT, DS_THETA_A_NOT_B, DS_THETA_COMBINE
