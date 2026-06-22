---
displayed_sidebar: docs
---

# ds_theta_a_not_b

Scalar set difference of two serialized Apache DataSketches Theta sketches.
Returns a serialized compact sketch whose distinct count estimates
`|A \ B|` (elements in A but not in B). Returns `NULL` if either input is `NULL`.

## Syntax

```Haskell
VARBINARY ds_theta_a_not_b(sketch_a, sketch_b)
```

- `sketch_a`, `sketch_b`: `VARBINARY` compact theta sketches.

## Examples

```SQL
-- Distinct users in cohort A who did not appear in cohort B.
SELECT ds_theta_estimate(ds_theta_a_not_b(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## Keywords

DS_THETA_A_NOT_B, DS_THETA_UNION, DS_THETA_INTERSECT
