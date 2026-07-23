---
displayed_sidebar: docs
---

# ds_theta_intersect

Scalar pairwise intersection of two serialized Apache DataSketches Theta sketches.
Returns a single serialized compact sketch whose distinct count estimates
`|A ∩ B|`. Returns `NULL` if either input is `NULL`.

Unlike HyperLogLog sketches, theta sketches preserve enough state to support
intersection, which is what makes this operation possible without storing the
underlying values.

## Syntax

```Haskell
VARBINARY ds_theta_intersect(sketch_a, sketch_b)
```

- `sketch_a`, `sketch_b`: `VARBINARY` compact theta sketches.

## Examples

```SQL
-- Distinct users who appeared in both cohort A and cohort B.
SELECT ds_theta_estimate(ds_theta_intersect(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## Keywords

DS_THETA_INTERSECT, DS_THETA_UNION, DS_THETA_A_NOT_B
