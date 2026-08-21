---
displayed_sidebar: docs
description: "Aggregate function that computes the intersection cardinality of two groups of Theta sketches in a single pass, routing rows by an anchor flag."
---

# ds_theta_intersect_cond_agg

Aggregate function that maintains two independent theta-union sketches — an
**anchor** group (`is_anchor = 1`) and a **window** group (`is_anchor = 0`) —
then at finalization intersects the two unions and returns the cardinality
estimate of the intersection as `DOUBLE`.

This is a single-pass equivalent of:

```SQL
ds_theta_estimate(
    ds_theta_intersect(
        ds_theta_combine(sketch) FILTER (WHERE is_anchor = 1),
        ds_theta_combine(sketch) FILTER (WHERE is_anchor = 0)
    )
)
```

## Syntax

```Haskell
DOUBLE ds_theta_intersect_cond_agg(sketch, is_anchor)
```

- `sketch`: `VARBINARY` compact theta sketch, typically produced by
  [`ds_theta_accumulate`](./ds_theta_accumulate.md) or
  [`ds_theta_combine`](./ds_theta_combine.md).
- `is_anchor`: `INT` flag. `1` routes the sketch to the anchor group;
  any other value routes it to the window group.

## Return value

Returns `DOUBLE`. Returns `0` if either group receives no sketches.

## Examples

```SQL
-- Distinct users who appeared in both the anchor cohort and the window cohort.
SELECT
    ds_theta_intersect_cond_agg(sketch, is_anchor) AS overlap_estimate
FROM (
    SELECT ds_theta_accumulate(user_id) AS sketch, 1 AS is_anchor
    FROM events WHERE cohort = 'anchor'
    UNION ALL
    SELECT ds_theta_accumulate(user_id) AS sketch, 0 AS is_anchor
    FROM events WHERE cohort = 'window'
) t;
```

## Keywords

DS_THETA_INTERSECT_COND_AGG, DS_THETA_COMBINE, DS_THETA_INTERSECT, DS_THETA_ESTIMATE
