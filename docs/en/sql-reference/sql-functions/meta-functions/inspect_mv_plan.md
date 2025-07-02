---
displayed_sidebar: docs
---

# inspect_mv_plan

`inspect_mv_plan(mv_name)`
`inspect_mv_plan(mv_name, use_cache)`

These functions return the logical plan of a materialized view.

## Arguments

`mv_name`: The name of the materialized view (VARCHAR).
`use_cache`: (Optional) A boolean value indicating whether to use the materialized view plan cache. Defaults to `TRUE`.

## Return Value

Returns a VARCHAR string containing the logical plan of the materialized view.

