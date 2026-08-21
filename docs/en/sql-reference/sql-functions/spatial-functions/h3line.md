---
displayed_sidebar: docs
description: "Returns an ordered array of H3 cells forming a grid path from start to end."
---

# h3Line

Returns an ordered array of H3 cell indexes forming a grid path from the start cell to the end cell (inclusive). The path follows the shortest grid route between the two cells. Both cells must be at the same resolution.

## Syntax

```Haskell
ARRAY<BIGINT> h3Line(BIGINT start, BIGINT end)
```

## Parameters

- `start`: The starting H3 cell index. Supported data type: BIGINT.
- `end`: The ending H3 cell index. Supported data type: BIGINT.

## Return value

Returns an `ARRAY<BIGINT>` of H3 cell indexes from the start cell to the end cell, in order. The array length equals `h3Distance(start, end) + 1`. Returns NULL if either argument is NULL, the cells are not at the same resolution, or the path cannot be computed (for example, when pentagons block the route).

## Examples

```sql
SELECT h3Line(590080540275638271, 590103561300344831);
+----------------------------------------------------------------------------------------------+
| h3Line(590080540275638271, 590103561300344831)                                               |
+----------------------------------------------------------------------------------------------+
| [590080540275638271,590080471556161535,590080883873021951,590106516237844479,590104385934065663,590103630019821567,590103561300344831] |
+----------------------------------------------------------------------------------------------+
```

## keyword

H3LINE,H3,SPATIAL
