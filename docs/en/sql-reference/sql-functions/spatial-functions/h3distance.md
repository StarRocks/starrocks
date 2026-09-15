---
displayed_sidebar: docs
description: "Returns the grid distance in cells between two H3 cells at the same resolution."
---

# h3Distance

Returns the grid distance between two H3 cells measured in number of cells. The grid distance is the minimum number of cell steps required to travel from one cell to the other. Both cells must be at the same resolution.

## Syntax

```Haskell
BIGINT h3Distance(BIGINT start, BIGINT end)
```

## Parameters

- `start`: The starting H3 cell index. Supported data type: BIGINT.
- `end`: The ending H3 cell index. Supported data type: BIGINT.

## Return value

Returns a BIGINT representing the grid distance in cells between the two H3 cells. Returns NULL if either argument is NULL, the cells are not at the same resolution, or the distance cannot be computed (for example, cells are not in the same connected region due to pentagons).

## Examples

```sql
SELECT h3Distance(590080540275638271, 590103561300344831);
+----------------------------------------------------+
| h3Distance(590080540275638271, 590103561300344831) |
+----------------------------------------------------+
|                                                  7 |
+----------------------------------------------------+
```

## keyword

H3DISTANCE,H3,SPATIAL
