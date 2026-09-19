---
displayed_sidebar: docs
description: "Returns the parent H3 cell of the given index at a coarser resolution."
---

# h3ToParent

Returns the parent H3 cell of the given index at a specified coarser resolution. In the H3 hierarchy, each cell at resolution r is fully contained within exactly one cell (its parent) at resolution r-1.

## Syntax

```Haskell
BIGINT h3ToParent(BIGINT h3index, INT resolution)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.
- `resolution`: The target parent resolution, which must be less than or equal to the resolution of `h3index`. Supported data type: INT.

## Return value

Returns a BIGINT representing the parent cell index at the specified resolution. Returns NULL if either argument is NULL, `resolution` is greater than the cell's resolution, or `h3index` is not a valid H3 cell index.

## Examples

```sql
SELECT h3ToParent(599405990164561919, 3);
+-----------------------------------+
| h3ToParent(599405990164561919, 3) |
+-----------------------------------+
| 590398848891879423                |
+-----------------------------------+
```

## keyword

H3TOPARENT,H3,SPATIAL
