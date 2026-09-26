---
displayed_sidebar: docs
description: "Returns all child H3 cells of the given index at a finer resolution."
---

# h3ToChildren

Returns all child H3 cells of the given index at a specified finer resolution. Each hexagon has 7 children at the next resolution level; a pentagon has 6.

## Syntax

```Haskell
ARRAY<BIGINT> h3ToChildren(BIGINT h3index, INT resolution)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.
- `resolution`: The target child resolution, which must be greater than or equal to the resolution of `h3index`. Supported data type: INT.

## Return value

Returns an `ARRAY<BIGINT>` of all child cell indexes at the specified resolution. Returns NULL if either argument is NULL, `resolution` is less than the cell's resolution, or `h3index` is not a valid H3 cell index.

## Examples

```sql
SELECT h3ToChildren(599405990164561919, 6);
+------------------------------------------------------------------------------------------------------------+
| h3ToChildren(599405990164561919, 6)                                                                        |
+------------------------------------------------------------------------------------------------------------+
| [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] |
+------------------------------------------------------------------------------------------------------------+
```

## keyword

H3TOCHILDREN,H3,SPATIAL
