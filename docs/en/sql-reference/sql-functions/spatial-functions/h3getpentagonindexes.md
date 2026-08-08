---
displayed_sidebar: docs
description: "Returns all 12 pentagon H3 cell indexes at the given resolution."
---

# h3GetPentagonIndexes

Returns all 12 pentagon H3 cell indexes at the given resolution. Every H3 resolution has exactly 12 pentagons, one at each of the 12 icosahedron vertices.

## Syntax

```Haskell
ARRAY<BIGINT> h3GetPentagonIndexes(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns an `ARRAY<BIGINT>` containing the 12 pentagon cell indexes at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT array_length(h3GetPentagonIndexes(3));
+---------------------------------------+
| array_length(h3GetPentagonIndexes(3)) |
+---------------------------------------+
| 12                                    |
+---------------------------------------+
```

## keyword

H3GETPENTAGONINDEXES,H3,SPATIAL
