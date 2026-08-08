---
displayed_sidebar: docs
description: "Returns all H3 cells within grid distance k of the origin cell, including the origin."
---

# h3kRing

Returns all H3 cells within grid distance k of the origin cell (a filled disk). The result includes the origin cell itself and is returned in an unordered array.

## Syntax

```Haskell
ARRAY<BIGINT> h3kRing(BIGINT h3index, INT k)
```

## Parameters

- `h3index`: The origin H3 cell index. Supported data type: BIGINT.
- `k`: The maximum grid distance. Must be a non-negative integer. Supported data type: INT.

## Return value

Returns an `ARRAY<BIGINT>` of H3 cell indexes within grid distance k of the origin (inclusive). The order of elements is not guaranteed. Returns NULL if either argument is NULL or `h3index` is not a valid H3 cell index.

## Examples

```sql
SELECT h3kRing(644325529233966508, 1);
+-----------------------------------------------------------------------------------------------+
| h3kRing(644325529233966508, 1)                                                                |
+-----------------------------------------------------------------------------------------------+
| [644325529233966354,644325529233966355,644325529233966497,644325529233966504,644325529233966508,644325529233966509,644325529233966510] |
+-----------------------------------------------------------------------------------------------+
```

## keyword

H3KRING,H3,SPATIAL
