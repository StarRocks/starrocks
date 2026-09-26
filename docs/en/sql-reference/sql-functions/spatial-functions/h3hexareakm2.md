---
displayed_sidebar: docs
description: "Returns the average area of H3 cells at the given resolution, in square kilometres."
---

# h3HexAreaKm2

Returns the average area of H3 cells at the given resolution, expressed in square kilometres.

## Syntax

```Haskell
DOUBLE h3HexAreaKm2(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns a DOUBLE representing the average cell area in square kilometres at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT h3HexAreaKm2(13);
+-------------------+
| h3HexAreaKm2(13)  |
+-------------------+
| 0.0000439         |
+-------------------+
```

## keyword

H3HEXAREAKM2,H3,SPATIAL
