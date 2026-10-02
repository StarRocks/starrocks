---
displayed_sidebar: docs
description: "Returns the average area of H3 cells at the given resolution, in square metres."
---

# h3HexAreaM2

Returns the average area of H3 cells at the given resolution, expressed in square metres.

## Syntax

```Haskell
DOUBLE h3HexAreaM2(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns a DOUBLE representing the average cell area in square metres at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT h3HexAreaM2(13);
+------------------+
| h3HexAreaM2(13)  |
+------------------+
| 43.9             |
+------------------+
```

## keyword

H3HEXAREAM2,H3,SPATIAL
