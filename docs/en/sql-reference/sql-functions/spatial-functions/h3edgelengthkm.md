---
displayed_sidebar: docs
description: "Returns the average edge length of H3 cells at the given resolution, in kilometres."
---

# h3EdgeLengthKm

Returns the average edge length of H3 cells at the given resolution, expressed in kilometres.

## Syntax

```Haskell
DOUBLE h3EdgeLengthKm(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns a DOUBLE representing the average edge length in kilometres at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT h3EdgeLengthKm(15);
+---------------------+
| h3EdgeLengthKm(15)  |
+---------------------+
| 0.000509713         |
+---------------------+
```

## keyword

H3EDGELENGTHKM,H3,SPATIAL
