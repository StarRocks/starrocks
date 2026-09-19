---
displayed_sidebar: docs
description: "Returns the average edge length of H3 cells at the given resolution, in metres."
---

# h3EdgeLengthM

Returns the average edge length of H3 cells at the given resolution, expressed in metres.

## Syntax

```Haskell
DOUBLE h3EdgeLengthM(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns a DOUBLE representing the average edge length in metres at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT h3EdgeLengthM(15);
+--------------------+
| h3EdgeLengthM(15)  |
+--------------------+
| 0.509713273        |
+--------------------+
```

## keyword

H3EDGELENGTHM,H3,SPATIAL
