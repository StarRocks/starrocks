---
displayed_sidebar: docs
description: "Returns the average edge length of H3 cells at the given resolution, in degrees."
---

# h3EdgeAngle

Returns the average edge length of H3 cells at the given resolution, expressed in degrees.

## Syntax

```Haskell
DOUBLE h3EdgeAngle(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns a DOUBLE representing the average edge length in degrees at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT h3EdgeAngle(10);
+------------------------+
| h3EdgeAngle(10)        |
+------------------------+
| 0.0005927224846720883  |
+------------------------+
```

## keyword

H3EDGEANGLE,H3,SPATIAL
