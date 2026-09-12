---
displayed_sidebar: docs
description: "Returns the exact length of a directed H3 edge in radians."
---

# h3ExactEdgeLengthRads

Returns the exact length of a directed H3 edge in radians (great-circle arc length on the unit sphere).

## Syntax

```Haskell
DOUBLE h3ExactEdgeLengthRads(BIGINT h3edge)
```

## Parameters

- `h3edge`: A directed H3 edge index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the exact length of the edge in radians. Returns NULL if the argument is NULL or is not a valid H3 directed edge index.

## Examples

```sql
SELECT h3ExactEdgeLengthRads(1310277011704381439);
+--------------------------------------------+
| h3ExactEdgeLengthRads(1310277011704381439) |
+--------------------------------------------+
| 0.030677980118976447                       |
+--------------------------------------------+
```

## keyword

H3EXACTEDGELENGTHRADS,H3,SPATIAL
