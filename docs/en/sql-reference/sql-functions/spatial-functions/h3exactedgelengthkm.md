---
displayed_sidebar: docs
description: "Returns the exact length of a directed H3 edge in kilometres."
---

# h3ExactEdgeLengthKm

Returns the exact length of a directed H3 edge in kilometres. Unlike `h3EdgeLengthKm`, which returns the average edge length for a resolution, this function computes the precise geodesic length of the individual edge.

## Syntax

```Haskell
DOUBLE h3ExactEdgeLengthKm(BIGINT h3edge)
```

## Parameters

- `h3edge`: A directed H3 edge index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the exact length of the edge in kilometres. Returns NULL if the argument is NULL or is not a valid H3 directed edge index.

## Examples

```sql
SELECT h3ExactEdgeLengthKm(1310277011704381439);
+------------------------------------------+
| h3ExactEdgeLengthKm(1310277011704381439) |
+------------------------------------------+
| 195.44963163407317                       |
+------------------------------------------+
```

## keyword

H3EXACTEDGELENGTHKM,H3,SPATIAL
