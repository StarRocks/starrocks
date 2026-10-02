---
displayed_sidebar: docs
description: "Returns the exact length of a directed H3 edge in metres."
---

# h3ExactEdgeLengthM

Returns the exact length of a directed H3 edge in metres. Unlike `h3EdgeLengthM`, which returns the average edge length for a resolution, this function computes the precise geodesic length of the individual edge.

## Syntax

```Haskell
DOUBLE h3ExactEdgeLengthM(BIGINT h3edge)
```

## Parameters

- `h3edge`: A directed H3 edge index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the exact length of the edge in metres. Returns NULL if the argument is NULL or is not a valid H3 directed edge index.

## Examples

```sql
SELECT h3ExactEdgeLengthM(1310277011704381439);
+-----------------------------------------+
| h3ExactEdgeLengthM(1310277011704381439) |
+-----------------------------------------+
| 195449.63163407316                      |
+-----------------------------------------+
```

## keyword

H3EXACTEDGELENGTHM,H3,SPATIAL
