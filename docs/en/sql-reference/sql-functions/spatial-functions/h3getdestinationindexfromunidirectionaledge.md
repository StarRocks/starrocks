---
displayed_sidebar: docs
description: "Returns the destination H3 cell index of a directed (unidirectional) edge."
---

# h3GetDestinationIndexFromUnidirectionalEdge

Returns the destination H3 cell index from a directed (unidirectional) edge. A directed edge connects two neighbouring cells, and the destination is the cell toward which the edge points.

## Syntax

```Haskell
BIGINT h3GetDestinationIndexFromUnidirectionalEdge(BIGINT edge)
```

## Parameters

- `edge`: A valid H3 directed edge index. Supported data type: BIGINT.

## Return value

Returns a BIGINT representing the destination cell index of the directed edge. Returns NULL if the argument is NULL or is not a valid H3 directed edge index.

## Examples

```sql
SELECT h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197);
+------------------------------------------------------------------+
| h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197) |
+------------------------------------------------------------------+
| 599686043507097597                                               |
+------------------------------------------------------------------+
```

## keyword

H3GETDESTINATIONINDEXFROMUNIDIRECTIONALEDGE,H3,SPATIAL
