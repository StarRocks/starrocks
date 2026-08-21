---
displayed_sidebar: docs
description: "Returns the origin H3 cell index of a directed (unidirectional) edge."
---

# h3GetOriginIndexFromUnidirectionalEdge

Returns the origin H3 cell index from a directed (unidirectional) edge. A directed edge connects two neighbouring cells, and the origin is the cell from which the edge departs.

## Syntax

```Haskell
BIGINT h3GetOriginIndexFromUnidirectionalEdge(BIGINT edge)
```

## Parameters

- `edge`: A valid H3 directed edge index. Supported data type: BIGINT.

## Return value

Returns a BIGINT representing the origin cell index of the directed edge. Returns NULL if the argument is NULL or is not a valid H3 directed edge index.

## Examples

```sql
SELECT h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197);
+-------------------------------------------------------------+
| h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197) |
+-------------------------------------------------------------+
| 599686042433355773                                          |
+-------------------------------------------------------------+
```

## keyword

H3GETORIGININDEXFROMUNIDIRECTIONALEDGE,H3,SPATIAL
