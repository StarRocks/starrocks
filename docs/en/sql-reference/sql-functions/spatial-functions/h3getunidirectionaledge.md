---
displayed_sidebar: docs
description: "Returns the directed H3 edge index between two neighboring cells."
---

# h3GetUnidirectionalEdge

Returns the directed (unidirectional) H3 edge index that represents the boundary from the origin cell to the destination cell. The two cells must be direct neighbors at the same resolution.

## Syntax

```Haskell
BIGINT h3GetUnidirectionalEdge(BIGINT origin, BIGINT destination)
```

## Parameters

- `origin`: The origin H3 cell index. Supported data type: BIGINT.
- `destination`: The destination H3 cell index. Supported data type: BIGINT.

## Return value

Returns a BIGINT representing the directed edge index from the origin to the destination cell. Returns NULL if either argument is NULL, if the cells are not valid H3 cell indexes, or if the cells are not neighbors.

## Examples

```sql
SELECT h3GetUnidirectionalEdge(599686042433355775, 599686043507097599);
+-----------------------------------------------------------------+
| h3GetUnidirectionalEdge(599686042433355775, 599686043507097599) |
+-----------------------------------------------------------------+
| 1248204388774707199                                             |
+-----------------------------------------------------------------+
```

## keyword

H3GETUNIDIRECTIONALEDGE,H3,SPATIAL
