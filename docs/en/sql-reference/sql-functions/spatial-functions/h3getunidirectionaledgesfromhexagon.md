---
displayed_sidebar: docs
description: "Returns all directed H3 edge indexes originating from the given cell."
---

# h3GetUnidirectionalEdgesFromHexagon

Returns all directed (unidirectional) H3 edge indexes that originate from the given cell. A hexagon has 6 edges; a pentagon has 5.

## Syntax

```Haskell
ARRAY<BIGINT> h3GetUnidirectionalEdgesFromHexagon(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns an `ARRAY<BIGINT>` of directed edge indexes originating from the cell. The array contains 6 elements for hexagons and 5 for pentagons. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3GetUnidirectionalEdgesFromHexagon(599686042433355775);
+-------------------------------------------------------------------------------------------+
| h3GetUnidirectionalEdgesFromHexagon(599686042433355775)                                   |
+-------------------------------------------------------------------------------------------+
| [1248204388774707199,1320261982812635135,1392319576850563071,1464377170888491007,1536434764926418943,1608492358964346879] |
+-------------------------------------------------------------------------------------------+
```

## keyword

H3GETUNIDIRECTIONALEDGESFROMHEXAGON,H3,SPATIAL
