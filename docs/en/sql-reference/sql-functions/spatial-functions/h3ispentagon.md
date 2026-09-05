---
displayed_sidebar: docs
description: "Returns 1 if the H3 cell index represents a pentagon cell."
---

# h3IsPentagon

Returns 1 if the given H3 cell index represents a pentagon. The H3 grid contains 12 pentagons per resolution level (one per icosahedron vertex); all other cells are hexagons.

## Syntax

```Haskell
BOOLEAN h3IsPentagon(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns 1 (true) if the cell is a pentagon, or 0 (false) if it is a hexagon. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3IsPentagon(644721767722457330);
+-----------------------------------+
| h3IsPentagon(644721767722457330)  |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

## keyword

H3ISPENTAGON,H3,SPATIAL
