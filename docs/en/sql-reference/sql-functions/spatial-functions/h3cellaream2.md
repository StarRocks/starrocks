---
displayed_sidebar: docs
description: "Returns the exact area of a specific H3 cell in square metres."
---

# h3CellAreaM2

Returns the exact area of the specified H3 cell in square metres. Unlike `h3HexAreaM2`, which returns the average area for all cells at a given resolution, this function computes the precise geodesic area of the individual cell.

## Syntax

```Haskell
DOUBLE h3CellAreaM2(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the exact area of the cell in square metres. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3CellAreaM2(579205133326352383);
+-----------------------------------+
| h3CellAreaM2(579205133326352383)  |
+-----------------------------------+
| 4106166334463.9233                |
+-----------------------------------+
```

## keyword

H3CELLAREAM2,H3,SPATIAL
