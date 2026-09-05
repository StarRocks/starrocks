---
displayed_sidebar: docs
description: "Returns the exact area of a specific H3 cell in steradians."
---

# h3CellAreaRads2

Returns the exact area of the specified H3 cell in steradians (solid angle measure). This is the geodesic area of the cell projected onto the unit sphere.

## Syntax

```Haskell
DOUBLE h3CellAreaRads2(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the exact area of the cell in steradians. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3CellAreaRads2(579205133326352383);
+--------------------------------------+
| h3CellAreaRads2(579205133326352383)  |
+--------------------------------------+
| 0.10116268528089567                  |
+--------------------------------------+
```

## keyword

H3CELLAREARADS2,H3,SPATIAL
