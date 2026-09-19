---
displayed_sidebar: docs
description: "Returns the number of unique H3 cells at the given resolution."
---

# h3NumHexagons

Returns the total number of unique H3 cells at the given resolution. H3 uses a hierarchical grid with 122 base cells at resolution 0 and approximately 7× more cells at each successive finer resolution.

## Syntax

```Haskell
BIGINT h3NumHexagons(INT resolution)
```

## Parameters

- `resolution`: H3 resolution level, between 0 and 15 inclusive. Supported data type: INT.

## Return value

Returns a BIGINT representing the total number of unique H3 cells at the specified resolution. Returns NULL if the argument is NULL or out of the valid range.

## Examples

```sql
SELECT h3NumHexagons(3);
+--------------------+
| h3NumHexagons(3)   |
+--------------------+
| 41162              |
+--------------------+
```

## keyword

H3NUMHEXAGONS,H3,SPATIAL
