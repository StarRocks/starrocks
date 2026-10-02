---
displayed_sidebar: docs
description: "Returns the resolution (0–15) of an H3 cell index."
---

# h3GetResolution

Returns the resolution of the given H3 cell index. H3 resolutions range from 0 (coarsest, ~4,250 km edge length) to 15 (finest, ~0.5 m edge length).

## Syntax

```Haskell
INT h3GetResolution(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns an INT in the range [0, 15] representing the resolution of the cell. Returns NULL if the argument is NULL or if the index is not a valid H3 cell.

## Examples

```sql
SELECT h3GetResolution(617700169958293503);
+-------------------------------------+
| h3GetResolution(617700169958293503) |
+-------------------------------------+
|                                   9 |
+-------------------------------------+
```

## keyword

H3GETRESOLUTION,H3,SPATIAL,RESOLUTION
