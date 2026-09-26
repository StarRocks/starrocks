---
displayed_sidebar: docs
description: "Returns 1 if the H3 cell's resolution is Class III (odd resolutions 1,3,5,7,9,11,13,15)."
---

# h3IsResClassIII

Returns 1 if the resolution of the given H3 cell is Class III. H3 resolutions are alternately Class II (even: 0,2,4,6,8,10,12,14) and Class III (odd: 1,3,5,7,9,11,13,15). Class III grids are rotated 19.1° relative to Class II grids.

## Syntax

```Haskell
BOOLEAN h3IsResClassIII(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns 1 (true) if the cell is at a Class III resolution, or 0 (false) if it is at a Class II resolution. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3IsResClassIII(617420388352917503);
+--------------------------------------+
| h3IsResClassIII(617420388352917503)  |
+--------------------------------------+
|                                    1 |
+--------------------------------------+
```

## keyword

H3ISRESCLASSIII,H3,SPATIAL
