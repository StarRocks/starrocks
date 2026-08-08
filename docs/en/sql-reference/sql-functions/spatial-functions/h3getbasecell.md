---
displayed_sidebar: docs
description: "Returns the base cell number (0–121) of an H3 cell index."
---

# h3GetBaseCell

Returns the base cell number of the given H3 cell index. Every H3 cell belongs to one of 122 base cells (numbered 0–121) which form the resolution-0 layer of the H3 hierarchy.

## Syntax

```Haskell
INT h3GetBaseCell(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns an INT in the range [0, 121] representing the base cell number. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3GetBaseCell(612916788725809151);
+-----------------------------------+
| h3GetBaseCell(612916788725809151) |
+-----------------------------------+
|                                12 |
+-----------------------------------+
```

## keyword

H3GETBASECELL,H3,SPATIAL
