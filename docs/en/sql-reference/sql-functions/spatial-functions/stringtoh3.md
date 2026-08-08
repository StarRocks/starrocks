---
displayed_sidebar: docs
description: "Parses a hexadecimal H3 string and returns the corresponding BIGINT index."
---

# stringToH3

Parses a hexadecimal H3 string and returns the corresponding BIGINT index. This is the inverse of `h3ToString`. The input must be a valid H3 hexadecimal string (without a `0x` prefix).

## Syntax

```Haskell
BIGINT stringToH3(VARCHAR h3string)
```

## Parameters

- `h3string`: A hexadecimal H3 index string. Supported data type: VARCHAR.

## Return value

Returns a BIGINT representing the H3 cell index. Returns NULL if the input is NULL or is not a valid H3 hexadecimal string.

## Examples

```sql
SELECT stringToH3('89184926cc3ffff');
+--------------------------------+
| stringToH3('89184926cc3ffff') |
+--------------------------------+
| 617420388351344639             |
+--------------------------------+
```

## keyword

STRINGTOH3,H3,SPATIAL
