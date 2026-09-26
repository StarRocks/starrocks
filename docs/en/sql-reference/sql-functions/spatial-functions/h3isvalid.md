---
displayed_sidebar: docs
description: "Returns true if a BIGINT value is a valid H3 cell index, false otherwise."
---

# h3IsValid

Returns whether a given BIGINT value is a valid H3 cell index. This function never returns NULL for non-NULL input — invalid or unrecognized values return `false`.

## Syntax

```Haskell
BOOLEAN h3IsValid(BIGINT h3index)
```

## Parameters

- `h3index`: A value to test as an H3 cell index. Supported data type: BIGINT.

## Return value

Returns a BOOLEAN: `true` (1) if the value is a valid H3 cell index, `false` (0) otherwise. Returns NULL only if the argument is NULL.

## Examples

Example 1: Valid H3 cell index.

```sql
SELECT h3IsValid(617700169958293503);
+-------------------------------+
| h3IsValid(617700169958293503) |
+-------------------------------+
|                             1 |
+-------------------------------+
```

Example 2: Invalid value.

```sql
SELECT h3IsValid(0);
+--------------+
| h3IsValid(0) |
+--------------+
|            0 |
+--------------+
```

## keyword

H3ISVALID,H3,SPATIAL,VALID
