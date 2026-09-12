---
displayed_sidebar: docs
description: "Converts an H3 cell index from its BIGINT representation to a hexadecimal string."
---

# h3ToString

Converts an H3 cell index from its BIGINT integer representation to a lowercase hexadecimal string. This is the canonical string format used by the H3 library and external tools.

## Syntax

```Haskell
VARCHAR h3ToString(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns a VARCHAR containing the lowercase hexadecimal string representation of the H3 index. Returns NULL if the argument is NULL.

## Examples

```sql
SELECT h3ToString(617420388352917503);
+-----------------------------------+
| h3ToString(617420388352917503)    |
+-----------------------------------+
| 89184926cdbffff                   |
+-----------------------------------+
```

## keyword

H3TOSTRING,H3,SPATIAL
