---
displayed_sidebar: docs
---

# field

Returns the index (position) of str in the str1, str2, str3, ... list. Returns 0 if str is not found.

This function is supported from v3.5.

## Syntax

```Haskell
INT field(expr1, ...);
```

## Examples

```Plain Text
MYSQL > select field('a', 'b', 'a', 'd');
+---------------------------+
| field('a', 'b', 'a', 'd') |
+---------------------------+
|                         2 |
+---------------------------+
```

## keyword

FIELD
