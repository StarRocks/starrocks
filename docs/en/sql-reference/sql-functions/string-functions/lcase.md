---
displayed_sidebar: docs
description: "Converts a string to lowercase; alias of lower."
---

# lcase



This function converts a string to lower-case. It is analogous to the function lower.

## Syntax

```Haskell
VARCHAR lcase(VARCHAR str)
```

## Examples

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```

## keyword

LCASE
