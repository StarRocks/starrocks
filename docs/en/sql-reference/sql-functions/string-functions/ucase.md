---
displayed_sidebar: docs
description: "Converts a string to uppercase; alias of upper."
products: [oss, enterprise, cloud, anywhere]
---

# ucase



This function converts a string to upper-case. It is analogous to the function upper.

## Syntax

```Haskell
VARCHAR ucase(VARCHAR str)
```

## Examples

```Plain Text
mysql> SELECT ucase("AbC123");
+-----------------+
|ucase('AbC123')  |
+-----------------+
|ABC123           |
+-----------------+
```

## keyword

UCASE
