---
displayed_sidebar: docs
---

# upper_utf8



Converts all utf8-encoded strings to uppercase. If the input is not a valid utf8-encoded text, undefined behavior may occur.

## Syntax

```Haskell
INT upper_utf8(VARCHAR str)
```

## Examples

```Plain Text
mysql> select upper_utf8('Hallo, München');
+-------------------------------+
| upper_utf8('Hallo, München')  |
+-------------------------------+
| HALLO, MÜNCHEN                |
+-------------------------------+
```

## keyword

UPPER_UTF8
