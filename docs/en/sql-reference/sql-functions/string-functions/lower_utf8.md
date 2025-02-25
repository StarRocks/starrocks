---
displayed_sidebar: docs
---

# lower_utf8



Converts all utf8-encoded strings to lowercase. If the input is not a valid utf8-encoded text, undefined behavior may occur.

## Syntax

```Haskell
INT lower_utf8(VARCHAR str)
```

## Examples

```Plain Text
mysql> select lower_utf8('Hallo, München');
+-------------------------------+
| lower_utf8('Hallo, München')  |
+-------------------------------+
| hallo, münchen                |
+-------------------------------+
```

## keyword

LOWER_UTF8
