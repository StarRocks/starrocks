# lpad

## Description

This function returns strings with a length of `len` (starting counting from the first syllable) in `str`. If `len` is longer than `str`, the return value is lengthened to `len` characters by adding pad characters in front of `str`.  If `str` is longer than `len`, the return value is shortened to `len` characters. `len` means the length of characters, not bytes.

## Syntax

```Haskell
VARCHAR lpad(VARCHAR str, INT len, VARCHAR pad)
```

## Examples

```Plain Text
MySQL > SELECT lpad("hi", 5, "xy");
+---------------------+
| lpad('hi', 5, 'xy') |
+---------------------+
| xyxhi               |
+---------------------+

MySQL > SELECT lpad("hi", 1, "xy");
+---------------------+
| lpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+
```

## keyword

LPAD
