---
displayed_sidebar: docs
---

# lpad

## Description

This function returns strings with a length of `len` (starting counting from the first syllable) in `str`. If `len` is longer than `str`, the return value is lengthened to `len` characters by adding pad characters in front of `str`.  If `str` is longer than `len`, the return value is shortened to `len` characters. `len` means the length of characters, not bytes.

## Syntax

```Haskell
VARCHAR lpad(VARCHAR str, INT len[, VARCHAR pad])
```

## Parameters

`str`: required, the string to be padded, which must evaluate to a VARCHAR value.

`len`: required, the length of return value, it means the length of characters, not bytes, which must evaluate to an INT value.

`pad`: optional, the characters to be added in front of str, which must be a VARCHAR value. If this parameter is not specified, spaces are added by default.

## Return value

Returns a VARCHAR value.

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

MySQL > SELECT lpad("hi", 5);
+---------------------+
| lpad('hi', 5, ' ')  |
+---------------------+
|    hi               |
+---------------------+
```

## keyword

LPAD
