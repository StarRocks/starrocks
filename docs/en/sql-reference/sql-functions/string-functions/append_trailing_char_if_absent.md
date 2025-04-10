---
displayed_sidebar: docs
---

# append_trailing_char_if_absent

## Description

If the str string is not empty and does not contain trailing_char character in the end, it appends trailing_char character to the end. trailing_char can only contain one character. If it contains multiple characters, this function will return NULL.

## Syntax

```Haskell
VARCHAR append_trailing_char_if_absent(VARCHAR str, VARCHAR trailing_char)
```

## Examples

```Plain Text
MySQL [test]> select append_trailing_char_if_absent('a','c');
+------------------------------------------+
|append_trailing_char_if_absent('a', 'c')  |
+------------------------------------------+
| ac                                       |
+------------------------------------------+
1 row in set (0.02 sec)

MySQL [test]> select append_trailing_char_if_absent('ac','c');
+-------------------------------------------+
|append_trailing_char_if_absent('ac', 'c')  |
+-------------------------------------------+
| ac                                        |
+-------------------------------------------+
1 row in set (0.00 sec)
```

## keyword

APPEND_TRAILING_CHAR_IF_ABSENT
