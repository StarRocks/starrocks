# char_length

## Description

This function returns the length of a string. For multibytes characters, it returns the number of characters. It currently only supports utf8 coding. Note: This function is also named as character_length.

## Syntax

```Haskell
INT char_length(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select char_length("abc");
+--------------------+
| char_length('abc') |
+--------------------+
|                  3 |
+--------------------+

MySQL > select char_length("中国");
+----------------------+
| char_length('中国')  |
+----------------------+
|                    2 |
+----------------------+
```

## keyword

CHAR_LENGTH, CHARACTER_LENGTH
