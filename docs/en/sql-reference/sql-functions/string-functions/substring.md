---
displayed_sidebar: "English"
---

# substring, substr

## Description

Extracts characters from a string and returns a substring.

If `len` is not specified, this function extracts characters from the position specified by `pos`. If `len` is specified, this function extracts `len` characters from the position specified by `pos`.

`pos` can be a negative integer. In this case, this function extracts characters starting from the end of the string.

## Syntax

```Haskell
VARCHAR substr(VARCHAR str, pos[, len])
```

## Parameters

- `str`: the string to extract characters, required. It must be a VARCHAR value.
- `pos`: the start position, required. The first position in the string is 1.
- `length`: the number of characters to extract, optional. It must be a positive integer.

## Return value

Returns a value of the VARCHAR type.

If the number of characters (`len`) to return exceeds the actual length of the matching characters, all the matching characters are returned.

If the position specified by `pos` exceeds the range of the string, an empty string is returned.

## Examples

```Plain Text
MySQL > select substring("starrockscluster", 1, 9);
+-------------------------------------+
| substring('starrockscluster', 1, 9) |
+-------------------------------------+
| starrocks                           |
+-------------------------------------+

MySQL > select substring("starrocks", -5, 5);
+-------------------------------+
| substring('starrocks', -5, 5) |
+-------------------------------+
| rocks                         |
+-------------------------------+

MySQL > select substring("apple", 1, 9);
+--------------------------+
| substring('apple', 1, 9) |
+--------------------------+
| apple                    |
+--------------------------+
```

## keyword

substring,string,sub
