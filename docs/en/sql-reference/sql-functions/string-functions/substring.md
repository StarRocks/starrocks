---
displayed_sidebar: docs
---

# substring, substr

<<<<<<< HEAD
## Description

Extracts characters from a string and returns a substring.

If `len` is not specified, this function extracts characters from the position specified by `pos`. If `len` is specified, this function extracts `len` characters from the position specified by `pos`.

`pos` can be a negative integer. In this case, this function extracts characters starting from the end of the string.
=======


Extracts characters staring from the specified position and returns a substring of specified length.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

## Syntax

```Haskell
VARCHAR substr(VARCHAR str, pos[, len])
```

## Parameters

<<<<<<< HEAD
- `str`: the string to extract characters, required. It must be a VARCHAR value.
- `pos`: the start position, required. The first position in the string is 1.
- `length`: the number of characters to extract, optional. It must be a positive integer.
=======
- `str`: required, the string to extract characters. It must be a VARCHAR value.
- `pos`: required, an integer that specifies the starting position. Note that the first character in a string is 1, not 0.
  - If `pos` is 0, an empty string is returned.
  - `pos` can be a negative integer. In this case, this function extracts characters starting from the end of the string. See Example 2.
  - If the position specified by `pos` exceeds the range of the string, an empty string is returned. See Example 3.
- `len`: optional, a positive integer that specifies the number of characters to extract.
  - If `len` is specified, this function extracts `len` characters starting from the position specified by `pos`.
  - If `len` is not specified, this function extracts all the characters starting at `pos`. See Example 1.
  - If `len` exceeds the actual length of the matching characters, all the matching characters are returned. See Example 4.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

## Return value

Returns a value of the VARCHAR type.

<<<<<<< HEAD
If the number of characters (`len`) to return exceeds the actual length of the matching characters, all the matching characters are returned.

If the position specified by `pos` exceeds the range of the string, an empty string is returned.

## Examples

```Plain Text
MySQL > select substring("starrockscluster", 1, 9);
+-------------------------------------+
| substring('starrockscluster', 1, 9) |
=======
## Examples

```Plain Text
-- Extract all characters starting from the first character "s".
MySQL > select substring("starrockscluster", 1);
+-------------------------------------+
| substring('starrockscluster', 1) |
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
+-------------------------------------+
| starrocks                           |
+-------------------------------------+

<<<<<<< HEAD
=======
-- The position is negative and the counting is from the end of the string.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
MySQL > select substring("starrocks", -5, 5);
+-------------------------------+
| substring('starrocks', -5, 5) |
+-------------------------------+
| rocks                         |
+-------------------------------+

<<<<<<< HEAD
=======
-- The position exceeds the length of the string and an empty string is returned.
MySQL > select substring("apple", 8, 2);
+--------------------------------+
| substring('apple', 8, 2)       |
+--------------------------------+
|                                |
+--------------------------------+

-- There are 5 matching characters. The length 9 exceeds the length of the matching characters and all the matching characters are returned.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
MySQL > select substring("apple", 1, 9);
+--------------------------+
| substring('apple', 1, 9) |
+--------------------------+
| apple                    |
+--------------------------+
```

## keyword

substring,string,sub
