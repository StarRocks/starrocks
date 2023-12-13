---
displayed_sidebar: "English"
---

# trim

## Description

Removes consecutive spaces or specified characters from the beginning and end of the `str` argument. Removing specified characters are supported from StarRocks 2.5.0.

## Syntax

```Haskell
VARCHAR trim(VARCHAR str[, VARCHAR characters])
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

`characters`: optional, the characters to remove, which must be a VARCHAR value. If this parameter is not specified, spaces are removed from the string by default. If this parameter is set to an empty string, an error is returned.

## Return value

Returns a VARCHAR value.

## Examples

Example 1: Remove the five spaces from the beginning and end of the string.

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
1 row in set (0.00 sec)
```

Example 2: Remove specified characters from the beginning and end of the string.

```Plain Text
MySQL > SELECT trim("abcd", "ad");
+--------------------+
| trim('abcd', 'ad') |
+--------------------+
| bc                 |
+--------------------+

MySQL > SELECT trim("xxabcdxx", "x");
+-----------------------+
| trim('xxabcdxx', 'x') |
+-----------------------+
| abcd                  |
+-----------------------+
```

## References

- [ltrim](ltrim.md)
- [rtrim](rtrim.md)
