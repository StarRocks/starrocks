---
displayed_sidebar: "English"
---

# rtrim

## Description

Removes the trailing spaces or specified characters from the end (right) of the `str` argument. Removing specified characters are supported from StarRocks 2.5.0.

## Syntax

```Haskell
VARCHAR rtrim(VARCHAR str[, VARCHAR characters]);
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

`characters`: optional, the characters to remove, which must be a VARCHAR value. If this parameter is not specified, spaces are removed from the string by default. If this parameter is set to an empty string, an error is returned.

## Return value

Returns a VARCHAR value.

## Examples

Example 1: Remove the three trailing spaces from the string.

```Plain Text
mysql> SELECT rtrim('   ab d   ');
+---------------------+
| rtrim('   ab d   ') |
+---------------------+
|    ab d             |
+---------------------+
1 row in set (0.00 sec)
```

Example 2: Remove specified characters from the end of the string.

```Plain Text
MySQL > SELECT rtrim("xxabcdxx", "x");
+------------------------+
| rtrim('xxabcdxx', 'x') |
+------------------------+
| xxabcd                 |
+------------------------+
```

## References

- [trim](trim.md)
- [ltrim](ltrim.md)
