# ltrim

## Description

Removes the leading spaces or specified characters from the beginning (left) of the `str` argument. Removing specified characters are supported from StarRocks 2.5.0.

## Syntax

```Haskell
VARCHAR ltrim(VARCHAR str[, VARCHAR characters])
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

`characters`: optional, the characters to remove, which must be a VARCHAR value. If this parameter is not specified, spaces are removed from the string by default. If this parameter is set to an empty string, an error is returned.

## Return value

Returns a VARCHAR value.

## Examples

Example 1: Remove spaces from the beginning of the string.

```Plain Text
MySQL > SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```

Example 2: Remove specified characters from the beginning of the string.

```Plain Text
MySQL > SELECT ltrim("xxabcdxx", "x");
+------------------------+
| ltrim('xxabcdxx', 'x') |
+------------------------+
| abcdxx                 |
+------------------------+
```

## References

- [trim](trim.md)
- [rtrim](rtrim.md)

## keyword

LTRIM
