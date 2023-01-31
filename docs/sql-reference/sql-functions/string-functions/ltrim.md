# ltrim

## Description

Removes the leading spaces from the beginning (left) of the `str` argument.

## Syntax

```Haskell
VARCHAR ltrim(VARCHAR str)
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

## Return value

Returns a VARCHAR value.

## Examples

Remove spaces from the beginning of the string.

```Plain Text
MySQL > SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```

## References

- [trim](trim.md)
- [rtrim](rtrim.md)

## keyword

LTRIM
