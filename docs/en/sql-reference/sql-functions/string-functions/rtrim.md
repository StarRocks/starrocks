# rtrim

## Description

Removes the trailing spaces from the end (right) of the `str` argument.

## Syntax

```Haskell
VARCHAR rtrim(VARCHAR str);
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

## Return value

Returns a VARCHAR value.

## Examples

Remove the three trailing spaces from the string.

```Plain Text
mysql> SELECT rtrim('   ab d   ');
+---------------------+
| rtrim('   ab d   ') |
+---------------------+
|    ab d             |
+---------------------+
1 row in set (0.00 sec)
```

## References

- [trim](trim.md)
- [ltrim](ltrim.md)
