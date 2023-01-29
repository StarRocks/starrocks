# trim

## Description

Removes consecutive spaces from the beginning and end of the `str` argument.

## Syntax

```Haskell
VARCHAR trim(VARCHAR str)
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

## Return value

Returns a VARCHAR value.

## Examples

Remove the five spaces from the beginning and end of the string.

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
1 row in set (0.00 sec)
```

## References

- [ltrim](ltrim.md)
- [rtrim](rtrim.md)
