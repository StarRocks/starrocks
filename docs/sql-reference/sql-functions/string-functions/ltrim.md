# ltrim

## Description

Removes the leading spaces from the beginning of the `str` argument.

## Syntax

```Haskell
VARCHAR ltrim(VARCHAR str)
```

## Examples

```Plain Text
MySQL > SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```

## keyword

LTRIM
