# ltrim

## description

### Syntax

```Haskell
VARCHAR ltrim(VARCHAR str)
```

This function removes the leading spaces from the beginning of the str argument.

## example

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
