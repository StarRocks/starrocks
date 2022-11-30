# right

## Description

This function returns a specified length of characters from the right side of a given string. Length unit: utf8 character.

## Syntax

```Haskell
VARCHAR right(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select right("Hello starrocks",5);
+-------------------------+
| right('Hello starrocks', 5) |
+-------------------------+
| starrocks                   |
+-------------------------+
```

## keyword

RIGHT
