# left

## Description

This function returns a specified number of characters from the left side of a given string. The unit for length: utf8 character.

## Syntax

```Haskell
VARCHAR left(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select left("Hello starrocks",5);
+------------------------+
| left('Hello starrocks', 5) |
+------------------------+
| Hello                  |
+------------------------+
```

## keyword

LEFT
