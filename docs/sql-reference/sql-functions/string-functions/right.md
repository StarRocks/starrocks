# right

## description

### Syntax

```Haskell
VARCHAR right(VARCHAR str)
```

This function returns a specified length of characters from the right side of a given string. Length unit: utf8 character.

## example

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
