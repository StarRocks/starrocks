# null_or_empty

## description

### Syntax

```Haskell
BOOLEAN NULL_OR_EMPTY (VARCHAR str)
```

如果字符串为空字符串或者NULL，返回true。否则，返回false。

## example

```Plain Text
MySQL > select null_or_empty(null);
+---------------------+
| null_or_empty(NULL) |
+---------------------+
|                   1 |
+---------------------+

MySQL > select null_or_empty("");
+-------------------+
| null_or_empty('') |
+-------------------+
|                 1 |
+-------------------+

MySQL > select null_or_empty("a");
+--------------------+
| null_or_empty('a') |
+--------------------+
|                  0 |
+--------------------+
```

## keyword

NULL_OR_EMPTY
