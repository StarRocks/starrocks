# null_or_empty

## description

### Syntax

```Haskell
BOOLEAN NULL_OR_EMPTY (VARCHAR str)
```

This function returns true when the string is empty or NULL. Otherwise, it returns false.

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
