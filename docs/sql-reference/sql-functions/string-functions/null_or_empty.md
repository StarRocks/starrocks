---
displayed_sidebar: "English"
---

# null_or_empty

## Description

This function returns true when the string is empty or NULL. Otherwise, it returns false.

## Syntax

```Haskell
BOOLEAN NULL_OR_EMPTY (VARCHAR str)
```

## Examples

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
