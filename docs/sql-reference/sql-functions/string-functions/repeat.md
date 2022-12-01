# repeat

## Description

This function repeats str by a number of times according to count. When count is below 1, it returns an empty string. When str or count is NULL, it returns NULL.

## Syntax

```Haskell
VARCHAR repeat(VARCHAR str, INT count)
```

## Examples

```Plain Text
MySQL > SELECT repeat("a", 3);
+----------------+
| repeat('a', 3) |
+----------------+
| aaa            |
+----------------+

MySQL > SELECT repeat("a", -1);
+-----------------+
| repeat('a', -1) |
+-----------------+
|                 |
+-----------------+
```

## keyword

REPEAT,
