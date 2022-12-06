# length

## Description

This function returns the length of a string (in bytes).

## Syntax

```Haskell
INT length(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select length("abc");
+---------------+
| length('abc') |
+---------------+
|             3 |
+---------------+

MySQL > select length("中国");
+------------------+
| length('中国')   |
+------------------+
|                6 |
+------------------+
```

## keyword

LENGTH
