# length

## description

### Syntax

```Haskell
INT length(VARCHAR str)
```

This function returns the length of a string (in bytes).

## example

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
