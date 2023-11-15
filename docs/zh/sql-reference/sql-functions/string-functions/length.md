# length

## description

### Syntax

```Haskell
INT length(VARCHAR str)
```

返回字符串的**字节**长度。

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
