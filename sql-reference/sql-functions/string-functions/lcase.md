# lcase

## description

### Syntax

```Haskell
VARCHAR lcase(VARCHAR str)
```

该函数与lower一致，将参数字符串转换为小写形式

## example

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```

## keyword

LCASE
