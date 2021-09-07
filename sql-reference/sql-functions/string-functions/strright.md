# strright

## description

### Syntax

```Haskell
VARCHAR strright(VARCHAR str,INT len)
```

它返回具有指定长度的字符串的右边部分, 长度的单位为「utf8字符」

## example

```Plain Text
MySQL > select strright("Hello starrocks",5);
+--------------------------+
|strright('Hello starrocks', 5)|
+--------------------------+
| starrocks                    |
+--------------------------+
```

## keyword

STRRIGHT
