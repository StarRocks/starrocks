# right

## description

### Syntax

```Haskell
VARCHAR right(VARCHAR str)
```

它返回具有指定长度的字符串的右边部分, 长度的单位为「utf8字符」

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
