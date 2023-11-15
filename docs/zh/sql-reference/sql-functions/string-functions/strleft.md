# strleft

## description

### Syntax

```Haskell
VARCHAR strleft(VARCHAR str,INT len)
```

它返回具有指定长度的字符串的左边部分,长度的单位为「utf8字符」

## example

```Plain Text
MySQL > select strleft("Hello starrocks",5);
+-------------------------+
|strleft('Hello starrocks', 5)|
+-------------------------+
| Hello                   |
+-------------------------+
```

## keyword

STRLEFT
