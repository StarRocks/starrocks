# ends_with

## description

### Syntax

```Haskell
BOOLEAN ENDS_WITH (VARCHAR str, VARCHAR suffix)
```

如果字符串以指定后缀结尾，返回true。否则，返回false。任意参数为NULL，返回NULL。

## example

```Plain Text
MySQL > select ends_with("Hello starrocks", "starrocks");
+-----------------------------------+
| ends_with('Hello starrocks', 'starrocks') |
+-----------------------------------+
|                                 1 |
+-----------------------------------+

MySQL > select ends_with("Hello starrocks", "Hello");
+-----------------------------------+
| ends_with('Hello starrocks', 'Hello') |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

## keyword

ENDS_WITH
