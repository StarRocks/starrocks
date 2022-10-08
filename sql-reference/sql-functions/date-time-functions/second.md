# second

## description

### Syntax

```Haskell
INT SECOND(DATETIME date)
```

获得日期中的秒的信息，返回值范围从0-59。

参数为Date或者Datetime类型

## example

```Plain Text
select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

SECOND
