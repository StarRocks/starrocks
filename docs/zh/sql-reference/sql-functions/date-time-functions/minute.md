# minute

## description

### Syntax

```Haskell
INT MINUTE(DATETIME date)
```

获得日期中的分钟的信息，返回值范围从0-59。

参数为Date或者Datetime类型

## example

```Plain Text
select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

MINUTE
