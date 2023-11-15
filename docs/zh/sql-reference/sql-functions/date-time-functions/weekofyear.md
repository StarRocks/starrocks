# weekofyear

## 功能

计算指定日期为对应年中的第几周。

### Syntax

```Haskell
INT WEEKOFYEAR(DATETIME date)
```

## 参数说明

参数为 DATE 或者 DATETIME 类型。

## 返回值说明

 返回 INT 类型的值。

## example

```Plain Text
select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## keyword

WEEKOFYEAR
