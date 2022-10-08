# weekofyear

## description

### Syntax

```Haskell
INT WEEKOFYEAR(DATETIME date)
```

获得一年中的第几周

参数为Date或者Datetime类型

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
