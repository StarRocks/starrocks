---
displayed_sidebar: docs
---

# weekofyear

## 功能

计算指定时间为一年中的第几周。

## 语法

```Haskell
INT WEEKOFYEAR(DATETIME date)
```

## 参数说明

参数为 DATE 或者 DATETIME 类型。

## 返回值说明

 返回 INT 类型的值。

## 示例

```Plain Text
select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```
