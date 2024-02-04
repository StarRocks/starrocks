---
displayed_sidebar: "Chinese"
---

# cbrt

## 功能

计算参数的立方根。

## 语法

```Haskell
DOUBLE cbrt(DOUBLE arg)
```

## 参数说明

`arg`:该函数在计算参数的立方根之前，会将数值转换为 DOUBLE 类型。

## 返回值说明

返回 DOUBLE 类型的值。如果参数为NULL，则返回 `NULL`。

## 示例

```Plain
mysql> select cbrt(8);
+---------+
| cbrt(8) |
+---------+
|       2 |
+---------+

mysql> select cbrt(-8);
+----------+
| cbrt(-8) |
+----------+
|       -2 |
+----------+

mysql> select cbrt(0);
+---------+
| cbrt(0) |
+---------+
|       0 |
+---------+

mysql> select cbrt("");
+----------+
| cbrt('') |
+----------+
|     NULL |
+----------+
```

## keywords

cbrt, cube root