---
displayed_sidebar: "Chinese"
---

# cot

## 功能

求参数 `x` 的余切值，`x` 是弧度。如果输入值为 `0` 或者 `NULL`，则返回 `NULL`。

## 语法

```Haskell
COT(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select cot(3.1415926/2);
+------------------------+
| cot(3.1415926 / 2)     |
+------------------------+
| 2.6794896585028646e-08 |
+------------------------+
1 row in set (0.25 sec)

mysql> select cot(0);
+--------+
| cot(0) |
+--------+
|   NULL |
+--------+
1 row in set (0.03 sec)
```
