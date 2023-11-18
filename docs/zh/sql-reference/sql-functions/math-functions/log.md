---
displayed_sidebar: "Chinese"
---

# log

## 功能

返回以 `base` 为底数的 `x` 的对数。如果未指定 `base`，则该函数等同于 [ln()](./ln.md)。

## 语法

```Haskell
log([base,] x);
```

## 参数说明

`base`: 底数，可选。支持的数据类型为 DOUBLE。`base` 必须大于 0，且不能为 1，否则返回 NULL。

`x`: 要计算对数的数值，必填。支持的数据类型为 DOUBLE。`x` 必须大于 0，否则返回 NULL。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

示例一：计算底数为 2 的 8 的对数。

```Plain Text
mysql> select log(2,8);
+-----------+
| log(2, 8) |
+-----------+
|         3 |
+-----------+
1 row in set (0.01 sec)
```

示例二：不指定底数，返回 2 的自然对数。

```Plain Text
mysql> select log(2);
+--------------------+
| log(2)             |
+--------------------+
| 0.6931471805599453 |
+--------------------+
1 row in set (0.00 sec)
```
