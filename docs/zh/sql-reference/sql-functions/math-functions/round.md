# round, dround

## 功能

如果只存在 `x`，则 ROUND 将 `x` 向最近的整数舍入。如果存在 `n`，ROUND 将 `x` 舍入到小数点后 `n` 个小数位。若 n 为负数，则 ROUND 将舍掉小数点取整，中间数向远离 0 的方向舍入。如果发生溢出，则生成错误。

## 语法

```Haskell
ROUND(x [,n]);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE 或 DECIMAL128。

`n`: 可选，支持的数据类型为 INT。

## 返回值说明

若只指定 `x`，则返回值的数据类型为：

["DECIMAL128"] -> "DECIMAL128"

["DOUBLE"] -> "BIGINT"

若 `x` 和 `n` 都指定，则返回值的数据类型为：

["DECIMAL128", "INT"] -> "DECIMAL128"

["DOUBLE", "INT"] -> "DOUBLE"

## 示例

```Plain Text
mysql> select round(3.14);
+-------------+
| round(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.00 sec)

mysql> select round(3.14,1);
+----------------+
| round(3.14, 1) |
+----------------+
|            3.1 |
+----------------+
1 row in set (0.00 sec)

mysql> select round(13.14,-1);
+------------------+
| round(13.14, -1) |
+------------------+
|               10 |
+------------------+
1 row in set (0.00 sec)
```
