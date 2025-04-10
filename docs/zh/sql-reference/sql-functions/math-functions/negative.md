---
displayed_sidebar: docs
---

# negative

## 功能

对参数 `x` 取其负数作为结果输出。

## 语法

```Haskell
NEGATIVE(x);
```

## 参数说明

`x`: 支持的数据类型为 BIGINT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

## 返回值说明

返回值的数据类型为与参数 `x` 类型相同。

## 示例

```Plain Text
mysql>  select negative(3);
+-------------+
| negative(3) |
+-------------+
|          -3 |
+-------------+
1 row in set (0.00 sec)

mysql> select negative(cast(3.14 as decimalv2));
+--------------------------------------+
| negative(CAST(3.14 AS DECIMAL(9,0))) |
+--------------------------------------+
|                                -3.14 |
+--------------------------------------+
1 row in set (0.01 sec)
```
