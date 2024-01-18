---
displayed_sidebar: "Chinese"
---

# positive

## 功能

返回表达式 `x` 的结果。

## 语法

```Haskell
POSITIVE(x);
```

## 参数说明

`x`: 支持的数据类型为 BIGINT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

## 返回值说明

返回值的数据类型为与参数 `x` 类型相同。

## 示例

```Plain Text
mysql> select positive(3);
+-------------+
| positive(3) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)

mysql> select positive(cast(3.14 as decimalv2));
+--------------------------------------+
| positive(CAST(3.14 AS DECIMAL(9,0))) |
+--------------------------------------+
|                                 3.14 |
+--------------------------------------+
1 row in set (0.01 sec)
```
