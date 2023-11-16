---
displayed_sidebar: "Chinese"
---

# mod

## 功能

取模函数，返回两个数相除之后的余数。

根据下列语法，返回 `dividend` 除以 `divisor` 后所得的余数。

## 语法

```Haskell
mod(dividend,divisor);
```

## 参数说明

- `dividend`: 被除数，支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。
- `divisor`: 除数，支持的数据类型和 `dividend` 相同。

> 注：`dividend` 和 `divisor` 数据类型必须一致。如果不一致，会进行隐式转换。

## 返回值说明

返回值的数据类型和符号与 `dividend` 相同。如果 `divisor` 为 0， 返回 NULL。

## 示例

```Plain
mysql> select mod(3.14,3.14);
+-----------------+
| mod(3.14, 3.14) |
+-----------------+
|               0 |
+-----------------+

mysql> select mod(3.14, 3);
+--------------+
| mod(3.14, 3) |
+--------------+
|         0.14 |
+--------------+

select mod(11,-5);
+------------+
| mod(11, -5)|
+------------+
|          1 |
+------------+

select mod(-11,5);
+-------------+
| mod(-11, 5) |
+-------------+
|          -1 |
+-------------+
```
