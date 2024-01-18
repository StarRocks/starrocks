---
displayed_sidebar: "Chinese"
---

# nullif

## 功能

若参数 `expr1` 与 `expr2` 相等，则返回 NULL，否则返回 `expr1` 的值。

## 语法

```Haskell
nullif(expr1,expr2);
```

## 参数说明

`expr1`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

`expr2`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

> 注：`expr1` 与 `expr2` 类型需要一致。

## 返回值说明

返回值的数据类型与 `expr1` 类型一致。

## 示例

```Plain Text
mysql> select nullif(1,2);
+--------------+
| nullif(1, 2) |
+--------------+
|            1 |
+--------------+
1 row in set (0.01 sec)
```
