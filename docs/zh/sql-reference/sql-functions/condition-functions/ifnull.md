---
displayed_sidebar: "Chinese"
---

# ifnull

## 功能

若参数 `expr1` 不为 NULL，返回 `expr1`，否则返回 `expr2`。

## 语法

```Haskell
ifnull(expr1,expr2);
```

## 参数说明

`expr1`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

`expr2`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

> 注：`expr1` 与 `expr2` 类型需要一致。

## 返回值说明

返回值的数据类型与 `expr1` 类型一致。

## 示例

```Plain Text
mysql> select ifnull(NULL,2);
+-----------------+
| ifnull(NULL, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.01 sec)
```
