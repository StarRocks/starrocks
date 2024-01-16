---
displayed_sidebar: "Chinese"
---

# if

## 功能

若参数 `expr1` 成立，返回结果 `expr2`，否则返回结果 `expr3`。

## 语法

```Haskell
if(expr1,expr2,expr3);
```

## 参数说明

`expr1`: 支持的数据类型为 BOOLEAN。

`expr2`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

`expr3`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

> 注：`expr2` 与 `expr3` 类型需要一致。

## 返回值说明

返回值的数据类型与 `expr2` 类型一致。

## 示例

```Plain Text
mysql> select if(false,1,2);
+-----------------+
| if(FALSE, 1, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.00 sec)
```
