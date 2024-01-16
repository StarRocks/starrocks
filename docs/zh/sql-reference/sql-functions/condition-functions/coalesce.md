---
displayed_sidebar: "Chinese"
---

# coalesce

## 功能

从左向右返回参数中的第一个非 NULL 表达式。

## 语法

```Haskell
coalesce(expr1,...);
```

## 参数说明

`expr1`: 支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DATETIME、DATE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、VARCHAR、BITMAP、PERCENTILE、HLL、TIME。

## 返回值说明

返回值的数据类型与 `expr1` 类型一致。

## 示例

```Plain Text
mysql> select coalesce(3,NULL,1,1);
+-------------------------+
| coalesce(3, NULL, 1, 1) |
+-------------------------+
|                       3 |
+-------------------------+
1 row in set (0.00 sec)
```
