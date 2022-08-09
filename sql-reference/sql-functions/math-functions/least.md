# least

## 功能

返回所给参数中的最小值。

## 语法

```Haskell
LEAST(expr1,...);
```

## 参数说明

`expr1`: 支持的数据类型为 SMALLINT、TINYINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、DATETIME、VARCHAR。

## 返回值说明

返回值的数据类型为与参数 `expr1` 类型相同。

## 示例

```Plain Text
mysql> select least(3);
+----------+
| least(3) |
+----------+
|        3 |
+----------+
1 row in set (0.00 sec)

mysql> select least(3,4,5,5,6);
+----------------------+
| least(3, 4, 5, 5, 6) |
+----------------------+
|                    3 |
+----------------------+
1 row in set (0.01 sec)
```
