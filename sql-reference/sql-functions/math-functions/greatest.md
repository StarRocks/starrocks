# greatest

## 功能

返回所给参数中的最大值。

## 语法

```Haskell
GREATEST(x,...);
```

## 参数说明

`x`: 支持的数据类型为 SMALLINT、TINYINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、DATETIME、VARCHAR。

## 返回值说明

返回值的数据类型为与参数 `x` 类型相同。

## 示例

```Plain Text
mysql> select greatest(3);
+-------------+
| greatest(3) |
+-------------+
|           3 |s
+-------------+
1 row in set (0.01 sec)

mysql> select greatest(3,4,5,5,6);
+-------------------------+
| greatest(3, 4, 5, 5, 6) |
+-------------------------+
|                       6 |
+-------------------------+
1 row in set (0.00 sec)
```
