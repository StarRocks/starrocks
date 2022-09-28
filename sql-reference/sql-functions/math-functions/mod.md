# mod

## 功能

取模函数，返回 `x` 除以 `y` 的余数，返回值的符号与 `x` 相同;如果 `y` 为 0，则生成一个错误。

## 语法

```Haskell
mod(x,y);
```

## 参数说明

`x`: 支持的数据类型为 BIGINT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

`y`: 支持的数据类型为 BIGINT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

## 返回值说明

返回值的数据类型为与参数`x`类型相同。

## 示例

```Plain Text
mysql> select mod(3.14,3.14);
+-----------------+
| mod(3.14, 3.14) |
+-----------------+
|               0 |
+-----------------+
1 row in set (0.14 sec)

mysql> select mod(cast(3.14 as decimalv2),cast(3.14 as decimal));
+----------------------------------------------------------------+
| mod(CAST(3.14 AS DECIMAL(9,0)), CAST(3.14 AS DECIMAL64(10,0))) |
+----------------------------------------------------------------+
|                                                    0.140000000 |
+----------------------------------------------------------------+
1 row in set (0.01 sec)
```
