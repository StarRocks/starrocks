# mod

## 功能

取模函数，返回 `x` 除以 `y` 的余数，返回值的符号与 `x` 相同;如果 `y` 为 0，则生成一个错误。

## 语法

```Haskell
mod(x,y);
```

## 参数说明

`x`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

`y`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

> 注：`x` 和 `y` 数据类型必须一致。

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

mysql> select mod(3.14, 3);
+--------------+
| mod(3.14, 3) |
+--------------+
|         0.14 |
+--------------+
1 row in set (0.01 sec)
```
