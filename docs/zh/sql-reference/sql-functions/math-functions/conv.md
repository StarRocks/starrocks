# conv

## 功能

对输入的参数 `x` 进行进制转换。

## 语法

```Haskell
CONV(x,y,z);
```

## 参数说明

`x`: 支持的数据类型为 VARCHAR、BIGINT。

`y`: 支持的数据类型为 TINYINT，原始进制。

`z`: 支持的数据类型为 TINYINT，目标进制。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select conv(8,10,2);
+----------------+
| conv(8, 10, 2) |
+----------------+
| 1000           |
+----------------+
1 row in set (0.00 sec)
```
