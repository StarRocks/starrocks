# bitand

## 功能

返回两个数值在按位进行 AND 运算后的结果。

## 语法

```Haskell
BITAND(x,y);
```

## 参数说明

`x`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

`y`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

> 注：使用时 `x` 与 `y` 数据类型要一致。

## 返回值说明

返回值的数据类型与 `x` 类型一致。

## 示例

```Plain Text
mysql> select bitand(3,0);
+--------------+
| bitand(3, 0) |
+--------------+
|            0 |
+--------------+
1 row in set (0.01 sec)
```
