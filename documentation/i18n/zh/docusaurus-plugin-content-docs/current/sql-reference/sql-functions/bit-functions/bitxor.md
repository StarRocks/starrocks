# bitxor

## 功能

返回两个数值在按位 XOR 运算后的结果。

## 语法

```Haskell
BITXOR(x,y);
```

## 参数说明

`x`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

`y`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

> 注：使用时 `x` 与 `y` 数据类型要相同。

## 返回值说明

返回值的数据类型与 `x` 类型一致。

## 示例

```Plain Text
mysql> select bitxor(3,0);
+--------------+
| bitxor(3, 0) |
+--------------+
|            3 |
+--------------+
1 row in set (0.00 sec)
```
