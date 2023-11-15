# pow, power, dpow, fpow

## 功能

返回参数 `x` 的 `y` 次方。

## 语法

```Haskell
POW(x,y);
POWER(x,y);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

`y`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select pow(2,2);
+-----------+
| pow(2, 2) |
+-----------+
|         4 |
+-----------+
1 row in set (0.00 sec)

mysql> select power(4,3);
+-------------+
| power(4, 3) |
+-------------+
|          64 |
+-------------+
1 row in set (0.00 sec)

```
