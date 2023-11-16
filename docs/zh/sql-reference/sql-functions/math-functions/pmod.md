---
displayed_sidebar: "Chinese"
---

# pmod

## 功能

返回模系下参数 `x` 对参数 `y` 取余的余数

## 语法

```Haskell
PMOD(x,y);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE、BIGINT。

`y`: 支持的数据类型为 DOUBLE、BIGINT。

> 注：`x` 和 `y` 数据类型必须一致。

## 返回值说明

返回值的数据类型为与参数 `x` 类型相同

## 示例

```Plain Text
mysql> select pmod(3.14,3.14);
+------------------+
| pmod(3.14, 3.14) |
+------------------+
|                0 |
+------------------+
1 row in set (0.02 sec)

mysql> select pmod(3,9);
+------------+
| pmod(3, 9) |
+------------+
|          3 |
+------------+
1 row in set (0.01 sec)
```

## 关键词

PMOD
