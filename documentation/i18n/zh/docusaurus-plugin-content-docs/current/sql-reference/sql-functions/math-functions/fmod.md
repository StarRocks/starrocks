---
displayed_sidebar: "Chinese"
---

# fmod

## 功能

取模函数，返回两个数相除之后的浮点余数。

根据下列语法，返回 `dividend` 除以 `divisor` 后所得的浮点余数。

## 语法

```Haskell
fmod(dividend,devisor);
```

## 参数说明

`dividend`: 被除数，支持的数据类型为 DOUBLE、FLOAT。

`devisor`: 除数，支持的数据类型为 DOUBLE、FLOAT。

> `dividend` 和 `devisor` 数据类型必须一致。如果不一致，会进行隐式转换。

## 返回值说明

返回值的数据类型和符号与 `dividend` 相同。如果 `divisor` 为 0， 返回 NULL。

## 示例

```Plain
mysql> select fmod(3.14,3.14);
+------------------+
| fmod(3.14, 3.14) |
+------------------+
|                0 |
+------------------+

mysql> select fmod(11.5,3);
+---------------+
| fmod(11.5, 3) |
+---------------+
|           2.5 |
+---------------+

mysql> select fmod(3,6);
+------------+
| fmod(3, 6) |
+------------+
|          3 |
+------------+

mysql> select fmod(3,0);
+------------+
| fmod(3, 0) |
+------------+
|       NULL |
+------------+
```
