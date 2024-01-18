---
displayed_sidebar: "Chinese"
---

# substr, substring

## 功能

若没有 `len` 参数时返回从位置 `pos` 开始的字符串 `str` 的一个子字符串， 若有 `len` 参数时返回从位置 `pos` 开始的字符串 `str` 的一个长度为 `len` 子字符串， `pos` 参数可以使用负值，在这种情况下，子字符串是以字符串 `str` 末尾开始计算 `pos` 个字符，而不是开头，pos 的值为 0 返回一个空字符串。

> 注：字符串中第一个字符的位置为 1。

## 语法

```Haskell
VARCHAR substr(VARCHAR str, INT pos[, INT len]);
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`pos`: 支持的数据类型为 INT。

`len`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select substr("starrocks",3);
+------------------------+
| substr('starrocks', 3) |
+------------------------+
| arrocks                |
+------------------------+
1 row in set (0.00 sec)

mysql> select substring("starrocks",-2);
+----------------------------+
| substring('starrocks', -2) |
+----------------------------+
| ks                         |
+----------------------------+
1 row in set (0.00 sec)
```
