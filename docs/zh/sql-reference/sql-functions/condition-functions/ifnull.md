---
displayed_sidebar: docs
---

# ifnull

## 功能

若 `expr1` 不为 NULL，返回 `expr1`。若 `expr1` 为 NULL，返回 `expr2`。

## 语法

```Haskell
ifnull(expr1,expr2);
```

## 参数说明

`expr1` 与 `expr2` 必须在数据类型上能够兼容，否则返回报错。

## 返回值说明

返回值的数据类型与 `expr1` 类型一致。

## 示例

```Plain Text
mysql> select ifnull(2,4);
+--------------+
| ifnull(2, 4) |
+--------------+
|            2 |
+--------------+

mysql> select ifnull(NULL,2);
+-----------------+
| ifnull(NULL, 2) |
+-----------------+
|               2 |
+-----------------+
```
