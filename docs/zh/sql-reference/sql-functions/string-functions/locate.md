---
displayed_sidebar: "Chinese"
---

# locate

## 功能

返回 `substr` 在 `str` 中出现的位置（从 1 开始计数，按「字符」计算）。如果指定了第 3 个参数 `pos`，则从 `pos` 下标开始的字符串处开始查找 `substr` 第一次出现的位置，如果没有找到则返回 0。

## 语法

```Haskell
locate(substr, str, pos)
```

## 参数说明

`substr`: 支持的数据类型为 VARCHAR。

`str`: 支持的数据类型为 VARCHAR。

`pos`: 可选参数，支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 INT。

## 示例

```Plain Text
MySQL > SELECT LOCATE('bar', 'foobarbar');
+----------------------------+
| locate('bar', 'foobarbar') |
+----------------------------+
|                          4 |
+----------------------------+

MySQL > SELECT LOCATE('xbar', 'foobar');
+--------------------------+
| locate('xbar', 'foobar') |
+--------------------------+
|                        0 |
+--------------------------+

MySQL > SELECT LOCATE('bar', 'foobarbar', 5);
+-------------------------------+
| locate('bar', 'foobarbar', 5) |
+-------------------------------+
|                             7 |
+-------------------------------+
```
