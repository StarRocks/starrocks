---
displayed_sidebar: "Chinese"
---

# crc32

## 功能

返回字符串的 32 位循环冗余校验值（Cyclic Redundancy Check）。如果输入参数为 NULL，则返回 NULL。

CRC32 是一个用于错误检测的函数，使用 CRC32 算法来检测源数据和目标数据之间的变化。

## 语法

```Haskell
BIGINT crc32(VARCHAR str)
```

## 参数说明

`str`：输入的字符串。

## 返回值说明

返回一个 32 位循环冗余校验值。

## 示例

```Plain Text
mysql > select crc32("starrocks");
+--------------------+
| crc32('starrocks') |
+--------------------+
|         2312449062 |
+--------------------+

mysql > select crc32(null);
+-------------+
| crc32(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.18 sec)
```

## Keywords

CRC32
