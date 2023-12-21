---
displayed_sidebar: "Chinese"
---

# substring_index

## 功能

从给定字符串中截取第 `count` 个分隔符之前或之后的字符串。

- 如果 `count` 是正数，从字符串左边开始截取第 `count` 个分隔符之前的字符串。比如，`select substring_index('https://www.starrocks.io', '.', 2);` 从左往右数截取第二个 `.` 分隔符前面的字符串，返回 `https://www.starrocks`。

- 如果 `count` 是正数，从字符串右边开始截取第 `count` 个分隔符之后的字符串。比如，`select substring_index('https://www.starrocks.io', '.', -2);` 从右往左数截取第二个 `.` 分隔符之后的字符串，返回 `starrocks.io`。

如何任一输入参数为 NULL，返回 NULL。

该函数从 3.2 版本开始支持。

## 语法

```Haskell
VARCHAR substring_index(VARCHAR str, VARCHAR delimiter, INT count)
```

## 参数说明

- `str`：必填，待截取的字符串。
- `delimiter`：必填，使用的分隔符。
- `count`：必填，分隔符出现的位置。不能为 0，否则返回 NULL。如果该参数值大于 `delimiter` 实际出现的次数，则返回整个字符串。

## 返回值说明

返回一个 VARCHAR 字符串。

## 示例

```Plain Text
-- 从左往右数截取第二个 `.` 分隔符前面的字符串。
mysql> select substring_index('https://www.starrocks.io', '.', 2);
+-----------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', 2) |
+-----------------------------------------------------+
| https://www.starrocks                               |
+-----------------------------------------------------+

-- Count 为负，从右往左数截取第二个 `.` 分隔符之后的字符串，
mysql> select substring_index('https://www.starrocks.io', '.', -2);
+------------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', -2) |
+------------------------------------------------------+
| starrocks.io                                         |
+------------------------------------------------------+

mysql> select substring_index("hello world", " ", 1);
+----------------------------------------+
| substring_index("hello world", " ", 1) |
+----------------------------------------+
| hello                                  |
+----------------------------------------+

mysql> select substring_index("hello world", " ", -1);
+-----------------------------------------+
| substring_index('hello world', ' ', -1) |
+-----------------------------------------+
| world                                   |
+-----------------------------------------+

-- Count 为 0，返回 NULL。
mysql> select substring_index("hello world", " ", 0);
+----------------------------------------+
| substring_index('hello world', ' ', 0) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+

-- Count 大于 `delimiter` 实际出现的次数，返回整个字符串。
mysql> select substring_index("hello world", " ", 2);
+----------------------------------------+
| substring_index("hello world", " ", 2) |
+----------------------------------------+
| hello world                            |
+----------------------------------------+

-- Count 大于 `delimiter` 实际出现的次数，返回整个字符串。
mysql> select substring_index("hello world", " ", -2);
+-----------------------------------------+
| substring_index("hello world", " ", -2) |
+-----------------------------------------+
| hello world                             |
+-----------------------------------------+
```

## keywords

substring_index
