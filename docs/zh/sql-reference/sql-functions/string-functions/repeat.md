---
displayed_sidebar: "Chinese"
---

# repeat

## 功能

将字符串 重复 count 次输出，count 小于 1 时返回空字符串。str 或 count 为 NULL 时，返回 NULL。

## 语法

```Haskell
repeat(str, count)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`count`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > SELECT repeat("a", 3);
+----------------+
| repeat('a', 3) |
+----------------+
| aaa            |
+----------------+

MySQL > SELECT repeat("a", -1);
+-----------------+
| repeat('a', -1) |
+-----------------+
|                 |
+-----------------+
```
