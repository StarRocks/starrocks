---
displayed_sidebar: "Chinese"
---

# concat

## 功能

将多个字符串连接起来。如果参数中任意一个值是 NULL，那么返回的结果为 NULL。

## 语法

```Haskell
concat(str,...)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > select concat("a", "b");
+------------------+
| concat('a', 'b') |
+------------------+
| ab               |
+------------------+

MySQL > select concat("a", "b", "c");
+-----------------------+
| concat('a', 'b', 'c') |
+-----------------------+
| abc                   |
+-----------------------+

MySQL > select concat("a", null, "c");
+------------------------+
| concat('a', NULL, 'c') |
+------------------------+
| NULL                   |
+------------------------+
```
