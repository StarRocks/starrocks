---
displayed_sidebar: docs
---

# lower

## 功能

将参数中所有的字符串转换成小写。

## 语法

```Haskell
lower(str)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 语法说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > SELECT lower("AbC123");
+-----------------+
| lower('AbC123') |
+-----------------+
| abc123          |
+-----------------+
```
