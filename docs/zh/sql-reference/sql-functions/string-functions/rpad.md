---
displayed_sidebar: "Chinese"
---

# rpad

## 功能

根据指定的长度在字符串后面（右侧）追加字符。

- 如果 len 大于 str 的长度，则在 str 的后面不断补充 pad 字符，直到该字符串的长度达到 len 为止。

- 如果 len 小于 str 的长度，该函数相当于截断 str 字符串，只返回长度为 len 的字符串。

> 注: len 指的是 **字符** 长度而不是字节长度。

## 语法

```Haskell
rpad(str, len, pad)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`len`: 支持的数据类型为 INT。

`pad`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > SELECT rpad("hi", 5, "xy");
+---------------------+
| rpad('hi', 5, 'xy') |
+---------------------+
| hixyx               |
+---------------------+

MySQL > SELECT rpad("hi", 1, "xy");
+---------------------+
| rpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+
```
