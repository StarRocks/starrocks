---
displayed_sidebar: "Chinese"
---

# lpad

## 功能

根据指定的长度在字符串前面（左侧）追加字符。

- 若 `len` 大于 `str` 的长度，在 `str` 的前面不断补充 `pad` 字符，直到该字符串的长度达到 `len` 为止。
- 若 `len` 小于 `str` 的长度，该函数相当于截断 `str` 字符串，只返回长度为 `len` 的字符串。

## 语法

```Haskell
lpad(str, len, pad)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`len`: 支持的数据类型为 INT，指的是 **字符** 长度而不是字节长度。

`pad`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > SELECT lpad("hi", 5, "xy");
+---------------------+
| lpad('hi', 5, 'xy') |
+---------------------+
| xyxhi               |
+---------------------+

MySQL > SELECT lpad("hi", 1, "xy");
+---------------------+
| lpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+
```
