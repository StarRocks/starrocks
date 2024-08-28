---
displayed_sidebar: docs
---

# char_length

## 功能

返回字符串的长度。
对于多字节字符，返回 **字符** 数，目前仅支持 UTF-8 编码，该函数还有一个别名 `character_length`。

## 语法

```Haskell
char_length(str)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 INT。

## 示例

```Plain Text
MySQL > select char_length("abc");
+--------------------+
| char_length('abc') |
+--------------------+
|                  3 |
+--------------------+

MySQL > select char_length("中国");
+----------------------+
| char_length('中国')  |
+----------------------+
|                    2 |
+----------------------+
```
