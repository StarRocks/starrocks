---
displayed_sidebar: docs
---

# append_trailing_char_if_absent

## 功能

如果 str 字符串非空并且末尾不包含 trailing_char 字符，则将 trailing_char 字符附加到末尾。

## 语法

```Haskell
append_trailing_char_if_absent(str, trailing_char)
```

## 参数说明

`str`: 给定字符串，支持的数据类型为 VARCHAR。

`trailing_char`: 给定字符，支持的数据类型为 VARCHAR，trailing_char 只能包含一个字符，若包含多个字符，将返回 NULL。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL [test]> select append_trailing_char_if_absent('a','c');
+------------------------------------------+
|append_trailing_char_if_absent('a', 'c')  |
+------------------------------------------+
| ac                                       |
+------------------------------------------+
1 row in set (0.02 sec)

MySQL [test]> select append_trailing_char_if_absent('ac','c');
+-------------------------------------------+
|append_trailing_char_if_absent('ac', 'c')  |
+-------------------------------------------+
| ac                                        |
+-------------------------------------------+
1 row in set (0.00 sec)
```
