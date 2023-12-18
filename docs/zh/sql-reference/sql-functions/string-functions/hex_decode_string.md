---
displayed_sidebar: "Chinese"
---

# hex_decode_string

## 功能

将输入字符串中每一对十六进制数字解析为一个数字，并将解析得到的数字转换为表示该数字的字节，然后返回一个二进制字符串。该函数是 [hex()](../../../sql-reference/sql-functions/string-functions/hex.md) 函数的反向函数。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
VARCHAR hex_decode_string(VARCHAR str);
```

## 参数说明

`str`：要解码的字符串，必须为 VARCHAR 类型。

如果发生以下任何情况，则返回一个空字符串：

- 输入字符串的长度为 0，或输入字符串中的字符数量为奇数。
- 输入字符串包含 [0-9]、[a-z] 和 [A-Z] 以外的字符。

## 返回值说明

返回一个 VARCHAR 类型的值。

## 示例

```Plain
mysql> select hex_decode_string(hex("Hello StarRocks"));
+-------------------------------------------+
| hex_decode_string(hex('Hello StarRocks')) |
+-------------------------------------------+
| Hello StarRocks                           |
+-------------------------------------------+
```

## 关键字

HEX_DECODE_STRING
