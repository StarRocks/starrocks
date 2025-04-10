---
displayed_sidebar: docs
---

# hex_decode_binary

## 功能

将一个十六进制编码的字符串解码为 VARBINARY 类型的值。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
VARBINARY hex_decode_binary(VARCHAR str);
```

## 参数说明

`str`：要解码的字符串，必须为 VARCHAR 类型。

如果发生以下任何情况，则返回一个 BINARY 类型的空值：

- 输入字符串的长度为 0，或输入字符串中的字符数量为奇数。
- 输入字符串包含 [0-9]、[a-z] 和 [A-Z] 以外的字符。

## 返回值说明

返回一个 VARBINARY 类型的值。

## 示例

```Plain
mysql> select hex(hex_decode_binary(hex("Hello StarRocks")));
+------------------------------------------------+
| hex(hex_decode_binary(hex('Hello StarRocks'))) |
+------------------------------------------------+
| 48656C6C6F2053746172526F636B73                 |
+------------------------------------------------+

mysql> select hex_decode_binary(NULL);
+--------------------------------------------------+
| hex_decode_binary(NULL)                          |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
```

## 关键字

HEX_DECODE_BINARY
