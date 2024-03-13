---
displayed_sidebar: "Chinese"
---

# base64_decode_binary

## 功能

解码某个 Base64 编码的字符串，并返回一个 VARBINARY 类型的值。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
VARBINARY base64_decode_binary(VARCHAR str);
```

## 参数说明

`str`：要解码的字符串，必须为 VARCHAR 类型。

## 返回值说明

返回一个 VARBINARY 类型的值。如果输入为 `NULL` 或无效的 Base64 编码字符串，则返回 `NULL`。如果输入为空，则返回错误消息。 该函数只支持输入一个字符串。输入多个字符串会导致报错。

## 示例

```Plain
mysql> select hex(base64_decode_binary(to_base64("Hello StarRocks")));
+---------------------------------------------------------+
| hex(base64_decode_binary(to_base64('Hello StarRocks'))) |
+---------------------------------------------------------+
| 48656C6C6F2053746172526F636B73                          |
+---------------------------------------------------------+

mysql> select base64_decode_binary(NULL);
+--------------------------------------------------------+
| base64_decode_binary(NULL)                             |
+--------------------------------------------------------+
| NULL                                                   |
+--------------------------------------------------------+
```

## 关键字

BASE64_DECODE_BINARY
