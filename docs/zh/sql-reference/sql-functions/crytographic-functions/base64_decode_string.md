---
displayed_sidebar: docs
---

# base64_decode_string

## 功能

同 [from_base64()](../crytographic-functions/from_base64.md) 函数，用于解码某个 Base64 编码的字符串，是 [to_base64()](../crytographic-functions/to_base64.md) 函数的反向函数。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
VARCHAR base64_decode_string(VARCHAR str);
```

## 参数说明

`str`：要解码的字符串，必须为 VARCHAR 类型。

## 返回值说明

返回一个 VARCHAR 类型的值。如果输入为 `NULL` 或无效的 Base64 编码字符串，则返回 `NULL`。如果输入为空，则返回错误消息。 该函数只支持输入一个字符串。输入多个字符串会导致报错。

## 示例

```Plain
mysql> select base64_decode_string(to_base64("Hello StarRocks"));
+----------------------------------------------------+
| base64_decode_string(to_base64('Hello StarRocks')) |
+----------------------------------------------------+
| Hello StarRocks                                    |
+----------------------------------------------------+
```

## 关键字

BASE64_DECODE_STRING
