---
displayed_sidebar: "Chinese"
---

# to_base64

## 功能

将字符串 `str` 进行 Base64 编码。反向函数为 [from_base64](from_base64.md)。

## 语法

```Haskell
to_base64(str);
```

## 参数说明

`str`: 要编码的字符串，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。如果输入为 NULL，则返回 NULL。如果输入为空，则返回报错。

该函数仅接收一个字符串，如果输入多个字符串，会返回报错。

## 示例

```Plain Text
mysql> select to_base64("starrocks");
+------------------------+
| to_base64('starrocks') |
+------------------------+
| c3RhcnJvY2tz           |
+------------------------+
1 row in set (0.00 sec)

mysql> select to_base64(123);
+----------------+
| to_base64(123) |
+----------------+
| MTIz           |
+----------------+
```
