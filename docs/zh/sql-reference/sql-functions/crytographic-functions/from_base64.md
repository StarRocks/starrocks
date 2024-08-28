---
displayed_sidebar: docs
---

# from_base64

## 功能

将 Base64 编码过的字符串 `str` 进行解码。反向函数为 [to_base64](to_base64.md)。

## 语法

```Haskell
from_base64(str);
```

## 参数说明

`str`: 待解码的字符串，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。如果输入值为 NULL 或者不是有效的 Base64 字符串，则返回 NULL。

该函数仅接收一个字符串，如果输入多个字符串，会返回报错。

## 示例

```Plain Text
mysql> select from_base64("starrocks");
+--------------------------+
| from_base64('starrocks') |
+--------------------------+
| ²֫®$                       |
+--------------------------+
1 row in set (0.00 sec)
```
