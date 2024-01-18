---
displayed_sidebar: "Chinese"
---

# aes_encrypt

## 功能

使用 AES_128_ECB 算法对字符串 `str` 进行加密并返回一个二进制字符串。

AES 全称为 Advanced Encryption Standard。ECB 全称为 Electronic Code Book，电码本模式。该算法使用一个长度为 128-bit 的 key 对字符串进行编码。

## 语法

```Haskell
aes_encrypt(str,key_str);
```

## 参数说明

`str`: 待加密的字符串，支持的数据类型为 VARCHAR。

`key_str`: 用于加密 `str` 的 key 字符串，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。如果输入值为 NULL，则返回 NULL。

## 示例

将字符串 `starrocks` 进行 AES 编码，并转为 Base64 字符串。

```Plain Text

mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3'));
+-------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3')) |
+-------------------------------------------------------------------------+
| uv/Lhzm74syo8JlfWarwKA==                                                |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
