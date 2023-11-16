---
displayed_sidebar: "Chinese"
---

# aes_decrypt

## 功能

使用 AES_128_ECB 算法将字符串 `str` 解密并返回一个二进制字符串。

## 语法

```Haskell
aes_decrypt(str,key_str);
```

## 参数说明

`str`: 待解密的字符串，支持的数据类型为 VARCHAR。

`key_str`: 用于加密 `str` 的 key 字符串，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。如果输入值非法，则返回 NULL。

## 示例

将 Base64 编码过的字符串进行解码，然后进行 AES 解密。

```Plain Text
mysql> select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA==  '),'F3229A0B371ED2D9441B830D21A390C3');
+--------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('uv/Lhzm74syo8JlfWarwKA==  '), 'F3229A0B371ED2D9441B830D21A390C3') |
+--------------------------------------------------------------------------------------------+
| starrocks                                                                                  |
+--------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
