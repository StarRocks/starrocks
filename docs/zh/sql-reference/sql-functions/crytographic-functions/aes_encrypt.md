---
displayed_sidebar: docs
---

# aes_encrypt



默认使用 AES_128_ECB 算法对字符串 `str` 进行加密并返回一个二进制字符串。

AES 全称为 Advanced Encryption Standard。ECB 全称为 Electronic Code Book，电码本模式。该算法使用一个长度为 128-bit 的 key 对字符串进行编码。

## 语法

```Haskell
aes_encrypt(str, key_str[, init_vector][, encryption_mode][, aad_str]);
```

## 参数说明
- 必填参数：
    - `str`: 待加密的字符串，支持的数据类型为 VARCHAR。

    - `key_str`: 用于加密 `str` 的 key 字符串，支持的数据类型为 VARCHAR。
- 可选参数：
    - `init_vector`: Initialization Vector(IV)，初始化向量是 AES 加密中至关重要的安全参数，用于确保相同明文加密后产生不同的密文。只在СВС/CFB/OFB/CTR/GCM模式下生效。支持的数据类型为 VARCHAR。
    - `encryption_mode`: 加密算法,支持算法如下。支持的数据类型为 VARCHAR。

        | ECB         | CBC         | CFB         | CFB1        | CFB8        | CFB128        | OFB         | CTR       | GCM        |
        |-------------|-------------|-------------|-------------|-------------|---------------|-------------|-----------|------------|
        | AES_128_ECB | AES_128_CBC | AES_128_CFB | AES_128_CFB1| AES_128_CFB8| AES_128_CFB128| AES_128_OFB| AES_128_CTR| AES_128_GCM|
        | AES_192_ECB | AES_192_CBC | AES_192_CFB | AES_192_CFB1| AES_192_CFB8| AES_192_CFB128| AES_192_OFB| AES_192_CTR| AES_192_GCM|
        | AES_256_ECB | AES_256_CBC | AES_256_CFB | AES_256_CFB1| AES_256_CFB8| AES_256_CFB128| AES_256_OFB| AES_256_CTR| AES_256_GCM|
    - `aad_str` 指的是附加数据（Additional Authenticated Data，AAD），这是认证加密模式（如 GCM）特有的概念，在加密过程中不被加密的数据。支持的数据类型为 VARCHAR。



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
```
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_ECB"));
+----------------------------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3', NULL, 'AES_128_ECB')) |
+----------------------------------------------------------------------------------------------+
| uv/Lhzm74syo8JlfWarwKA==                                                                     |
+----------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
```
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CBC"));
+---------------------------------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3', 'abcdefg', 'AES_128_CBC')) |
+---------------------------------------------------------------------------------------------------+
| taXlwIvir9yff94F5Uv/KA==                                                                          |
+---------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
```
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_128_GCM", "abcdefg"));
+-----------------------------------------------------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3', 'abcdefghijklmnop', 'AES_128_GCM', 'abcdefg')) |
+-----------------------------------------------------------------------------------------------------------------------+
| YWJjZGVmZ2hpamtsdpJC2rnrGmvqKQv/WcoO6NuOCXvUnC8pCw==                                                                  |
+-----------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
