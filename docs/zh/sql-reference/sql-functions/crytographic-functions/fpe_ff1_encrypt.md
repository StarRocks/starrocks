---
displayed_sidebar: "Chinese"
---

# fpe_ff1_encrypt

## 功能

将字符串 `str` 进行Format-Preserving Encryption (FPE)  FF1方法加密。反向函数为 [fpe_ff1_decrypt](fpe_ff1_decrypt.md)。

## 语法

```Haskell
fpe_ff1_encrypt(str, key, radix);
```

## 参数说明

`str`: 要加密的字符串，支持的数据类型为 VARCHAR。

`key`: 用于加密 `str` 的 key 字符串，长度限制为16、24或者32，支持的数据类型为 VARCHAR。

`radix`: 加密 `str` 的数值基数，如果基数是10，则明文的字符集包含字符串"0123456789"中的字符。如果基数是16，则字符集为字符串"0123456789abcdef"中的字符。支持的最大基数为36，基数为36时的字符集为"0123456789abcdefghijklmnopqrstuvwxyz"。支持的数据类型为 INT。



## 返回值说明

返回值的数据类型为 VARCHAR。如果输入为 NULL，则返回 NULL。如果输入为空，则返回报错。

该函数仅接收3个参数，如果输入多个参数，会返回报错。

## 示例

```Plain Text
mysql> select fpe_ff1_encrypt('893892', 'abcdefghijk12345abcdefghijk12345', 10);
+-------------------------------------------------------------------+
| fpe_ff1_encrypt('893892', 'abcdefghijk12345abcdefghijk12345', 10) |
+-------------------------------------------------------------------+
| 697512                                                            |
+-------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select fpe_ff1_encrypt(42302920232, 'abcdefghijk12345abcdefghijk12345', 10);
+----------------------------------------------------------------------+
| fpe_ff1_encrypt(42302920232, 'abcdefghijk12345abcdefghijk12345', 10) |
+----------------------------------------------------------------------+
| 30094485160                                                          |
+----------------------------------------------------------------------+
1 row in set (0.00 sec)
```