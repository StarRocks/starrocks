---
displayed_sidebar: "Chinese"
---

# aes_encrypt

## 功能

使用 AES_128_ECB 算法，将参数加密并返回一个二进制字符串。

## 语法

```Haskell
aes_encrypt(x,y);
```

## 参数说明

`x`: 支持的数据类型为 VARCHAR，它是加密的字符串。

`y`: 支持的数据类型为 VARCHAR，它是用于加密参数 `x` 的 key 字符串。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3'));
+-------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3')) |
+-------------------------------------------------------------------------+
| uv/Lhzm74syo8JlfWarwKA==                                                |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

## 关键词

AES_ENCRYPT
