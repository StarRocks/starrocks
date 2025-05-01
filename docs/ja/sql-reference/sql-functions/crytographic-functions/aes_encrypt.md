---
displayed_sidebar: docs
---

# aes_encrypt

## 説明

AES_128_ECBアルゴリズムを使用して文字列を暗号化し、バイナリ文字列を返します。

AESはadvanced encryption standard（高度暗号化標準）の略で、ECBはelectronic code book（電子コードブック）の略です。文字列を暗号化するために使用されるキーは128ビットの文字列です。

## 構文

```Haskell
aes_encrypt(str,key_str);
```

## パラメータ

`str`: 暗号化する文字列。VARCHAR型でなければなりません。

`key_str`: `str`を暗号化するために使用されるキー。VARCHAR型でなければなりません。

## 戻り値

VARCHAR型の値を返します。入力がNULLの場合、NULLが返されます。

## 例

この関数を使用して`starrocks`をAESで暗号化し、暗号化された文字列をBase64エンコードされた文字列に変換します。

```Plain Text
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3'));
+-------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3')) |
+-------------------------------------------------------------------------+
| uv/Lhzm74syo8JlfWarwKA==                                                |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```