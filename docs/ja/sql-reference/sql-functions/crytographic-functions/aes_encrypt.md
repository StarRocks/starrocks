---
displayed_sidebar: docs
---

# aes_encrypt

AES_128_ECB アルゴリズムを使用して文字列を暗号化し、バイナリ文字列を返します。

AES は advanced encryption standard の略で、ECB は electronic code book の略です。文字列を暗号化するために使用されるキーは 128 ビットの文字列です。

## 構文

```Haskell
aes_encrypt(str,key_str);
```

## パラメータ

`str`: 暗号化する文字列。VARCHAR 型でなければなりません。

`key_str`: `str` を暗号化するために使用されるキー。VARCHAR 型でなければなりません。

## 戻り値

VARCHAR 型の値を返します。入力が NULL の場合、NULL が返されます。

## 例

この関数を使用して `starrocks` を AES 暗号化し、暗号化された文字列を Base64 エンコードされた文字列に変換します。

```Plain Text
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3'));
+-------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3')) |
+-------------------------------------------------------------------------+
| uv/Lhzm74syo8JlfWarwKA==                                                |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```