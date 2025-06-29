---
displayed_sidebar: docs
---

# aes_decrypt

AES_128_ECB アルゴリズムを使用して文字列を復号し、バイナリ文字列を返します。

AES は advanced encryption standard の略で、ECB は electronic code book の略です。文字列を暗号化するために使用されるキーは 128 ビットの文字列です。

## Syntax

```Haskell
aes_decrypt(str,key_str);
```

## Parameters

`str`: 復号する文字列。VARCHAR 型でなければなりません。

`key_str`: `str` を暗号化するために使用されるキー。VARCHAR 型でなければなりません。

## Return value

VARCHAR 型の値を返します。入力が無効な場合は、NULL が返されます。

## Examples

Base64 文字列をデコードし、この関数を使用してデコードされた文字列を元の文字列に復号します。

```Plain Text
mysql> select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA==  '),'F3229A0B371ED2D9441B830D21A390C3');
+--------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('uv/Lhzm74syo8JlfWarwKA==  '), 'F3229A0B371ED2D9441B830D21A390C3') |
+--------------------------------------------------------------------------------------------+
| starrocks                                                                                  |
+--------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```