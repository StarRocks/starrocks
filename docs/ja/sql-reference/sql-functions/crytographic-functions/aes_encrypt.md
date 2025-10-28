---
displayed_sidebar: docs
---

# aes_encrypt

デフォルトでは、AES_128_ECBアルゴリズムを使用して文字列strを暗号化し、バイナリ文字列を返します。

AES は advanced encryption standard の略で、ECB は electronic code book の略です。文字列を暗号化するために使用されるキーは 128 ビットの文字列です。

## 構文

```Haskell
aes_encrypt(str, key_str[, init_vector][, encryption_mode][, aad_str]);
```

## パラメータ

- 必須パラメータ:

    - `str`: 暗号化する文字列。VARCHAR 型でなければなりません。

    - `key_str`: `str` を暗号化するために使用されるキー。VARCHAR 型でなければなりません。
- オプションパラメータ:
    - `init_vector`: Initialization Vector (IV) は、AES暗号化において重要なセキュリティパラメータであり、同一の平文が暗号化されるごとに異なる暗号文を生成することを保証します。CBC、CFB、OFB、CTR、GCM モードでのみ有効です。VARCHAR 型でなければなりません。
    - `encryption_mode`: 対応アルゴリズムは以下の通りです。VARCHAR 型でなければなりません。

        | ECB         | CBC         | CFB         | CFB1        | CFB8        | CFB128        | OFB         | CTR       | GCM        |
        |-------------|-------------|-------------|-------------|-------------|---------------|-------------|-----------|------------|
        | AES_128_ECB | AES_128_CBC | AES_128_CFB | AES_128_CFB1| AES_128_CFB8| AES_128_CFB128| AES_128_OFB| AES_128_CTR| AES_128_GCM|
        | AES_192_ECB | AES_192_CBC | AES_192_CFB | AES_192_CFB1| AES_192_CFB8| AES_192_CFB128| AES_192_OFB| AES_192_CTR| AES_192_GCM|
        | AES_256_ECB | AES_256_CBC | AES_256_CFB | AES_256_CFB1| AES_256_CFB8| AES_256_CFB128| AES_256_OFB| AES_256_CTR| AES_256_GCM|
    - `aad_str` 附加認証データ（AAD）を指します。これはGCMなどの認証暗号モードに特有の概念であり、暗号化プロセスにおいて暗号化されないデータです。VARCHAR 型でなければなりません。

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