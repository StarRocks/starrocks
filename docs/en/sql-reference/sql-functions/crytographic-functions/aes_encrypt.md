---
displayed_sidebar: docs
---

# aes_encrypt



By default, use the AES_128_ECB algorithm to encrypt the string and return a binary string.

AES is short for advanced encryption standard and ECB is short for electronic code book. The key used to encrypt the string is a 128-bit string.

## Syntax

```Haskell
aes_encrypt(str, key_str[, init_vector][, encryption_mode][, aad_str]);
```

## Parameters
- Required parameters:

    - `str`: the string to encrypt. It must be of the VARCHAR type.

    - `key_str`: the key used to encrypt `str`. It must be of the VARCHAR type.
- Optional parameters:
    - `init_vector`: Initialization Vector (IV): A crucial security parameter in AES encryption that ensures identical plaintexts yield distinct ciphertexts. It is only utilized in CBC, CFB, OFB, CTR, and GCM modes. It must be of the VARCHAR type.
    - `encryption_mode`: The encryption algorithm. Supported algorithms are listed below. It must be of the VARCHAR type.

        | ECB         | CBC         | CFB         | CFB1        | CFB8        | CFB128        | OFB         | CTR       | GCM        |
        |-------------|-------------|-------------|-------------|-------------|---------------|-------------|-----------|------------|
        | AES_128_ECB | AES_128_CBC | AES_128_CFB | AES_128_CFB1| AES_128_CFB8| AES_128_CFB128| AES_128_OFB| AES_128_CTR| AES_128_GCM|
        | AES_192_ECB | AES_192_CBC | AES_192_CFB | AES_192_CFB1| AES_192_CFB8| AES_192_CFB128| AES_192_OFB| AES_192_CTR| AES_192_GCM|
        | AES_256_ECB | AES_256_CBC | AES_256_CFB | AES_256_CFB1| AES_256_CFB8| AES_256_CFB128| AES_256_OFB| AES_256_CTR| AES_256_GCM|

    - `aad_str`:  Denotes Additional Authenticated Data (AAD). This is a parameter unique to authenticated encryption modes (e.g., GCM). It allows for including data that must be authenticated for integrity (preventing tampering) but does not require confidentiality (it remains unencrypted). It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL, NULL is returned.

## Examples

Use this function to AES encrypt `starrocks` and convert the encrypted string into a Base64-encoded string.

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

