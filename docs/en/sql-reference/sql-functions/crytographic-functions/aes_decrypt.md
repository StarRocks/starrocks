---
displayed_sidebar: docs
---

# aes_decrypt



By default, use the AES_128_ECB algorithm to decrypt a string and returns a binary string.

AES is short for advanced encryption standard and ECB is short for electronic code book. The key used to encrypt the string is a 128-bit string.

## Syntax

```Haskell
aes_decrypt(str, key_str[, init_vector][, encryption_mode][, aad_str]);
```

## Parameters
- Required parameters:


    - `str`: the string to decrypt. It must be of the VARCHAR type.

    - `key_str`: the key used to encrypt `str`. It must be of the VARCHAR type.
  - 
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

Returns a value of the VARCHAR type. If the input is invalid, NULL is returned.

## Examples

Decode a Base64 string and use this function to decrypt the decoded string into the original string.

```Plain Text
mysql> select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA==  '),'F3229A0B371ED2D9441B830D21A390C3');
+--------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('uv/Lhzm74syo8JlfWarwKA==  '), 'F3229A0B371ED2D9441B830D21A390C3') |
+--------------------------------------------------------------------------------------------+
| starrocks                                                                                  |
+--------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
```
mysql> select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA=='),'F3229A0B371ED2D9441B830D21A390C3', NULL, "AES_128_ECB");
+---------------------------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('uv/Lhzm74syo8JlfWarwKA=='), 'F3229A0B371ED2D9441B830D21A390C3', NULL, 'AES_128_ECB') |
+---------------------------------------------------------------------------------------------------------------+
| starrocks                                                                                                     |
+---------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
```
mysql> select AES_DECRYPT(from_base64('taXlwIvir9yff94F5Uv/KA=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefg", "AES_128_CBC");
+--------------------------------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('taXlwIvir9yff94F5Uv/KA=='), 'F3229A0B371ED2D9441B830D21A390C3', 'abcdefg', 'AES_128_CBC') |
+--------------------------------------------------------------------------------------------------------------------+
| starrocks                                                                                                          |
+--------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
```
mysql> select AES_DECRYPT(from_base64('YWJjZGVmZ2hpamtsdpJC2rnrGmvqKQv/WcoO6NuOCXvUnC8pCw=='),'F3229A0B371ED2D9441B830D21A390C3', "abcdefghijklmnop", "AES_128_GCM", "abcdefg");
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('YWJjZGVmZ2hpamtsdpJC2rnrGmvqKQv/WcoO6NuOCXvUnC8pCw=='), 'F3229A0B371ED2D9441B830D21A390C3', 'abcdefghijklmnop', 'AES_128_GCM', 'abcdefg') |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| starrocks                                                                                                                                                          |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
