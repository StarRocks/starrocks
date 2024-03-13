---
displayed_sidebar: "English"
---

# url_decode

## Description

Converts an url to a decode string.

## Syntax

```haskell
<<<<<<< HEAD
url_decode(str)
=======
VARCHAR url_decode(VARCHAR str)
>>>>>>> 9983aa0b8e ([Doc] fix spelling errors (backport #42490) (#42526))
```

## Parameters

- `str`: the string to decode. If `str` is not a string type, it will try implicit cast first.

## Return values

Return an encode string.

## Examples

```plaintext
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy');
+---------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy') |
+---------------------------------------------------------------------------------------+
| https://docs.starrocks.io/en-us/latest/quick_start/Deploy                             |
+---------------------------------------------------------------------------------------+
```
