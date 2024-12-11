---
displayed_sidebar: docs
---

# url_decode

<<<<<<< HEAD
## Description

Converts an url to a decode string.
=======


Translates a string back from the [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) format. This function is an inverse of [url_encode](./url_encode.md).

This functions is supported from v3.2.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## Syntax

```haskell
<<<<<<< HEAD
url_decode(str)
=======
VARCHAR url_decode(VARCHAR str)
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
```

## Parameters

<<<<<<< HEAD
- `str`: the string to decode. If `str` is not a string type, it will try implicit cast first.

## Return values

Return an encode string.
=======
- `str`: the string to decode. If `str` is not a string, the system will try implicit cast first.

## Return value

Returns a decoded string.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## Examples

```plaintext
<<<<<<< HEAD
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy');
+---------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy') |
+---------------------------------------------------------------------------------------+
| https://docs.starrocks.io/en-us/latest/quick_start/Deploy                             |
+---------------------------------------------------------------------------------------+
=======
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F');
+------------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F') |
+------------------------------------------------------------------------------------------+
| https://docs.starrocks.io/docs/introduction/StarRocks_intro/                             |
+------------------------------------------------------------------------------------------+
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
```
