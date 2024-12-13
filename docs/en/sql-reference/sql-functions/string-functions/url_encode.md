---
displayed_sidebar: docs
---

# url_encode

<<<<<<< HEAD
## Description

Converts an encode url string.
=======


Translates a string into the [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) format.

This functions is supported from v3.2.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

## Syntax

```haskell
<<<<<<< HEAD
url_encode(str)
=======
VARCHAR url_encode(VARCHAR str)
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
```

## Parameters

<<<<<<< HEAD
- `str`: the string to encode. If `str` is not a string type, it will try implicit cast first.

## Return values

Return an encode string.
=======
- `str`: the string to encode. If `str` is not a string, this function will try implicit cast first.

## Return value

Returns an encoded string compliant with the [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) format.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

## Examples

```plaintext
<<<<<<< HEAD
mysql> select url_encode('https://docs.starrocks.io/en-us/latest/quick_start/Deploy');
+-------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/en-us/latest/quick_start/Deploy') |
+-------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy |
+-------------------------------------------------------------------------+
=======
mysql> select url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/');
+----------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/') |
+----------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F |
+----------------------------------------------------------------------------+
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
```
