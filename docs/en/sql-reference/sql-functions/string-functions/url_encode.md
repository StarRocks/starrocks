---
displayed_sidebar: docs
---

# url_encode

## Description

Translates a string into the [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) format.

This functions is supported from v3.2.

## Syntax

```haskell
VARCHAR url_encode(VARCHAR str)
```

## Parameters

- `str`: the string to encode. If `str` is not a string, this function will try implicit cast first.

## Return value

Returns an encoded string compliant with the [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) format.

## Examples

```plaintext
mysql> select url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/');
+----------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/docs/introduction/StarRocks_intro/') |
+----------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F |
+----------------------------------------------------------------------------+
```
