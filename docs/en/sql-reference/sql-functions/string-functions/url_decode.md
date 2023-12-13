---
displayed_sidebar: "English"
---

# url_decode

## Description

Translates a string back from the [application/x-www-form-urlencoded](https://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1) format. This function is an inverse of [url_encode](./url_encode.md).

This functions is supported from v3.2.

## Syntax

```haskell
VARCAHR url_decode(VARCHAR str)
```

## Parameters

- `str`: the string to decode. If `str` is not a string, the system will try implicit cast first.

## Return value

Returns a decoded string.

## Examples

```plaintext
mysql> select url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F');
+------------------------------------------------------------------------------------------+
| url_decode('https%3A%2F%2Fdocs.starrocks.io%2Fdocs%2Fintroduction%2FStarRocks_intro%2F') |
+------------------------------------------------------------------------------------------+
| https://docs.starrocks.io/docs/introduction/StarRocks_intro/                             |
+------------------------------------------------------------------------------------------+
```
