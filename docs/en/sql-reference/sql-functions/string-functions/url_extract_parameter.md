---
displayed_sidebar: docs
---

# url_extract_parameter

## Description

Extracts the value of the requested `name` parameter from the query string of a URL. Parameter extraction is handled in the manner as specified in [RFC 1866#section-8.2.1](https://datatracker.ietf.org/doc/html/rfc1866.html#section-8.2.1). If the specified parameter name does not exist, NULL is returned.

This function is supported from v3.2.

## Syntax

```haskell
VARCHAR url_extract_parameter(VARCHAR str, VARCHAR name)
```

## Parameters

- `str`: the URL string to extract parameters.
- `name`: the name of the parameter in the query string.

## Return value

Returns a VARCHAR value.

## Examples

```plaintext
mysql> select url_extract_parameter("https://starrocks.io/doc?k1=10&k2=3&k1=100", "k1");
+---------------------------------------------------------------------------+
| url_extract_parameter('https://starrocks.io/doc?k1=10&k2=3&k1=100', 'k1') |
+---------------------------------------------------------------------------+
| 10                                                                        |
+---------------------------------------------------------------------------+

mysql> select url_extract_parameter('https://starrocks.com/doc?k0=10&k1=%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D%20%22%25%2D%2E%3C%3E%5C%5E%5F%60%7B%7C%7D%7E&k2','k1');
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| url_extract_parameter('https://starrocks.com/doc?k0=10&k1=%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D%20%22%25%2D%2E%3C%3E%5C%5E%5F%60%7B%7C%7D%7E&k2', 'k1') |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| !#$&'()*+,/:;=?@[] "%-.<>\^_`{|}~                                                                                                                                        |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
