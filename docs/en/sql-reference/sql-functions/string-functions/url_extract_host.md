---
displayed_sidebar: "English"
---

# URL_EXTRACT_HOST

## Description

extract host from url string.

## Syntax

```haskell
url_extract_host(str)
```

## Parameters

- `str`: the string to extract its host string. If `str` is not a string type, it will try implicit cast first.

## Return values

Return an encode string.

## Examples

```plaintext
mysql> select url_extract_host('httpa://starrocks.com/test/api/v1');
+-------------------------------------------------------+
| url_extract_host('httpa://starrocks.com/test/api/v1') |
+-------------------------------------------------------+
| starrocks.com                                         |
+-------------------------------------------------------+
```