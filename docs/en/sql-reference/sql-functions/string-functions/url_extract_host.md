---
displayed_sidebar: "English"
unlisted: true
---

# url_extract_host

## Description

Extracts the host section from a URL.

## Syntax

```haskell
VARCHAR url_extract_host(VARACHR str)
```

## Parameters

- `str`: the string to extract its host string. If `str` is not a string, this function will try implicit cast first.

## Return value

Returns the host string.

## Examples

```plaintext
mysql> select url_extract_host('httpa://starrocks.com/test/api/v1');
+-------------------------------------------------------+
| url_extract_host('httpa://starrocks.com/test/api/v1') |
+-------------------------------------------------------+
| starrocks.com                                         |
+-------------------------------------------------------+
```
