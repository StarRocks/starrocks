---
<<<<<<< HEAD
unlisted: true
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
displayed_sidebar: docs
---

# url_extract_host

<<<<<<< HEAD
## Description

Extracts the host section from a URL.

=======


Extracts the host section from a URL.

This function is supported from v3.3 onwards.

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
## Syntax

```haskell
VARCHAR url_extract_host(VARCHAR str)
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
