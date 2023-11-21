---
displayed_sidebar: "English"
---

# URL_ENCODE

## Description

Converts an encode url string.

## Syntax

```haskell
url_encode(str)
```

## Parameters

- `str`: the string to encode. If `str` is not a string type, it will try implicit cast first.

## Return values

Return an encode string.

## Examples

```plaintext
mysql> select url_encode('https://docs.starrocks.io/en-us/latest/quick_start/Deploy');
+-------------------------------------------------------------------------+
| url_encode('https://docs.starrocks.io/en-us/latest/quick_start/Deploy') |
+-------------------------------------------------------------------------+
| https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy |
+-------------------------------------------------------------------------+
```
