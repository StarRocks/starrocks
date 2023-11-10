# URL_DECODE

## Description

Converts an url to a decode string.

## Syntax

```haskell
url_decode(str)
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
