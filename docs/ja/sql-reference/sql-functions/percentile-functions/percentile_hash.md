---
displayed_sidebar: docs
---

# percentile_hash

DOUBLE 値を PERCENTILE 値として構築します。

## Syntax

```Haskell
PERCENTILE_HASH(x);
```

## Parameters

`x`: サポートされているデータ型は DOUBLE です。

## Return value

PERCENTILE 値を返します。

## Examples

```Plain Text
mysql> select percentile_approx_raw(percentile_hash(234.234), 0.99);
+-------------------------------------------------------+
| percentile_approx_raw(percentile_hash(234.234), 0.99) |
+-------------------------------------------------------+
|                                    234.23399353027344 |
+-------------------------------------------------------+
1 row in set (0.00 sec)
```