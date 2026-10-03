---
displayed_sidebar: docs
description: "Constructs DOUBLE values as PERCENTILE values."
---

# percentile_hash



Constructs DOUBLE values as PERCENTILE values.

## Syntax

```Haskell
PERCENTILE_HASH(x[, compression]);
```

## Parameters

`x`: The supported data type is DOUBLE.

`compression`: Optional constant expression with an integer value in [2048, 10000]. Decimal notation such as `5000.0`, casts, and constant arithmetic are accepted when the result is integral. Fractional and non-constant values are rejected. Explicit `NULL` and out-of-range integers use `10000`. Omitting this argument preserves the legacy compression of `1000`.

Upgrade all BE and CN nodes to a version supporting the two-argument overload before using it.

## Return value

Returns a PERCENTILE value.

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
