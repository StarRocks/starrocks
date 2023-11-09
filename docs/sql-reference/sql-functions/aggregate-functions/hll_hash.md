---
displayed_sidebar: "English"
---

# hll_hash

## Description

Converts a value to an hll type. Typically used in imports to map a value in the source data to an HLL column type in the StarRocks table.

## Syntax

```Haskell
HLL_HASH(column_name)
```

## Parameters

`column_name`: The name of the generated HLL column.

## Return value

Returns a value of the HLL type.

## Example

```plain text
mysql> select hll_cardinality(hll_hash("a"));
+--------------------------------+
| hll_cardinality(hll_hash('a')) |
+--------------------------------+
|                              1 |
+--------------------------------+
```
