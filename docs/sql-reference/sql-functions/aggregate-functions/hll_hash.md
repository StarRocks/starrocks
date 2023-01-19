# HLL_HASH

## Description

Converts a value to an hll type. Typically used in imports to map a value in the source data to an HLL column type in the Starrocks table.

## Syntax

```Haskell
HLL_HASH(column_name)
```

## Parameters

`column_name`: The new generated hll column name.

## Return value

Returns a value of the hll type.

## Example

```plain text
mysql> select hll_cardinality(hll_hash("a"));
+--------------------------------+
| hll_cardinality(hll_hash('a')) |
+--------------------------------+
|                              1 |
+--------------------------------+
```