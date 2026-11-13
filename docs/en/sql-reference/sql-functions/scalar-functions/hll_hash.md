---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a value to an hll type."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hll_hash



Converts a value to an hll type. Typically used in imports to map a value in the source data to an HLL column type in the StarRocks table.

## Syntax

```plaintext
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
