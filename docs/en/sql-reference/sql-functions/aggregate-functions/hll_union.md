---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the concatenation of a set of HLL values."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---


# hll_union



Returns the concatenation of a set of HLL values.

## Syntax

```plaintext
hll_union(hll)
```

## Examples

```Plain
mysql> select k1, hll_cardinality(hll_union(v1)) from tbl group by k1;
+------+----------------------------------+
| k1   | hll_cardinality(hll_union(`v1`)) |
+------+----------------------------------+
|    2 |                                4 |
|    1 |                                3 |
+------+----------------------------------+
```
