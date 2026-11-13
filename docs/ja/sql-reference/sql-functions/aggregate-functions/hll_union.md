---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "HLL 値のセットを連結して返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hll_union

HLL 値のセットを連結して返します。

## 構文

```plaintext
hll_union(hll)
```

## 例

```Plain
mysql> select k1, hll_cardinality(hll_union(v1)) from tbl group by k1;
+------+----------------------------------+
| k1   | hll_cardinality(hll_union(`v1`)) |
+------+----------------------------------+
|    2 |                                4 |
|    1 |                                3 |
+------+----------------------------------+
```