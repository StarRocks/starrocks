---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "値がNULLでないかどうかを確認し、結果を1または0で返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# isnotnull

値が `NULL` でないかを確認し、`NULL` でない場合は `1` を返し、`NULL` の場合は `0` を返します。

## Syntax

```plaintext
ISNOTNULL(v)
```

## Parameters

- `v`: チェックする値。すべての日付型がサポートされています。

## Return value

`NULL` でない場合は 1 を返し、`NULL` の場合は 0 を返します。

## Examples

```plain text
MYSQL > SELECT c1, isnotnull(c1) FROM t1;
+------+--------------+
| c1   | `c1` IS NULL |
+------+--------------+
| NULL |            0 |
|    1 |            1 |
+------+--------------+
```