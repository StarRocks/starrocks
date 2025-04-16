---
displayed_sidebar: docs
---

# isnotnull

値が `NULL` でないかを確認し、`NULL` でない場合は `1` を返し、`NULL` の場合は `0` を返します。

## Syntax

```Haskell
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