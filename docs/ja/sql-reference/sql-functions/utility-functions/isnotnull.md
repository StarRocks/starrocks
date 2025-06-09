---
displayed_sidebar: docs
---

# isnotnull

## 説明

値が `NULL` でないかを確認し、`NULL` でない場合は `1` を返し、`NULL` の場合は `0` を返します。

## 構文

```Haskell
ISNOTNULL(v)
```

## パラメータ

- `v`: チェックする値。すべての日付型がサポートされています。

## 戻り値

`NULL` でない場合は 1 を返し、`NULL` の場合は 0 を返します。

## 例

```plain text
MYSQL > SELECT c1, isnotnull(c1) FROM t1;
+------+--------------+
| c1   | `c1` IS NULL |
+------+--------------+
| NULL |            0 |
|    1 |            1 |
+------+--------------+
```