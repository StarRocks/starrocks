---
displayed_sidebar: docs
---

# isnull

値が `NULL` かどうかを確認し、`NULL` の場合は `1` を返し、`NULL` でない場合は `0` を返します。

## 構文

```Haskell
ISNULL(v)
```

## パラメータ

- `v`: チェックする値。すべての日付型がサポートされています。

## 戻り値

`NULL` の場合は 1 を返し、`NULL` でない場合は 0 を返します。

## 例

```plain text
MYSQL > SELECT c1, isnull(c1) FROM t1;
+------+--------------+
| c1   | `c1` IS NULL |
+------+--------------+
| NULL |            1 |
|    1 |            0 |
+------+--------------+
```