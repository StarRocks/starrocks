---
displayed_sidebar: docs
---

# bitmap_union_int

## 説明

TINYINT、SMALLINT、INT 型の列における異なる値の数をカウントし、COUNT (DISTINCT expr) の合計を返します。

## 構文

```Haskell
BIGINT bitmap_union_int(expr)
```

### パラメータ

`expr`: 列の式。サポートされている列の型は TINYINT、SMALLINT、INT です。

## 戻り値

BIGINT 型の値を返します。

## 例

```Plaintext
mysql> select bitmap_union_int(k1) from tbl1;
+------------------------+
| bitmap_union_int(`k1`) |
+------------------------+
|                      2 |
+------------------------+
```
