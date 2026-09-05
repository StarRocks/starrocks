---
displayed_sidebar: docs
description: "有向H3辺の正確な長さをメートル単位で返します。"
---

# h3ExactEdgeLengthM

有向H3辺の正確な長さをメートル単位で返します。解像度の平均辺長を返す`h3EdgeLengthM`とは異なり、この関数は個々の辺の正確な測地長を計算します。

## Syntax

```Haskell
DOUBLE h3ExactEdgeLengthM(BIGINT h3edge)
```

## パラメータ

- `h3edge`: 有向H3辺インデックス。サポートされるデータ型: BIGINT。

## 戻り値

辺の正確な長さ（メートル）をDOUBLEで返します。引数がNULL、またはインデックスが有効なH3有向辺でない場合はNULLを返します。

## 例

```sql
SELECT h3ExactEdgeLengthM(1310277011704381439);
+-----------------------------------------+
| h3ExactEdgeLengthM(1310277011704381439) |
+-----------------------------------------+
| 195449.63163407316                      |
+-----------------------------------------+
```

## キーワード

H3EXACTEDGELENGTHM,H3,SPATIAL
