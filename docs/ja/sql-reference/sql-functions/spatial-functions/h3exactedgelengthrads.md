---
displayed_sidebar: docs
description: "有向H3辺の正確な長さをラジアン単位で返します。"
---

# h3ExactEdgeLengthRads

有向H3辺の正確な長さをラジアン単位（単位球面上の大円弧長）で返します。

## Syntax

```Haskell
DOUBLE h3ExactEdgeLengthRads(BIGINT h3edge)
```

## パラメータ

- `h3edge`: 有向H3辺インデックス。サポートされるデータ型: BIGINT。

## 戻り値

辺の正確な長さ（ラジアン）をDOUBLEで返します。引数がNULL、またはインデックスが有効なH3有向辺でない場合はNULLを返します。

## 例

```sql
SELECT h3ExactEdgeLengthRads(1310277011704381439);
+--------------------------------------------+
| h3ExactEdgeLengthRads(1310277011704381439) |
+--------------------------------------------+
| 0.030677980118976447                       |
+--------------------------------------------+
```

## キーワード

H3EXACTEDGELENGTHRADS,H3,SPATIAL
