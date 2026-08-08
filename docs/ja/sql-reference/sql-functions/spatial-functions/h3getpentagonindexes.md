---
displayed_sidebar: docs
description: "指定した解像度におけるすべての12個の五角形H3セルインデックスを返します。"
---

# h3GetPentagonIndexes

指定した解像度におけるすべての12個の五角形H3セルインデックスを返します。H3の各解像度には、二十面体の12個の頂点それぞれに1つずつ、合計12個の五角形があります。

## Syntax

```Haskell
ARRAY<BIGINT> h3GetPentagonIndexes(INT resolution)
```

## パラメータ

- `resolution`: H3解像度レベル（0〜15）。サポートされるデータ型: INT。

## 戻り値

指定した解像度での12個の五角形セルインデックスを含む`ARRAY<BIGINT>`を返します。引数がNULL、または有効範囲外の場合はNULLを返します。

## 例

```sql
SELECT array_length(h3GetPentagonIndexes(3));
+---------------------------------------+
| array_length(h3GetPentagonIndexes(3)) |
+---------------------------------------+
| 12                                    |
+---------------------------------------+
```

## キーワード

H3GETPENTAGONINDEXES,H3,SPATIAL
