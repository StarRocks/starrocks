---
displayed_sidebar: docs
description: "指定した解像度における一意のH3セルの総数を返します。"
---

# h3NumHexagons

指定した解像度における一意のH3セルの総数を返します。H3は階層型グリッドを使用し、解像度0では122個の基底セルがあり、解像度が上がるごとに約7倍のセル数になります。

## Syntax

```Haskell
BIGINT h3NumHexagons(INT resolution)
```

## パラメータ

- `resolution`: H3解像度レベル（0〜15）。サポートされるデータ型: INT。

## 戻り値

指定した解像度での一意のH3セルの総数をBIGINTで返します。引数がNULL、または有効範囲外の場合はNULLを返します。

## 例

```sql
SELECT h3NumHexagons(3);
+--------------------+
| h3NumHexagons(3)   |
+--------------------+
| 41162              |
+--------------------+
```

## キーワード

H3NUMHEXAGONS,H3,SPATIAL
