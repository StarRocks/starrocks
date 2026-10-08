---
displayed_sidebar: docs
description: "ネイティブ GEO 値のポリゴン要素の境界長を返します。"
---

# ST_Perimeter

ネイティブ GEO 値に含まれる 2 次元要素の境界長を返します。外側リングと内側リングの両方を含みます。`GEOGRAPHY` の単位はメートル、`GEOMETRY` は入力 CRS の座標単位です。

## 構文

```SQL
ST_Perimeter(GEOGRAPHY value)
ST_Perimeter(GEOMETRY value)
```

`POLYGON`、`MULTIPOLYGON`、および `GEOMETRYCOLLECTION` 内のポリゴン要素を再帰的に合計します。点と線は 0 です。

`NULL` は `NULL`、`EMPTY` は `0` を返します。不正な WKB、未対応の次元または descriptor、トポロジ的に無効な入力は制御されたエラーになります。球面リングは `ST_Area` と同じ、向きに依存しない小領域モデルを使用します。

## 例

```SQL
SELECT ST_Perimeter(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857'));
-- 16
```

## キーワード

ST_PERIMETER,ST,PERIMETER,GEOGRAPHY,GEOMETRY
