---
displayed_sidebar: docs
description: "ネイティブ GEOGRAPHY または GEOMETRY 値の 2 次元面積を返します。"
---

# ST_Area

ネイティブ GEO 値に含まれる 2 次元要素の面積を返します。`GEOGRAPHY` は StarRocks の球面地球モデルを使用し、単位は平方メートルです。`GEOMETRY` は入力 CRS の座標単位の二乗です。座標変換は行いません。

## 構文

```SQL
ST_Area(GEOGRAPHY value)
ST_Area(GEOMETRY value)
```

`POLYGON` と `MULTIPOLYGON` は塗りつぶされた面積を加算し、穴の面積を減算します。`GEOMETRYCOLLECTION` 内のポリゴン要素は再帰的に合計され、点と線は 0 を加算します。重なるポリゴン要素はトポロジ的に無効なため、二重計上せずエラーになります。

`GEOGRAPHY` ではリングの向きで内部を選択しません。各リングは球面上の小さい領域に正規化されるため、1 個のリングで半球を超える領域は表現できません。穴と MultiPolygon も同じ規則に従います。

`NULL` は `NULL`、`EMPTY` は `0` を返します。不正な WKB、未対応の次元または descriptor、トポロジ的に無効な入力は制御されたエラーになります。

## 例

```SQL
SELECT ST_Area(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))',
    'EPSG:3857'));
-- 15
```

## キーワード

ST_AREA,ST,AREA,GEOGRAPHY,GEOMETRY
