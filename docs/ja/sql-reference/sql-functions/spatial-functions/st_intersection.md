---
displayed_sidebar: docs
description: "2 つのネイティブ GEOMETRY ポリゴンの平面上の共通部分を返します。"
---

# ST_Intersection

2 つのネイティブ `GEOMETRY` 値の平面上の共通部分を計算します。入力は有効な XY `POLYGON` または `MULTIPOLYGON` で、CRS ディスクリプタが互換である必要があります。結果は入力 CRS を保持し、再投影しません。

## 構文

```SQL
ST_Intersection(GEOMETRY lhs, GEOMETRY rhs)
```

境界の関係によって、結果は点、線、ポリゴン、マルチジオメトリ、ジオメトリコレクション、または空ジオメトリになります。`NULL` は `NULL` を返します。無効なトポロジ、未対応の次元や入力種類、互換性のない CRS、および `GEOGRAPHY` 入力はエラーになります。

入力の倍精度座標に GEOS の標準の浮動小数点精度を使用します。StarRocks はスナップ用グリッドを指定せず、無効な入力を修復しません。結果の境界には浮動小数点の丸めが反映される場合があります。

## 例

```SQL
SELECT ST_AsText(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', 'EPSG:3857')));
```

この入力では、結果の `ST_Area` は CRS 単位の二乗で `1` です。

## キーワード

ST_INTERSECTION,ST,GEOMETRY,POLYGON
