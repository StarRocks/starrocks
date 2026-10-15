---
displayed_sidebar: docs
description: "ネイティブ GEO 値の次元加重重心を返します。"
---

# ST_Centroid

入力と同じネイティブ論理型および descriptor を持つ `POINT` 重心を返します。重心は加重された幾何中心であり、必ずしもポリゴン内部にあるとは限りません。

## 構文

```SQL
ST_Centroid(GEOGRAPHY value)
ST_Centroid(GEOMETRY value)
```

ポリゴンは面積、線は長さ、点は均等に重み付けされます。入力に存在する最も高い非空次元だけが寄与し、ポリゴン、線、点の順に優先されます。この規則は `MULTI*` と `GEOMETRYCOLLECTION` に再帰的に適用され、穴の面積とモーメントは減算されます。

`GEOGRAPHY` は単位球面上で計算し、OGC:CRS84 の経度/緯度順で返します。`GEOMETRY` は平面座標を使用します。結果は入力の CRS と論理型を保持し、座標変換は行いません。

`NULL` は `NULL`、`EMPTY` は `POINT EMPTY` を返します。定義できないゼロ球面ベクトル、または非空要素がない入力は `POINT EMPTY` を返します。不正な WKB、未対応の次元または descriptor、トポロジ的に無効な入力は制御されたエラーになります。

## 例

```SQL
SELECT ST_AsText(ST_Centroid(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857')));
-- POINT (2 2)
```

## キーワード

ST_CENTROID,ST,CENTROID,GEOGRAPHY,GEOMETRY
