---
displayed_sidebar: docs
description: "2 つのネイティブなポリゴンまたはマルチポリゴンが交差するかを判定します。"
---

# ST_Intersects

2 つのネイティブなポリゴンまたはマルチポリゴンが点を共有するかを返します。内部の重なり、包含、および外周や穴の境界での接触は `true` になります。一方のポリゴンが他方の穴の内部に完全に収まる場合は `false` です。

## 構文

```SQL
ST_Intersects(GEOGRAPHY lhs, GEOGRAPHY rhs)
ST_Intersects(GEOMETRY lhs, GEOMETRY rhs)
```

両方の引数は、同じネイティブ論理型の XY `POLYGON` または `MULTIPOLYGON` である必要があります。`GEOGRAPHY` は球面 OGC:CRS84 エッジを使用します。`GEOMETRY` は平面エッジを使用し、両方の引数に互換性のあるディスクリプタが必要です。この関数は座標変換を行いません。

`NULL` は伝播し、いずれかの入力が `EMPTY` の場合は `false` を返します。未対応のジオメトリファミリ、`GEOMETRYCOLLECTION`、`GEOGRAPHY`/`GEOMETRY` の混在、および互換性のない `GEOMETRY` ディスクリプタはエラーになります。

## 例

```SQL
SELECT ST_Intersects(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0))', 'EPSG:3857'));
-- true：2 つのポリゴンは辺を共有しています

SELECT ST_Intersects(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))', 'EPSG:3857'));
-- false：2 番目のポリゴンは穴の内部にあります
```

## キーワード

ST_INTERSECTS,ST,INTERSECTS,GEOGRAPHY,GEOMETRY
