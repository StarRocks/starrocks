---
displayed_sidebar: docs
description: "点がポリゴン内部にあるかどうかを判定します。"
---

# ST_Within

点がポリゴン内部にあるかを返します。`ST_Contains` の逆で、外周および穴の境界を含みません。

## 構文

```SQL
ST_Within(GEOGRAPHY point, GEOGRAPHY polygon)
ST_Within(GEOMETRY point, GEOMETRY polygon)
```

`polygon` は XY `POLYGON` または `MULTIPOLYGON`、`point` は XY `POINT` である必要があります。`GEOMETRY` ディスクリプタは一致している必要があります。`NULL` は伝播し、`EMPTY` 入力は `false` を返します。

## 例

```SQL
SELECT ST_Within(
    ST_GeomFromText('POINT (5 5)', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'));
-- true
```

## キーワード

ST_WITHIN,ST,WITHIN
