---
displayed_sidebar: docs
description: "境界を含めて点がポリゴンに覆われるかどうかを判定します。"
---

# ST_CoveredBy

点がポリゴン内部、外周、または穴の境界上にあるかを返します。`ST_Covers` の逆です。

## 構文

```SQL
ST_CoveredBy(GEOGRAPHY point, GEOGRAPHY polygon)
ST_CoveredBy(GEOMETRY point, GEOMETRY polygon)
```

`polygon` は XY `POLYGON` または `MULTIPOLYGON`、`point` は XY `POINT` である必要があります。`GEOMETRY` ディスクリプタは一致している必要があります。`NULL` は伝播し、`EMPTY` 入力は `false` を返します。

## 例

```SQL
SELECT ST_CoveredBy(
    ST_GeomFromText('POINT (0 5)', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'));
-- true
```

## キーワード

ST_COVEREDBY,ST,COVEREDBY
