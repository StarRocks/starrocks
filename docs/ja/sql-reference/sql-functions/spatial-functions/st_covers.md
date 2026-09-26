---
displayed_sidebar: docs
description: "境界を含めてポリゴンが点を覆うかどうかを判定します。"
---

# ST_Covers

点がポリゴン内部、外周、または穴の境界上にあるかを返します。

## 構文

```SQL
ST_Covers(GEOGRAPHY polygon, GEOGRAPHY point)
ST_Covers(GEOMETRY polygon, GEOMETRY point)
```

`polygon` は XY `POLYGON` または `MULTIPOLYGON`、`point` は XY `POINT` である必要があります。`GEOMETRY` ディスクリプタは一致している必要があります。`NULL` は伝播し、`EMPTY` 入力は `false` を返します。

## 例

```SQL
SELECT ST_Covers(
    ST_GeogFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_GeogFromText('POINT (0 5)'));
-- true
```

## キーワード

ST_COVERS,ST,COVERS
