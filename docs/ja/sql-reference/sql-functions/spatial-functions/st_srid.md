---
displayed_sidebar: docs
sidebar_position: 50
description: "ネイティブ GEOMETRY の descriptor に記録された数値 EPSG SRID を返します。"
---

# ST_SRID

ネイティブ `GEOMETRY` の CRS に対応する数値 SRID を返します。`OGC:CRS84` は 4326 に対応します。明確な数値 EPSG 対応がない CRS では `NULL` を返し、EPSG:0 に置き換えません。

## 構文

```SQL
INT ST_SRID(GEOMETRY value)
```

入力が `NULL` の場合は `NULL` を返します。メタデータを読み取るだけで、座標は変換しません。`GEOGRAPHY` は受け付けません。

## 例

```SQL
SELECT ST_SRID(ST_GeomFromText('POINT (10 20)', 'EPSG:4326'));
-- 4326
```

## キーワード

ST_SRID, GEOMETRY, CRS, EPSG
