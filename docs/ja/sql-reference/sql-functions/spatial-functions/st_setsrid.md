---
displayed_sidebar: docs
sidebar_position: 51
description: "座標を変換せずにネイティブ GEOMETRY の CRS メタデータを変更します。"
---

# ST_SetSRID

ネイティブ `GEOMETRY` に EPSG:4326 または EPSG:3857 の CRS メタデータを割り当てます。WKB バイト列と座標は変わりません。再投影には [ST_Transform](st_transform.md) を使用してください。

## 構文

```SQL
GEOMETRY ST_SetSRID(GEOMETRY value, INT target_srid)
```

`target_srid` は FE が定数に畳み込める 4326 または 3857 に限ります。入力 CRS は EPSG:4326、EPSG:3857、または OGC:CRS84 です。この範囲制限により、外部 CRS データベースなしで 2 次元メタデータを保持できます。SQL 座標は X/Y 順で、EPSG:4326 では経度/緯度順です。7 種類の OGC family、collection、`EMPTY` を含む XY WKB をサポートします。`NULL` は `NULL` を返します。不正な WKB や矛盾した CRS メタデータはエラーです。`GEOGRAPHY` は受け付けません。

## 例

```SQL
SELECT ST_SRID(ST_SetSRID(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- 3857。点の座標は (10, 20) のままです。
```

## キーワード

ST_SETSRID, GEOMETRY, CRS, EPSG
