---
displayed_sidebar: docs
sidebar_position: 52
description: "EPSG:4326 と EPSG:3857 の間でネイティブ GEOMETRY の座標を再投影します。"
---

# ST_Transform

ネイティブ `GEOMETRY` を EPSG:4326（経度/緯度）と EPSG:3857（Web Mercator）の間で再投影します。結果 descriptor にターゲット CRS が記録されます。[ST_SetSRID](st_setsrid.md) と異なり、座標が変わります。Web Mercator の数式を使用し、外部 CRS ライブラリは不要です。

## 構文

```SQL
GEOMETRY ST_Transform(GEOMETRY value, INT target_srid)
```

`target_srid` は FE が定数に畳み込める 4326 または 3857 に限ります。入力 CRS は EPSG:4326、EPSG:3857、または OGC:CRS84（この関数では EPSG:4326 の別名）です。EPSG:4326 の SQL X/Y は経度/緯度を意味します。入力と出力の SRID が同じ場合、座標は変わりません。

順変換では経度 -180～180 度、緯度約 -85.05112878～85.05112878 度を受け付けます。逆変換では Web Mercator の X/Y を約 ±20037508.3427893 メートル以内に制限します。範囲外の座標、未対応 CRS、不正な WKB、矛盾した descriptor はエラーです。

7 種類の XY OGC family と入れ子の collection の全座標を変換します。`EMPTY` は空のまま、`NULL` は `NULL` です。Z/M 座標と `GEOGRAPHY` はサポートしません。

## 例

```SQL
SELECT ST_X(ST_Transform(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- 約 1113194.90793274 メートル
```

## キーワード

ST_TRANSFORM, GEOMETRY, CRS, EPSG, Web Mercator
