---
displayed_sidebar: docs
description: "WGS84の経緯度座標を指定した解像度のH3セルインデックス（BIGINT）に変換します。"
---

# geoToH3

WGS84の経緯度座標点を、指定した解像度のH3六角形グリッドセルインデックスに変換します。戻り値はBIGINT型の64ビットH3セルインデックスで、ClickHouse・Snowflake・DatabricksのH3セマンティクスと互換性があります。

## Syntax

```Haskell
BIGINT geoToH3(DOUBLE lng, DOUBLE lat, INT resolution)
```

## パラメータ

- `lng`: 地点の経度（度、WGS84）。サポートされるデータ型: DOUBLE。
- `lat`: 地点の緯度（度、WGS84）。サポートされるデータ型: DOUBLE。
- `resolution`: H3解像度レベル（[0, 15]の範囲）。値が高いほど、より小さく精度の高いセルになります。サポートされるデータ型: INT。

## 戻り値

指定した座標点を含むH3セルインデックスをBIGINTで返します。引数がNULL、座標が有効範囲外、または解像度が[0, 15]の範囲外の場合はNULLを返します。

## 例

```sql
SELECT geoToH3(-122.4194, 37.7749, 9);
+--------------------------------+
| geoToH3(-122.4194, 37.7749, 9) |
+--------------------------------+
|          617700169958293503    |
+--------------------------------+
```

## キーワード

GEOTOH3,H3,GEO,SPATIAL
