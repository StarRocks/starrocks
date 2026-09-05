---
displayed_sidebar: docs
description: "H3セルインデックスから、そのセルの中心点の緯度（度）を返します。"
---

# h3ToGeoLat

指定したH3セルインデックスに対応するセル中心点の緯度を度単位（WGS84）で返します。

## Syntax

```Haskell
DOUBLE h3ToGeoLat(BIGINT h3index)
```

## パラメータ

- `h3index`: H3セルインデックス。サポートされるデータ型: BIGINT。

## 戻り値

H3セル中心点の緯度をDOUBLEで返します。引数がNULL、またはインデックスが有効なH3セルでない場合はNULLを返します。

## 例

```sql
SELECT h3ToGeoLat(617700169958293503);
+--------------------------------+
| h3ToGeoLat(617700169958293503) |
+--------------------------------+
|            37.77492951615992   |
+--------------------------------+
```

## キーワード

H3TOGEOLAT,H3,GEO,SPATIAL,LATITUDE
