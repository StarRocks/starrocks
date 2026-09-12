---
displayed_sidebar: docs
description: "H3セルインデックスから、そのセルの中心点の経度（度）を返します。"
---

# h3ToGeoLng

指定したH3セルインデックスに対応するセル中心点の経度を度単位（WGS84）で返します。

## Syntax

```Haskell
DOUBLE h3ToGeoLng(BIGINT h3index)
```

## パラメータ

- `h3index`: H3セルインデックス。サポートされるデータ型: BIGINT。

## 戻り値

H3セル中心点の経度をDOUBLEで返します。引数がNULL、またはインデックスが有効なH3セルでない場合はNULLを返します。

## 例

```sql
SELECT h3ToGeoLng(617700169958293503);
+--------------------------------+
| h3ToGeoLng(617700169958293503) |
+--------------------------------+
|          -122.41935029526676   |
+--------------------------------+
```

## キーワード

H3TOGEOLONG,H3,GEO,SPATIAL,LONGITUDE
