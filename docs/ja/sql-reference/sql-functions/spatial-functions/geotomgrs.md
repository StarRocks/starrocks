---
displayed_sidebar: docs
description: "WGS84の経緯度座標を軍事格子座標系（MGRS）文字列にエンコードします。"
---

# geoToMGRS

WGS84の（経度、緯度）座標を[軍事格子座標系（MGRS）](https://en.wikipedia.org/wiki/Military_Grid_Reference_System)文字列にエンコードします。オプションの `precision` 引数で東距/北距の桁数を制御します（デフォルト5＝1m精度、0＝100km方格のみ）。UTM範囲[−80°, 84°]の緯度のみ対応。

## Syntax

```Haskell
VARCHAR geoToMGRS(DOUBLE longitude, DOUBLE latitude[, INT precision])
```

## パラメータ

- `longitude`: 経度（度）。範囲: `[-180, 180]`。サポートされるデータ型: DOUBLE。
- `latitude`: 緯度（度）。範囲: `[-80, 84]`。サポートされるデータ型: DOUBLE。
- `precision`: オプション。東距/北距それぞれの桁数。範囲: `[0, 5]`、デフォルト `5`。サポートされるデータ型: INT。

## 戻り値

MGRS参照文字列（VARCHAR）を返します。`latitude`が`[-80, 84]`外、`longitude`が`[-180, 180]`外、`precision`が`[0, 5]`外、またはいずれかの引数がNULLの場合はNULLを返します。

## 例

```sql
SELECT geoToMGRS(2.294497, 48.858222);
+--------------------------------+
| geoToMGRS(2.294497, 48.858222) |
+--------------------------------+
| 31UDQ4825111935                |
+--------------------------------+

SELECT geoToMGRS(2.294497, 48.858222, 3);
+-----------------------------------+
| geoToMGRS(2.294497, 48.858222, 3) |
+-----------------------------------+
| 31UDQ482119                       |
+-----------------------------------+
```

## キーワード

GEOTOMGRS,MGRS,GEO,SPATIAL,UTM
