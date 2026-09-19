---
displayed_sidebar: docs
description: "2つの地理座標点間のHaversine（大圏）距離をメートル単位で返します。"
---

# h3PointDistM

緯度と経度（度単位）で指定した2つの地理座標点間のHaversine（大圏）距離をメートル単位で返します。

## Syntax

```Haskell
DOUBLE h3PointDistM(DOUBLE lat1, DOUBLE lon1, DOUBLE lat2, DOUBLE lon2)
```

## パラメータ

- `lat1`: 1つ目の地点の緯度（度）。サポートされるデータ型: DOUBLE。
- `lon1`: 1つ目の地点の経度（度）。サポートされるデータ型: DOUBLE。
- `lat2`: 2つ目の地点の緯度（度）。サポートされるデータ型: DOUBLE。
- `lon2`: 2つ目の地点の経度（度）。サポートされるデータ型: DOUBLE。

## 戻り値

2点間の大圏距離（メートル）をDOUBLEで返します。いずれかの引数がNULLの場合はNULLを返します。

## 例

```sql
SELECT h3PointDistM(-10.0, 0.0, 10.0, 0.0);
+--------------------------------------+
| h3PointDistM(-10.0, 0.0, 10.0, 0.0) |
+--------------------------------------+
| 2223901.039504589                    |
+--------------------------------------+
```

## キーワード

H3POINTDISTM,H3,SPATIAL
