---
displayed_sidebar: docs
description: "返回两个地理坐标点之间的 Haversine（大圆）距离（以米为单位）。"
---

# h3PointDistM

返回两个地理坐标点之间的 Haversine（大圆）距离（以米为单位）。坐标点以度为单位的纬度和经度表示。

## 语法

```Haskell
DOUBLE h3PointDistM(DOUBLE lat1, DOUBLE lon1, DOUBLE lat2, DOUBLE lon2)
```

## 参数说明

- `lat1`：第一个坐标点的纬度（度）。支持的数据类型为 DOUBLE。
- `lon1`：第一个坐标点的经度（度）。支持的数据类型为 DOUBLE。
- `lat2`：第二个坐标点的纬度（度）。支持的数据类型为 DOUBLE。
- `lon2`：第二个坐标点的经度（度）。支持的数据类型为 DOUBLE。

## 返回值说明

返回 DOUBLE 类型，表示两点之间的大圆距离（米）。如果任意参数为 NULL，则返回 NULL。

## 示例

```sql
SELECT h3PointDistM(-10.0, 0.0, 10.0, 0.0);
+--------------------------------------+
| h3PointDistM(-10.0, 0.0, 10.0, 0.0) |
+--------------------------------------+
| 2223901.039504589                    |
+--------------------------------------+
```

## 关键词

H3POINTDISTM,H3,SPATIAL
