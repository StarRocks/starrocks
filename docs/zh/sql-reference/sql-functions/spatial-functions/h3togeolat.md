---
displayed_sidebar: docs
description: "返回 H3 单元中心点的纬度（度）。"
---

# h3ToGeoLat

返回给定 H3 单元索引所对应单元中心点的纬度，单位为度（WGS84）。

## 语法

```Haskell
DOUBLE h3ToGeoLat(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 DOUBLE 类型的 H3 单元中心点纬度（度）。如果参数为 NULL 或索引不是有效的 H3 单元，则返回 NULL。

## 示例

```sql
SELECT h3ToGeoLat(617700169958293503);
+--------------------------------+
| h3ToGeoLat(617700169958293503) |
+--------------------------------+
|            37.77492951615992   |
+--------------------------------+
```

## 关键词

H3TOGEOLAT,H3,GEO,SPATIAL,LATITUDE
