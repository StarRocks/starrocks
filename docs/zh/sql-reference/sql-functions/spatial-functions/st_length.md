---
displayed_sidebar: docs
description: "返回原生 GEO 值中一维分量的长度。"
---

# ST_Length

返回原生 GEO 值中一维分量的总长度。`GEOGRAPHY` 沿大圆边计算，单位为米；`GEOMETRY` 使用输入 CRS 的坐标单位。

## 语法

```SQL
ST_Length(GEOGRAPHY value)
ST_Length(GEOMETRY value)
```

`LINESTRING`、`MULTILINESTRING` 以及 `GEOMETRYCOLLECTION` 中的线分量会递归求和。点和面的边界贡献 0；面的边界长度请使用 `ST_Perimeter`。

`NULL` 返回 `NULL`，`EMPTY` 返回 `0`。WKB 格式错误、不支持的维度或 descriptor、球面上的对跖歧义线段以及拓扑无效输入会返回受控错误。

## 示例

```SQL
SELECT ST_Length(ST_GeomFromText('LINESTRING (0 0, 3 4)', 'EPSG:3857'));
-- 5

SELECT ST_Length(ST_GeogFromText('LINESTRING (0 0, 1 0)'));
-- 约 111195.101177484 米
```

## 关键字

ST_LENGTH,ST,LENGTH,GEOGRAPHY,GEOMETRY
