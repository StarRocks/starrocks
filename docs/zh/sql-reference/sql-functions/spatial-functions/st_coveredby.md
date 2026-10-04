---
displayed_sidebar: docs
description: "判断点是否被多边形覆盖，包括边界。"
---

# ST_CoveredBy

判断点是否位于多边形内部、外环边界或孔洞边界上。该函数是 `ST_Covers` 的反向关系。

## 语法

```SQL
ST_CoveredBy(GEOGRAPHY point, GEOGRAPHY polygon)
ST_CoveredBy(GEOMETRY point, GEOMETRY polygon)
```

`polygon` 必须是 XY `POLYGON` 或 `MULTIPOLYGON`，`point` 必须是 XY `POINT`。`GEOMETRY` 描述符必须匹配。`NULL` 传递，`EMPTY` 输入返回 `false`。

## 示例

```SQL
SELECT ST_CoveredBy(
    ST_GeomFromText('POINT (0 5)', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'));
-- true
```

## 关键词

ST_COVEREDBY,ST,COVEREDBY
