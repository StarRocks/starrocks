---
displayed_sidebar: docs
description: "判断点是否严格位于多边形内部。"
---

# ST_Within

判断点是否位于多边形内部。该函数是 `ST_Contains` 的反向关系，不包含外环和孔洞边界。

## 语法

```SQL
ST_Within(GEOGRAPHY point, GEOGRAPHY polygon)
ST_Within(GEOMETRY point, GEOMETRY polygon)
```

`polygon` 必须是 XY `POLYGON` 或 `MULTIPOLYGON`，`point` 必须是 XY `POINT`。`GEOMETRY` 描述符必须匹配。`NULL` 传递，`EMPTY` 输入返回 `false`。

## 示例

```SQL
SELECT ST_Within(
    ST_GeomFromText('POINT (5 5)', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'));
-- true
```

## 关键词

ST_WITHIN,ST,WITHIN
