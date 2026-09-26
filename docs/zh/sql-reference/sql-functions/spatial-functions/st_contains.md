---
displayed_sidebar: docs
description: "判断多边形是否严格包含一个点。"
---

# ST_Contains

判断点是否位于多边形内部。位于外环边界或孔洞边界上的点不视为被包含。

## 语法

```SQL
ST_Contains(GEOGRAPHY polygon, GEOGRAPHY point)
ST_Contains(GEOMETRY polygon, GEOMETRY point)
ST_Contains(VARCHAR shape1, VARCHAR shape2)
```

原生重载要求第二个参数为 XY `POINT`，第一个参数为 XY `POLYGON` 或 `MULTIPOLYGON`。两个 `GEOMETRY` 参数的描述符必须匹配。混合原生 `GEOGRAPHY`/`GEOMETRY` 调用会被拒绝。`NULL` 传递，`EMPTY` 输入返回 `false`。

`VARCHAR` 签名是现有旧版重载，其行为保持不变。

## 示例

```SQL
SELECT ST_Contains(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POINT (5 5)', 'EPSG:3857'));
-- true

SELECT ST_Contains(
    ST_GeogFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_GeogFromText('POINT (0 5)'));
-- false：点位于边界上
```

## 关键词

ST_CONTAINS,ST,CONTAINS
