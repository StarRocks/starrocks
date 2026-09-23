---
displayed_sidebar: docs
description: "返回点的 Y 坐标。对于 GEOGRAPHY，Y 表示以度为单位的纬度。"
---

# ST_Y

返回点的 Y 坐标。

对于 `GEOGRAPHY` 值，在 OGC:CRS84 球面语义下，Y 表示以度为单位的纬度。输入值必须是非空的 `POINT`。

## 语法

```SQL
DOUBLE ST_Y(VARCHAR point)
DOUBLE ST_Y(GEOGRAPHY point)
```

## 返回值说明

输入为 `NULL` 时返回 `NULL`。对于 `GEOGRAPHY`，如果输入为空值或不是 `POINT` 类型，则返回错误。不支持的维度或描述符也会返回错误。

## 示例

```SQL
SELECT ST_Y(ST_Point(24.7, 56.7));
-- 56.7

SELECT ST_Y(ST_GeogFromText('POINT (24.7 56.7)'));
-- 56.7
```
