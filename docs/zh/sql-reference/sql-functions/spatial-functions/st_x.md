---
displayed_sidebar: docs
description: "返回点的 X 坐标。对于 GEOGRAPHY，X 表示以度为单位的经度。"
---

# ST_X

返回点的 X 坐标。

对于 `GEOGRAPHY` 值，在 OGC:CRS84 球面语义下，X 表示以度为单位的经度。输入值必须是非空的 `POINT`。

## 语法

```SQL
DOUBLE ST_X(VARCHAR point)
DOUBLE ST_X(GEOGRAPHY point)
```

## 返回值说明

输入为 `NULL` 时返回 `NULL`。对于 `GEOGRAPHY`，如果输入为空值或不是 `POINT` 类型，则返回错误。不支持的维度或描述符也会返回错误。

## 示例

```SQL
SELECT ST_X(ST_Point(24.7, 56.7));
-- 24.7

SELECT ST_X(ST_GeogFromText('POINT (24.7 56.7)'));
-- 24.7
```
