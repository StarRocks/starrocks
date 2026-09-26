---
displayed_sidebar: docs
description: "判断多边形是否覆盖一个点，包括边界。"
---

# ST_Covers

判断点是否位于多边形内部、外环边界或孔洞边界上。

## 语法

```SQL
ST_Covers(GEOGRAPHY polygon, GEOGRAPHY point)
ST_Covers(GEOMETRY polygon, GEOMETRY point)
```

`polygon` 必须是 XY `POLYGON` 或 `MULTIPOLYGON`，`point` 必须是 XY `POINT`。`GEOMETRY` 描述符必须匹配。`NULL` 传递，`EMPTY` 输入返回 `false`。

## 示例

```SQL
SELECT ST_Covers(
    ST_GeogFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_GeogFromText('POINT (0 5)'));
-- true
```

## 关键词

ST_COVERS,ST,COVERS
