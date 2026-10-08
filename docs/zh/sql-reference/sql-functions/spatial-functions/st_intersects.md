---
displayed_sidebar: docs
description: "判断两个原生多边形或多重多边形是否相交。"
---

# ST_Intersects

判断两个原生多边形或多重多边形是否共享任意点。内部重叠、包含关系，以及在外边界或孔洞边界上的接触均返回 `true`。如果一个多边形完全位于另一个多边形的孔洞内，则返回 `false`。

## 语法

```SQL
ST_Intersects(GEOGRAPHY lhs, GEOGRAPHY rhs)
ST_Intersects(GEOMETRY lhs, GEOMETRY rhs)
```

两个参数都必须是相同原生逻辑类型的 XY `POLYGON` 或 `MULTIPOLYGON`。`GEOGRAPHY` 使用球面 OGC:CRS84 边；`GEOMETRY` 使用平面边，并要求两个参数具有兼容的描述符。本函数不执行坐标转换。

`NULL` 会传递，任一输入为 `EMPTY` 时返回 `false`。不支持的几何类型、`GEOMETRYCOLLECTION`、混合使用 `GEOGRAPHY`/`GEOMETRY`，以及不兼容的 `GEOMETRY` 描述符都会产生错误。

## 示例

```SQL
SELECT ST_Intersects(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0))', 'EPSG:3857'));
-- true：两个多边形共享一条边

SELECT ST_Intersects(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))', 'EPSG:3857'));
-- false：第二个多边形位于孔洞内
```

## 关键字

ST_INTERSECTS,ST,INTERSECTS,GEOGRAPHY,GEOMETRY
