---
displayed_sidebar: docs
description: "使用显式 CRS 从 WKB 构造原生 GEOMETRY 值。"
---

# ST_GeomFromWKB

从二维 OGC Well-Known Binary（WKB）构造原生平面 `GEOMETRY` 值。

## 语法

```Haskell
GEOMETRY ST_GeomFromWKB(VARBINARY wkb, VARCHAR crs)
```

`crs` 必须是非空字符串字面量。没有默认 CRS，也不会执行隐式坐标转换或重投影。支持两种 WKB 字节序、全部七种二维 OGC 几何类型、`EMPTY` 和空子元素。不支持 EWKB 和 Z/M 坐标。无效 WKB 返回 `NULL`；WKB 为 `NULL` 时返回 `NULL`。

## 示例

```SQL
SELECT ST_AsText(ST_GeomFromWKB(
    ST_AsBinary(ST_GeomFromText('POINT (1 2)', 'EPSG:3857')),
    'EPSG:3857'));
```

```text
POINT (1 2)
```

## 关键字

ST_GEOMFROMWKB, GEOMETRY, WKB, CRS
