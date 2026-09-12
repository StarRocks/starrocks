---
displayed_sidebar: docs
description: "将 OGC WKB 转换为原生 GEOMETRY 值。"
---

# ST_GeomFromWKB, ST_GeometryFromWKB

将二维 OGC Well-Known Binary (WKB) 值转换为原生 `GEOMETRY` 值。

## 语法

```Haskell
GEOMETRY ST_GeomFromWKB(VARBINARY wkb)
GEOMETRY ST_GeometryFromWKB(VARBINARY wkb)
```

支持小端和大端 OGC WKB 输入、全部七种 OGC 几何类型及其 `EMPTY` 形式。不支持 EWKB、SRID 和 Z/M 坐标。无效输入返回 `NULL`。

## 关键词

ST_GEOMFROMWKB,ST_GEOMETRYFROMWKB,ST,GEOMFROMWKB,GEOMETRYFROMWKB
