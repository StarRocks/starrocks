---
displayed_sidebar: docs
description: "以原生 WKB 形式存储二维 OGC 几何值。"
---

# GEOMETRY

`GEOMETRY` 以规范的小端 OGC Well-Known Binary (WKB) 序列化形式存储二维几何值。

支持 `POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON`、`GEOMETRYCOLLECTION` 以及各类型的 `EMPTY` 值。使用 [ST_GeomFromText](../../sql-functions/spatial-functions/st_geometryfromtext.md) 或 [ST_GeomFromWKB](../../sql-functions/spatial-functions/st_geometryfromwkb.md) 构造值，使用 [ST_AsText](../../sql-functions/spatial-functions/st_astext.md) 或 [ST_AsBinary](../../sql-functions/spatial-functions/st_asbinary.md) 序列化值。

## 限制

- 仅支持二维 OGC WKT 和 WKB。不支持 EWKT/EWKB、SRID 和 Z/M 坐标。
- 规范化仅适用于二进制序列化，不会规范化坐标、环或集合的顺序。
- 输入验证仅检查结构，不检查拓扑。例如，不会检测自相交等拓扑错误。
- `GEOMETRY` 列不能用作键、排序键、分桶键或分区键。
- 不支持对 `GEOMETRY` 值进行比较、分组、排序或算术运算。
- 列默认值仅支持 `NULL`。
