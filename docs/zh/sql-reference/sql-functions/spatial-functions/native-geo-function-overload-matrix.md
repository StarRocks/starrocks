---
displayed_sidebar: docs
description: "列出原生 GEOGRAPHY 和 GEOMETRY 重载、函数 ID、支持的输入以及运行时行为。"
---

# 原生 GEO 函数重载矩阵

本文定义初始 GEO 函数组中原生 `GEOGRAPHY` 和 `GEOMETRY` 函数的重载契约。函数 ID 用于 FE 到 BE 的分发。系统按 SQL 逻辑类型解析重载，并在运行时单独校验值描述符；不会根据 WKB 字节推断球面或平面语义。

## 通用行为

- 原生构造和序列化函数支持全部七种二维 OGC 类型：`POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON` 和 `GEOMETRYCOLLECTION`，也支持带类型的 `EMPTY` 和空子对象。
- 参数为 `NULL` 时返回 `NULL`。对于格式错误的 WKT/WKB 或不支持的构造参数，构造函数也返回 `NULL`。
- 原生计算函数要求 XY 值和有效描述符。不支持的类型、维度或描述符会返回受控错误，除非下表明确规定该行的 `EMPTY` 返回 `NULL`。
- `GEOGRAPHY` 与 `GEOMETRY` 是不同的逻辑类型。混合类型调用没有对应重载，因此会被拒绝。
- `GEOGRAPHY` 使用 `OGC:CRS84`、球面边、经度/纬度坐标顺序和 SRID 4326。`GEOMETRY` 使用平面语义和描述符中显式指定的 CRS。本矩阵中的函数都不执行坐标重投影。

## 构造函数

| 签名 | 函数 ID | 返回类型 | 支持的类型和维度 | CRS 和描述符行为 |
| --- | ---: | --- | --- | --- |
| `ST_GeogFromText(VARCHAR wkt)` | 120020 | `GEOGRAPHY` | 全部七种二维类型和 `EMPTY` | 使用原生 `OGC:CRS84` 球面描述符和 SRID 4326。 |
| `ST_GeogFromText(VARCHAR wkt, INT srid)` | 120021 | `GEOGRAPHY` | 全部七种二维类型和 `EMPTY` | `srid` 必须为 4326；结果使用原生 `OGC:CRS84` 球面描述符。 |
| `ST_GeomFromText(VARCHAR wkt, VARCHAR crs)` | 120022 | `GEOMETRY` | 全部七种二维类型和 `EMPTY` | `crs` 必须是非空字符串字面量，并写入平面结果描述符。没有默认 CRS。 |
| `ST_GeogFromWKB(VARBINARY wkb)` | 120030 | `GEOGRAPHY` | 全部七种二维类型和 `EMPTY`；支持两种 WKB 字节序 | 使用原生 `OGC:CRS84` 球面描述符和 SRID 4326。不支持 EWKB。 |
| `ST_GeogFromWKB(VARBINARY wkb, INT srid)` | 120031 | `GEOGRAPHY` | 全部七种二维类型和 `EMPTY`；支持两种 WKB 字节序 | `srid` 必须为 4326；结果使用原生 `OGC:CRS84` 球面描述符。不支持 EWKB。 |
| `ST_GeomFromWKB(VARBINARY wkb, VARCHAR crs)` | 120032 | `GEOMETRY` | 全部七种二维类型和 `EMPTY`；支持两种 WKB 字节序 | `crs` 必须是非空字符串字面量，并写入平面结果描述符。不支持 EWKB 和 Z/M 坐标。 |

Geography 坐标必须位于支持的经纬度范围内。Geometry 构造函数不会重新解释坐标，也不会把坐标转换到其他 CRS。参见 [ST_GeogFromText](st_geogfromtext.md)、[ST_GeogFromWKB](st_geogfromwkb.md)、[ST_GeomFromText](st_geometryfromtext.md) 和 [ST_GeomFromWKB](st_geomfromwkb.md)。

## 序列化函数

| 签名 | 函数 ID | 返回类型 | 行为 |
| --- | ---: | --- | --- |
| `ST_AsText(GEOGRAPHY value)` | 120040 | `VARCHAR` | 输出 WKT，不修改坐标或输入描述符。 |
| `ST_AsText(GEOMETRY value)` | 120041 | `VARCHAR` | 输出 WKT，不修改坐标或输入描述符。 |
| `ST_AsWKT(GEOGRAPHY value)` | 120050 | `VARCHAR` | 原生 `ST_AsText(GEOGRAPHY)` 行为的别名。 |
| `ST_AsWKT(GEOMETRY value)` | 120051 | `VARCHAR` | 原生 `ST_AsText(GEOMETRY)` 行为的别名。 |
| `ST_AsBinary(GEOGRAPHY value)` | 120060 | `VARBINARY` | 输出 WKB。逻辑类型、CRS 和边语义仍是类型元数据，不编码为 EWKB。 |
| `ST_AsBinary(GEOMETRY value)` | 120061 | `VARBINARY` | 输出 WKB。逻辑类型和 CRS 仍是类型元数据，不编码为 EWKB。 |
| `ST_AsWKB(GEOGRAPHY value)` | 120070 | `VARBINARY` | 原生 `ST_AsBinary(GEOGRAPHY)` 行为的别名。 |
| `ST_AsWKB(GEOMETRY value)` | 120071 | `VARBINARY` | 原生 `ST_AsBinary(GEOMETRY)` 行为的别名。 |

参见 [ST_AsText 和 ST_AsWKT](st_astext.md) 以及 [ST_AsBinary 和 ST_AsWKB](st_asbinary.md)。

## 初始计算函数

| 签名 | 函数 ID | 返回值和单位 | 支持的值 | `NULL`、`EMPTY` 和描述符行为 |
| --- | ---: | --- | --- | --- |
| `ST_X(GEOGRAPHY point)` | 120080 | `DOUBLE`，经度（度） | 非空 XY `POINT` | `NULL` 传递。`EMPTY`、非 `POINT` 或不支持的描述符报错。 |
| `ST_X(GEOMETRY point)` | 120081 | `DOUBLE`，输入 CRS 单位 | 非空 XY `POINT` | `NULL` 传递。`EMPTY`、非 `POINT` 或不支持的描述符报错。 |
| `ST_Y(GEOGRAPHY point)` | 120090 | `DOUBLE`，纬度（度） | 非空 XY `POINT` | `NULL` 传递。`EMPTY`、非 `POINT` 或不支持的描述符报错。 |
| `ST_Y(GEOMETRY point)` | 120091 | `DOUBLE`，输入 CRS 单位 | 非空 XY `POINT` | `NULL` 传递。`EMPTY`、非 `POINT` 或不支持的描述符报错。 |
| `ST_GeometryType(GEOGRAPHY value)` | 120170 | `VARCHAR` 类型名称 | 所有受支持的 XY 类型 | `NULL` 传递。带类型的 `EMPTY` 保留其类型名称。不支持的维度或描述符报错。 |
| `ST_GeometryType(GEOMETRY value)` | 120171 | `VARCHAR` 类型名称 | 所有受支持的 XY 类型 | `NULL` 传递。带类型的 `EMPTY` 保留其类型名称。不支持的维度或描述符报错。 |
| `ST_Distance(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120180 | `DOUBLE`，米 | 球面 CRS84 契约下的 XY `POINT`/`POINT` | `NULL` 或 `EMPTY` 返回 `NULL`。不支持的类型、维度或描述符报错。 |
| `ST_Distance(GEOMETRY lhs, GEOMETRY rhs)` | 120181 | `DOUBLE`，输入 CRS 单位 | 描述符匹配的 XY `POINT`/`POINT` | `NULL` 或 `EMPTY` 返回 `NULL`。不支持的类型、维度或不兼容描述符报错。 |

参见 [ST_X](st_x.md)、[ST_Y](st_y.md)、[ST_GeometryType](st_geometrytype.md) 和 [ST_Distance](st_distance.md)。

## 旧版兼容性

原生重载不会重新编号或替换现有的 `VARCHAR` 函数：

| 旧版签名 | 函数 ID |
| --- | ---: |
| `ST_X(VARCHAR)` | 120001 |
| `ST_Y(VARCHAR)` | 120002 |
| `ST_AsText(VARCHAR)` | 120004 |
| `ST_AsWKT(VARCHAR)` | 120005 |
| `ST_GeometryFromText(VARCHAR)` | 120006 |
| `ST_GeomFromText(VARCHAR)` | 120007 |

## 升级与参考测试契约

滚动升级时，应先升级 BE，再升级 FE。这些函数使用普通的稳定函数 ID 分发，不引入单独的 GEO 版本门控。不支持新 FE 与旧 BE 的升级顺序。

该契约由聚焦的 FE analyzer 测试验证重载解析、返回类型、旧版兼容性和函数 ID，并由 BE registry 测试验证每个原生 ID。现有 BE 函数测试覆盖常量、Nullable、变化输入、`EMPTY`、格式错误输入、类型、维度、CRS 和描述符行为。
