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
| `ST_Distance(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120180 | `DOUBLE`，米 | 球面 CRS84 契约下的 XY `POINT`/`POINT`，或 `POINT` 与 `LINESTRING`/`MULTILINESTRING` | `NULL` 或 `EMPTY` 返回 `NULL`。不支持的类型、维度、描述符或不明确的对跖线段报错。 |
| `ST_Distance(GEOMETRY lhs, GEOMETRY rhs)` | 120181 | `DOUBLE`，输入 CRS 单位 | 描述符匹配的 XY `POINT`/`POINT`，或 `POINT` 与 `LINESTRING`/`MULTILINESTRING` | `NULL` 或 `EMPTY` 返回 `NULL`。不支持的类型、维度或不兼容描述符报错。 |
| `ST_DWithin(GEOGRAPHY lhs, GEOGRAPHY rhs, DOUBLE distance)` | 120230 | `BOOLEAN`；`distance` 单位为米 | XY `POINT` 与 `LINESTRING`/`MULTILINESTRING`，参数顺序不限 | 阈值包含相等边界。`NULL` 传递；`EMPTY` 返回 `false`。阈值必须是有限的非负数。 |
| `ST_DWithin(GEOMETRY lhs, GEOMETRY rhs, DOUBLE distance)` | 120231 | `BOOLEAN`；`distance` 使用输入 CRS 单位 | 描述符匹配的 XY `POINT` 与 `LINESTRING`/`MULTILINESTRING`，参数顺序不限 | 阈值包含相等边界。`NULL` 传递；`EMPTY` 返回 `false`。阈值必须是有限的非负数。 |

参见 [ST_X](st_x.md)、[ST_Y](st_y.md)、[ST_GeometryType](st_geometrytype.md)、[ST_Distance](st_distance.md) 和 [ST_DWithin](st_dwithin.md)。

## 包含关系谓词

| 签名 | 函数 ID | 边界行为 |
| --- | ---: | --- |
| `ST_Contains(GEOGRAPHY polygon, GEOGRAPHY point)` | 120190 | 仅当点位于多边形内部时返回 `true`。 |
| `ST_Contains(GEOMETRY polygon, GEOMETRY point)` | 120191 | 仅当点位于多边形内部时返回 `true`。 |
| `ST_Within(GEOGRAPHY point, GEOGRAPHY polygon)` | 120200 | `ST_Contains` 的反向关系，不包含边界。 |
| `ST_Within(GEOMETRY point, GEOMETRY polygon)` | 120201 | `ST_Contains` 的反向关系，不包含边界。 |
| `ST_Covers(GEOGRAPHY polygon, GEOGRAPHY point)` | 120210 | 包含外环和内环边界。 |
| `ST_Covers(GEOMETRY polygon, GEOMETRY point)` | 120211 | 包含外环和内环边界。 |
| `ST_CoveredBy(GEOGRAPHY point, GEOGRAPHY polygon)` | 120220 | `ST_Covers` 的反向关系，包含边界。 |
| `ST_CoveredBy(GEOMETRY point, GEOMETRY polygon)` | 120221 | `ST_Covers` 的反向关系，包含边界。 |

这些重载支持 XY `POINT` 与 `POLYGON` 或 `MULTIPOLYGON`。`GEOGRAPHY` 按球面 CRS84 边计算；`GEOMETRY` 按平面边计算并要求描述符匹配。`NULL` 传递，`EMPTY` 输入返回 `false`。不支持的类型、维度、描述符以及混合 `GEOGRAPHY`/`GEOMETRY` 调用会被拒绝。参见 [ST_Contains](st_contains.md)、[ST_Within](st_within.md)、[ST_Covers](st_covers.md) 和 [ST_CoveredBy](st_coveredby.md)。

## 相交谓词

| 签名 | 函数 ID | 边界行为 |
| --- | ---: | --- |
| `ST_Intersects(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120240 | 球面重叠、包含和边界接触均返回 `true`。 |
| `ST_Intersects(GEOMETRY lhs, GEOMETRY rhs)` | 120241 | 平面重叠、包含和边界接触均返回 `true`。 |

这些重载支持 XY `POLYGON` 和 `MULTIPOLYGON`。`GEOMETRY` 输入必须具有匹配的描述符。`NULL` 传递，`EMPTY` 输入返回 `false`。不支持的类型、集合以及混合的原生逻辑类型会被拒绝。参见 [ST_Intersects](st_intersects.md)。

## 度量和属性

| 签名 | 函数 ID | 结果和单位 | Family 行为 |
| --- | ---: | --- | --- |
| `ST_Area(GEOGRAPHY value)` | 120250 | `DOUBLE`，平方米 | 递归合计面面积，扣除洞，低维分量贡献 0。 |
| `ST_Area(GEOMETRY value)` | 120251 | `DOUBLE`，输入 CRS 单位的平方 | 递归合计面面积，扣除洞，低维分量贡献 0。 |
| `ST_Length(GEOGRAPHY value)` | 120260 | `DOUBLE`，米 | 递归合计线长度，点和面贡献 0。 |
| `ST_Length(GEOMETRY value)` | 120261 | `DOUBLE`，输入 CRS 单位 | 递归合计线长度，点和面贡献 0。 |
| `ST_Perimeter(GEOGRAPHY value)` | 120270 | `DOUBLE`，米 | 递归合计面的外环和内环长度。 |
| `ST_Perimeter(GEOMETRY value)` | 120271 | `DOUBLE`，输入 CRS 单位 | 递归合计面的外环和内环长度。 |
| `ST_Centroid(GEOGRAPHY value)` | 120280 | `GEOGRAPHY POINT`，descriptor 不变 | 使用最高非空维度和球面尺寸权重。 |
| `ST_Centroid(GEOMETRY value)` | 120281 | `GEOMETRY POINT`，descriptor 不变 | 使用最高非空维度和平面尺寸权重。 |
| `ST_IsValid(GEOGRAPHY value)` | 120290 | `BOOLEAN` | 使用球面有效性，包括 MultiPolygon 内部重叠检查。 |
| `ST_IsValid(GEOMETRY value)` | 120291 | `BOOLEAN` | 使用稳健的平面拓扑有效性。 |

所有 overload 均接受七种 XY OGC family，并在适用时递归处理 `MULTI*` 和 `GEOMETRYCOLLECTION`。`NULL` 传播；度量对 `EMPTY` 返回 0，`ST_Centroid` 返回 `POINT EMPTY`，`ST_IsValid` 返回 `true`。可读取但拓扑无效的值仅由 `ST_IsValid` 返回 `false`，度量和质心会拒绝该输入。WKB 格式错误、不支持的维度或 descriptor 会返回错误。

球面面环与方向无关，并归一化为较小区域；单个环不表示超过半球的区域。参见 [ST_Area](st_area.md)、[ST_Length](st_length.md)、[ST_Perimeter](st_perimeter.md)、[ST_Centroid](st_centroid.md) 和 [ST_IsValid](st_isvalid.md)。
## CRS 元数据与坐标转换

| 签名 | 函数 ID | 结果 | 行为 |
| --- | ---: | --- | --- |
| `ST_SRID(GEOMETRY value)` | 120300 | `INT` 或 `NULL` | 从描述符读取数字 EPSG 映射，不转换坐标。 |
| `ST_SetSRID(GEOMETRY value, INT constant)` | 120305 | `GEOMETRY`，目标 EPSG 描述符 | 只修改元数据；WKB 和坐标保持不变。 |
| `ST_Transform(GEOMETRY value, INT constant)` | 120310 | `GEOMETRY`，目标 EPSG 描述符 | 使用 Web Mercator 公式在 EPSG:4326 和 EPSG:3857 之间重投影 XY 坐标。 |

两个目标 SRID 都必须是可由 FE 折叠为常量的 4326 或 3857。源 CRS 必须是 EPSG:4326、EPSG:3857 或 OGC:CRS84。`ST_Transform` 处理七种 XY OGC 几何类型及集合；`NULL` 传播，`EMPTY` 保持为空。参见 [ST_SRID](st_srid.md)、[ST_SetSRID](st_setsrid.md) 和 [ST_Transform](st_transform.md)。

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
| `ST_Contains(VARCHAR, VARCHAR)` | 120014 |

## 升级与参考测试契约

滚动升级时，应先升级 BE，再升级 FE。这些函数使用普通的稳定函数 ID 分发，不引入单独的 GEO 版本门控。不支持新 FE 与旧 BE 的升级顺序。

该契约由聚焦的 FE analyzer 测试验证重载解析、返回类型、旧版兼容性和函数 ID，并由 BE registry 测试验证每个原生 ID。现有 BE 函数测试覆盖常量、Nullable、变化输入、`EMPTY`、格式错误输入、类型、维度、CRS 和描述符行为。

## H3 网格函数

几何参数需要原生 XY CRS84 GEOGRAPHY；网格索引是有符号 BIGINT。有效性、NULL/EMPTY、填充近似和限制见 [H3 函数](h3-functions.md)。

| Signature | Function ID | Result |
| --- | ---: | --- |
| `H3_FromGeo(GEOGRAPHY, INT)` | 120320 | `BIGINT` |
| `H3_GridDisk(BIGINT, INT)` | 120321 | `ARRAY<BIGINT>` |
| `H3_ToParent(BIGINT, INT)` | 120322 | `BIGINT` |
| `H3_ToChildren(BIGINT, INT)` | 120323 | `ARRAY<BIGINT>` |
| `H3_Resolution(BIGINT)` | 120324 | `INT` |
| `H3_ToBoundary(BIGINT)` | 120325 | `GEOGRAPHY` |
| `H3_PolygonToCells(GEOGRAPHY, INT)` | 120326 | `ARRAY<BIGINT>` |
