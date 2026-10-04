---
displayed_sidebar: docs
description: "联合简化窗口分区中的原生平面多边形，并保持覆盖拓扑。"
---

# ST_CoverageSimplify

对整个窗口分区中的边匹配原生笛卡尔 XY 多边形进行联合简化。相邻行的共享边共同简化，每个结果保持原始行的位置和 CRS。与逐个处理几何值的标量函数 [ST_SimplifyPreserveTopology](st_simplifypreservetopology.md) 不同。

## 语法

```SQL
ST_CoverageSimplify(GEOMETRY geom, DOUBLE tolerance)
    OVER (PARTITION BY coverage_id)
ST_CoverageSimplify(GEOMETRY geom, DOUBLE tolerance, BOOLEAN simplify_boundary)
    OVER (PARTITION BY coverage_id)
```

`OVER ()` 将所有行作为一个分区；整个分区可以跨多个 chunk。禁止窗口内 ORDER BY、显式窗口 frame、DISTINCT、IGNORE NULLS、RESPECT NULLS 和分析执行 hint。查询外层 ORDER BY 可用。不提供标量、数组或普通聚合重载。不支持 spill，必须设置 `SET enable_spill = false`。

## 参数和输入

两个参数必须是可折叠的计划常量。容差必须为有限非负值，其平方也必须能表示为有限 DOUBLE。双参数形式默认 `simplify_boundary = true`，显式 NULL 不使用默认值。

容差是基于面积的 Visvalingam–Whyatt 参数，与有效三角形面积的平方根相关，使用输入坐标单位：EPSG:4326 为笛卡尔度，EPSG:3857 为投影米，并非地表测地距离。不进行重投影、吸附或坐标裁剪；不保证 Hausdorff/最大距离、面积不变、最大顶点减少量或与 PostGIS 完全一致。

`true` 可简化共享边及外边界。`false` 仅简化共享内部边，保持外部边界、间隙及未填充孔洞的原始边。由另一个成员填充的孔洞属于共享内部边界。

仅接受具有统一平面 GEOMETRY CRS 描述符的 POLYGON/MULTIPOLYGON。非空内部不得重叠，共享边顶点必须匹配。允许间隙（`gapWidth = 0`），但不匹配的边细分、T 形连接、重复非空多边形、无效环或孔洞均报错。不自动修复、合并或补充节点。不支持 GEOGRAPHY、POINT、LINESTRING、GEOMETRYCOLLECTION 和 Z/M。UNKNOWN/MIXED 维度元数据只有在实际值通过 XY 验证后才可接受。

## 返回值和错误

返回相同 CRS 的原生 XY WKB GEOMETRY，每行一个结果。保持 POLYGON/MULTIPOLYGON 类型、成员顺序、孔洞、环方向、接触点、交汇点及共享边。单成员 MULTIPOLYGON 仍为 MULTIPOLYGON。按位置映射行，不按几何值去重。

- NULL 几何值不参与计算，并在原位置返回 NULL。
- POLYGON/MULTIPOLYGON EMPTY 保持类型和位置。
- 容差或显式边界参数为 NULL 时，所有结果为 NULL，不运行覆盖内核；类型和常量检查仍执行。
- 容差为零时先验证输入和覆盖，再返回原始 WKB，包括重复的闭合坐标。
- 空输入或全 NULL/EMPTY 仍检查参数。无效覆盖、损坏 WKB、不支持的描述符/维度和资源超限在发布该分区结果前报错。

## 资源限制

以下可动态修改的正数 BE 配置在每个分区开始时取快照：

| 配置 | 默认值 | 限制 |
| --- | ---: | --- |
| `geo_coverage_max_rows_per_partition` | 10000 | 所有行，包括 NULL/EMPTY。 |
| `geo_coverage_max_vertices_per_partition` | 1000000 | 原始 WKB 坐标数，包括闭合和重复位置。 |
| `geo_coverage_max_input_bytes_per_partition` | 67108864 | 输入 WKB 字节数，包括 EMPTY。 |
| `geo_coverage_max_working_bytes_per_partition` | 268435456 | 保留输入、模型、索引、临时数据、映射和输出容量的字节数。 |

在窗口累积前执行准入检查。共享输入/输出按实际保留分配大小计入，因此小切片也可能保留较大 chunk。多个分区还受查询内存限制。超限错误指出配置名，不截断分区或输出部分结果。复杂拓扑即使未达到大小限制，也可能触发一亿个计费工作单元限制。累积、内核循环和输出以及受限库调用前后检查取消；不保证固定取消延迟或运行时间。

## 示例

```SQL
SET enable_spill = false;
WITH districts AS (
    SELECT 1 AS district_id, 7 AS coverage_id,
           'POLYGON ((0 0,0 8,4 8,4.1 6,3.8 4,4.2 2,4 0,0 0))' AS wkt
    UNION ALL
    SELECT 2, 7, 'POLYGON ((4 0,4.2 2,3.8 4,4.1 6,4 8,8 8,8 0,4 0))'
)
SELECT district_id,
       ST_AsText(ST_CoverageSimplify(ST_GeomFromText(wkt, 'EPSG:3857'), 1, false)
                 OVER (PARTITION BY coverage_id)) AS simplified
FROM districts ORDER BY district_id;
-- 1  POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))
-- 2  POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))

SELECT ST_AsText(ST_CoverageSimplify(
    ST_GeomFromText('MULTIPOLYGON EMPTY', 'EPSG:4326'), 1) OVER ());
-- MULTIPOLYGON EMPTY

SELECT ST_AsText(ST_CoverageSimplify(
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857'), 0, NULL) OVER ());
-- NULL
```
