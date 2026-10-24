---
displayed_sidebar: docs
description: "在保留拓扑和 CRS 的同时简化原生 XY 几何。"
---

# ST_SimplifyPreserveTopology

减少原生笛卡尔 XY `GEOMETRY` 的顶点，同时保留有效性、维度、组成部分、环、孔洞归属、已有接触和分离关系。如果删除顶点不安全，则保留原有部分。这是针对单个几何的标量运算，不会在多行之间执行覆盖简化。

## 语法

```SQL
ST_SimplifyPreserveTopology(GEOMETRY geometry, DOUBLE tolerance)
```

容差可以是常量，也可以随行变化。必须为有限非负数，单位为输入坐标单位。不支持第三个参数。

## 输入和结果

支持 POINT、LINESTRING、POLYGON、对应的 MULTI 类型以及嵌套 GEOMETRYCOLLECTION。结果保留输入类型、组成部分顺序、集合树、带类型的 EMPTY 子元素、CRS 和平面 XY 描述符。只有一个成员的 MULTIPOLYGON 仍为 MULTIPOLYGON。点和重复点成员保持不变。线保留端点，环保留闭合和方向。结果仅使用原始顶点，并保留各路径中的原始顺序。

任一参数为 `NULL` 时返回 `NULL`。容差为零时，在完成结构、维度和拓扑校验后返回原始 WKB。EMPTY 保留原有类型；非 NULL 的 EMPTY 也要求容差有效。负数、NaN 或无穷容差、格式错误的 WKB、无效多边形拓扑、退化线或环以及超出限制均报错。不会修复无效输入，也不会通过丢弃组成部分来生成有效结果。

不支持 GEOGRAPHY、Z/M、未知或混合维度。此函数支持集合不表示其他 GEO 函数也支持集合。不提供聚合、窗口或数组形式。

## 单位和误差界限

EPSG:4326 使用笛卡尔角度，容差 1000 表示 **1000 度**。EPSG:3857 使用投影米，而非测地地面距离。不执行坐标转换、环绕、截断或球面计算。

对线的路径和多边形边界，输入与结果的双向 Hausdorff 距离不超过容差。每个替换线段都检查完整的原始子路径，包括此前删除的顶点，因此误差不会累积。接触和距离判断对存储的有限 double 坐标执行精确的二进制有理数比较，不会通过隐含 epsilon、吸附或精度网格更改输入。不保证面积不变、填充多边形内部的距离界限、最大简化程度或与 PostGIS/JTS 顶点一致。不同输入行之间的共享边界不受约束。

## 资源限制

每个输入限制为 256 KiB WKB、5,000 个坐标位置（包括闭合点）、1,024 个几何节点与环，以及 32 层嵌套。准备和每次计算受 2,000 万个计费工作单位的限制。精确整数大小、活动边、索引和查询结果有界；输入大小未超限时也可能触发工作量限制。准备模型和临时分配都受查询内存限制约束。取消和内存检查在行之间、有界分配组之前以及工作循环内执行。不保证固定的峰值内存或执行时间。

## 示例

```SQL
SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('LINESTRING (0 0,1 0,2 0)', 'EPSG:3857'), 0.1));
-- LINESTRING (0 0, 2 0)

SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('MULTIPOLYGON EMPTY', 'EPSG:4326'), 1));
-- MULTIPOLYGON EMPTY

SELECT ST_SRID(ST_SimplifyPreserveTopology(
    ST_GeomFromText('POINT (1 2)', 'EPSG:4326'), 0));
-- 4326

SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('POINT (1 2)', 'EPSG:3857'), NULL));
-- NULL
```
