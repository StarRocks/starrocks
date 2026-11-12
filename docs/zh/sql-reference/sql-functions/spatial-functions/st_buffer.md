---
displayed_sidebar: docs
description: "为原生 XY 几何对象生成圆角笛卡尔缓冲区。"
---

# ST_Buffer

按有符号距离为原生 `GEOMETRY` 生成平面圆角缓冲区。

## 语法

```SQL
ST_Buffer(GEOMETRY geometry, DOUBLE distance)
```

距离可以是常量，也可以逐行变化。不支持选项或分段数参数。

## 输入和结果

| XY 输入类型 | 正距离 | 零或负距离 |
| --- | --- | --- |
| POINT、MULTIPOINT | 圆形缓冲区 | POLYGON EMPTY |
| LINESTRING、MULTILINESTRING | 圆角连接和圆形端帽 | POLYGON EMPTY |
| POLYGON、MULTIPOLYGON | 扩张 | 零保留有效区域；负值向内收缩 |

结果是经过验证的原生 XY `GEOMETRY`，保留输入 CRS 和平面边语义。一个区域分量返回 `POLYGON`，多个返回 `MULTIPOLYGON`，空结果返回 `POLYGON EMPTY`。缓冲可以合并分量、填满孔洞或使收缩后的多边形分裂。只有一个分量的 MULTIPOLYGON 结果规范化为 POLYGON。环方向、起始顶点和分量顺序可能改变。

任一参数为 `NULL` 时返回 `NULL`。带类型的 EMPTY（含空子元素）在有限距离下返回 POLYGON EMPTY。即使距离为零或负值，非 NULL 输入行也必须结构和拓扑有效。零距离不修复无效多边形。非有限距离、无效拓扑、损坏的 WKB 或超出资源限制均返回错误，不修复或截断几何。

不支持 `GEOGRAPHY`、GEOMETRYCOLLECTION（包括 EMPTY）、Z/M，以及未知或混合维度。此函数是标量函数，不是聚合、窗口或数组函数。

## 单位和近似

坐标按输入 CRS 的笛卡尔 X/Y 解释。EPSG:4326 的单位是度：距离 1000 表示 **1000 度**，结果可以超出经纬度范围。不自动变换坐标、截断或环绕经纬度，也不切换到球面策略。EPSG:3857 使用投影米，不保证相同数值的地面测地半径。

圆形策略每整圆使用 32 个点，边为直线，距离是对称有符号距离。圆弧弦偏差为 `abs(distance) * (1 - cos(pi / 32))`，约为半径的 0.004815 倍。Boost.Geometry 1.80 还以 `abs(distance) / 1000` 为阈值对非零距离输入预简化。圆角可能额外细分；顶点数和 WKB 顺序不是兼容性保证。这些参数不构成任意复杂输入的通用拓扑或 Hausdorff 误差保证。计算使用浮点数。局部坐标平移损失输入精度，或舍入为 WKB 双精度后失去结果拓扑时，返回错误。多边形零距离绕过缓冲与预简化。

## 资源限制

输入最多为 256 KiB WKB 和 5,000 个坐标位置（含闭环位置）；输出最多 10,000 个位置。查询内存限制覆盖预处理模型与临时分配。在行间及库调用前后检查取消和内存错误；单个 Boost 调用内部不能中断。这些限制不保证固定峰值内存或执行时间。

## 示例

```SQL
SELECT ROUND(ST_Area(ST_Buffer(
    ST_GeomFromText('POINT (0 0)', 'EPSG:3857'), 1.0)), 6);
-- 3.121445

SELECT ST_Area(ST_Buffer(
    ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))', 'EPSG:3857'), -1.0));
-- 64

SELECT ST_AsText(ST_Buffer(
    ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))', 'EPSG:3857'), -5.0));
-- POLYGON EMPTY

SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT (0 0)', 'EPSG:4326'), 0));
-- POLYGON EMPTY

SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT (0 0)', 'EPSG:3857'), NULL));
-- NULL
```
