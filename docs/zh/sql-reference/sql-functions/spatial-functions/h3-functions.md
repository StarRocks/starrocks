---
displayed_sidebar: docs
description: "使用七个 H3 网格和多边形函数为原生 GEOGRAPHY 建立索引。"
---

# H3 函数

这些函数使用 H3 4.5.0 和原生 XY CRS84 `GEOGRAPHY`（经度、纬度，单位为度）。公开的网格索引类型为正数 `BIGINT`，其值可能超过 2^53。客户端必须使用无损 64 位整数，或以十进制字符串传输；JavaScript `Number` 不能精确表示所有索引。不支持 `GEOMETRY` 或隐式 CRS 转换。

| 函数 | 返回类型 | 规则 |
| --- | --- | --- |
| `H3_FromGeo(geography, resolution INT)` | `BIGINT` | 非空 `POINT` 所在网格；分辨率 0–15；`POINT EMPTY` 返回 `NULL`。 |
| `H3_GridDisk(cell BIGINT, k INT)` | `ARRAY<BIGINT>` | 距离不超过 `k` 的网格，包含原网格；`k >= 0`，`k = 0` 返回 `[cell]`。 |
| `H3_ToParent(cell BIGINT, resolution INT)` | `BIGINT` | 从 0 到当前网格分辨率的父网格。 |
| `H3_ToChildren(cell BIGINT, resolution INT)` | `ARRAY<BIGINT>` | 从当前分辨率到 15 的子网格。 |
| `H3_Resolution(cell BIGINT)` | `INT` | 有效网格的分辨率。 |
| `H3_ToBoundary(cell BIGINT)` | `GEOGRAPHY` | 使用 H3 返回的全部顶点构造闭合的球面 CRS84 多边形。 |
| `H3_PolygonToCells(geography, resolution INT)` | `ARRAY<BIGINT>` | 对有效 `POLYGON`/`MULTIPOLYGON`（包括孔洞）执行 H3 标准中心填充；分辨率 0–15；空多边形返回 `[]`。 |

任一参数为 `NULL` 时返回 `NULL`。数组中的网格索引有效且不重复，但顺序不保证；稳定显示可使用 `array_sort`。无效网格（包括 0、负数、边或顶点索引）、分辨率、拓扑、CRS、维度或几何类型都会报错。即使输入为 `EMPTY`，错误的数值参数仍会报错。

`H3_PolygonToCells` 使用 H3 标准中心填充（`flags = 0`），并不等价于精确球面 `ST_Contains`/`ST_Covers`。非空多边形也可能返回 `[]`，结果不是保守覆盖范围，不能代替精确空间谓词。H3 的经纬度填充模型在反经线、极区和大型多边形附近存在限制；中心位于边界时遵循 H3 4.5.0 的规则。需要精确判断时，请在近似索引后使用适当的精确谓词。

H3 4.5.0 不支持在单次 `polygonToCells` 调用内部取消。StarRocks 在调用前后及行间检查取消；以下准入限制约束工作量，但不保证固定取消延迟。

BE 构建选项 `WITH_H3` 默认开启。使用 `WITH_H3=OFF` 构建的 BE 对这些函数返回受控的不支持错误。

## 资源限制

这些可修改的 BE 配置在每行分配和填充之前检查。超过限制将报错，不截断结果；查询和列的内存限制仍然生效。

| BE 配置项 | 默认值 | 单位和含义 |
| --- | ---: | --- |
| `h3_max_cells_per_row` | 100000 | 输出网格数，以及去重前的展开槽位总数。 |
| `h3_max_grid_disk_k` | 128 | 最大网格距离。 |
| `h3_max_polygon_vertices` | 10000 | 所有环和组件的坐标位置，包括闭合位置。 |
| `h3_max_polygon_components` | 256 | 每行多边形组件数，包括空组件。 |
| `h3_max_working_bytes` | 67108864 | worker 准备及临时缓冲区的字节数（64 MiB）。 |
| `h3_max_estimated_work_per_row` | 10000000 | 各组件 `U × (V + 1)` 的总和，`U` 为 H3 估计槽位，`V` 为坐标位置数；这是准入启发式规则，不是 CPU 时间上限。 |

## 示例

```sql
SELECT H3_FromGeo(ST_GeogFromText('POINT (0 0)'), 3);
-- 592035265791393791
SELECT H3_Resolution(592035265791393791);
-- 3
SELECT H3_ToParent(592035265791393791, 2);
SELECT array_sort(H3_GridDisk(592035265791393791, 1));
SELECT array_sort(H3_ToChildren(592035265791393791, 4));
SELECT ST_AsText(H3_ToBoundary(592035265791393791));
SELECT array_sort(H3_PolygonToCells(
  ST_GeogFromText('POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 3));
```
