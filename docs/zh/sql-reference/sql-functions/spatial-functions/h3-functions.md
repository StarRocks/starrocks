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
