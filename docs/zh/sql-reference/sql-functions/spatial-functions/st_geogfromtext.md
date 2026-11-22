---
displayed_sidebar: docs
description: "从 WKT 构造原生 GEOGRAPHY 值。"
products: [oss, enterprise, cloud, anywhere]
---

# ST_GeogFromText

从 Well-Known Text（WKT）构造原生 `GEOGRAPHY` 值。

## 语法

```Haskell
GEOGRAPHY ST_GeogFromText(VARCHAR wkt)
GEOGRAPHY ST_GeogFromText(VARCHAR wkt, INT srid)
```

## 参数说明

- `wkt`：二维 OGC WKT 值。支持全部七种 OGC 几何类型和 `EMPTY` 成员。
- `srid`：可选。空间参考标识符。当前仅支持 `4326`；省略时默认为 `4326`。

坐标采用球面 `OGC:CRS84` 语义：经度范围为 `[-180, 180]`，纬度范围为 `[-90, 90]`。

## 返回值说明

返回 `GEOGRAPHY` 值。如果任一参数为 `NULL`、WKT 无效、坐标超出支持范围或 `srid` 不是 `4326`，则返回 `NULL`。

在查询中，兼容的原生地理空间值可以用作 `ARRAY` 元素、`MAP` 值或 `STRUCT` 字段。您可以构造这些容器、读取元素和字段、使用 `cardinality` 或 `array_length`、用 `UNNEST` 展开数组，也可以使用 `array_map`、`array_filter`，以及不带排序和 `DISTINCT` 的 `array_agg` 和 `map_agg`。

`MAP` 的键不能包含地理空间值。需要对地理空间值进行相等性判断、排序或哈希计算的操作会被拒绝，例如比较、排序和去重。视图、物化视图、SQL UDF、生成列、SQL 执行计划基线和 Pipe 等持久化 SQL 定义不能使用嵌套地理空间表达式。

向客户端返回容器前，请先用 `ST_AsText` 或 `ST_AsBinary` 转换其中的地理空间元素或字段。标量 `GEOGRAPHY` 可以直接返回。标量 `GEOMETRY` 也需要显式转换。

```sql
SELECT array_map(x -> ST_AsText(x), [ST_GeogFromText('POINT (1 2)')]);
```

## 示例

```SQL
SELECT ST_AsText(ST_GeogFromText('LINESTRING (1 2, 3 4)', 4326));
```

```text
LINESTRING (1 2, 3 4)
```

```SQL
SELECT ST_AsText(ST_GeogFromText('POINT EMPTY'));
```

```text
POINT EMPTY
```

## 关键字

ST_GEOGFROMTEXT, GEOGRAPHY, WKT
