---
displayed_sidebar: docs
---

# VARIANT

自 4.0 版本起,StarRocks 支持 VARIANT 数据类型,用于查询 Parquet 格式的 Iceberg 表中的半结构化数据。本文介绍 VARIANT 的基本概念,以及 StarRocks 如何查询 VARIANT 类型数据并通过 variant 函数进行处理。

## 什么是 VARIANT

VARIANT 是一种半结构化数据类型,可以存储不同数据类型的值,包括基本类型(整数、浮点数、字符串、布尔值、日期、时间戳)和复杂类型(结构体、映射、数组)。VARIANT 数据以二进制格式编码,以实现高效的存储和查询。

VARIANT 类型在处理来自 Apache Iceberg 表的数据时特别有用,这些表使用带有 variant 编码的 Parquet 格式,允许灵活的模式演化和异构数据的高效存储。

有关 Parquet variant 编码格式的更多信息,请参阅 [Parquet Variant 编码规范](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)。

## 使用 VARIANT 数据

### 从 Iceberg 表查询 VARIANT 类型数据

StarRocks 支持从以 Parquet 格式存储的 Iceberg 表中查询 VARIANT 类型数据。查询使用 Parquet variant 编码的 Iceberg 表时,会自动识别 VARIANT 列。

查询包含 VARIANT 列的 Iceberg 表示例:

```SQL

-- 查询包含 VARIANT 列的表
SELECT
    id,
    variant_col
FROM iceberg_catalog.db.table_with_variants;
```

### 从 VARIANT 数据提取值

StarRocks 提供了多个函数来从 VARIANT 数据中提取类型化的值:

示例 1:使用类型化的 getter 函数提取基本值。

```SQL
SELECT
    get_variant_int(variant_col, '$') AS int_value,
    get_variant_string(variant_col, '$') AS string_value,
    get_variant_bool(variant_col, '$') AS bool_value,
    get_variant_double(variant_col, '$') AS double_value
FROM iceberg_catalog.db.table_with_variants;
```

示例 2:使用 JSON 路径表达式导航嵌套结构。

```SQL
SELECT
    get_variant_string(variant_col, '$.user.name') AS user_name,
    get_variant_int(variant_col, '$.user.age') AS user_age,
    get_variant_string(variant_col, '$.address.city') AS city
FROM iceberg_catalog.db.table_with_variants;
```

示例 3:访问数组元素。

```SQL
SELECT
    get_variant_int(variant_col, '$.scores[0]') AS first_score,
    get_variant_int(variant_col, '$.scores[1]') AS second_score
FROM iceberg_catalog.db.table_with_variants;
```

示例 4:查询嵌套的 variant 数据并返回 VARIANT 类型。

```SQL
SELECT
    variant_query(variant_col, '$.metadata') AS metadata,
    variant_query(variant_col, '$.items[0]') AS first_item
FROM iceberg_catalog.db.table_with_variants;
```

示例 5:检查 VARIANT 值的类型。

```SQL
SELECT
    variant_typeof(variant_col) AS root_type,
    variant_typeof(variant_query(variant_col, '$.data')) AS data_type
FROM iceberg_catalog.db.table_with_variants;
```

### 将 VARIANT 数据转换为 SQL 类型

您可以使用 CAST 函数将 VARIANT 数据转换为标准 SQL 类型:

```SQL
SELECT
    CAST(variant_query(variant_col, '$.count') AS INT) AS count,
    CAST(variant_query(variant_col, '$.price') AS DECIMAL(10, 2)) AS price,
    CAST(variant_query(variant_col, '$.active') AS BOOLEAN) AS is_active,
    CAST(variant_query(variant_col, '$.name') AS STRING) AS name
FROM iceberg_catalog.db.table_with_variants;
```

复杂类型也可以从 VARIANT 转换:

```SQL
SELECT
    CAST(variant_col AS STRUCT<id INT, name STRING>) AS user_struct,
    CAST(variant_col AS MAP<STRING, INT>) AS config_map,
    CAST(variant_col AS ARRAY<DOUBLE>) AS values_array
FROM iceberg_catalog.db.table_with_variants;
```

## VARIANT 函数

VARIANT 函数可用于查询和提取 VARIANT 列中的数据。有关详细信息,请参阅各个函数的文档:

- [variant_query](../../sql-functions/variant-functions/variant_query.md):查询 VARIANT 值中的路径并返回 VARIANT
- [get_variant_int](../../sql-functions/variant-functions/get_variant_int.md):从 VARIANT 提取整数值
- [get_variant_bool](../../sql-functions/variant-functions/get_variant_bool.md):从 VARIANT 提取布尔值
- [get_variant_double](../../sql-functions/variant-functions/get_variant_double.md):从 VARIANT 提取双精度浮点值
- [get_variant_string](../../sql-functions/variant-functions/get_variant_string.md):从 VARIANT 提取字符串值
- [variant_typeof](../../sql-functions/variant-functions/variant_typeof.md):返回 VARIANT 值的类型名称

## VARIANT 路径表达式

VARIANT 函数使用 JSON 路径表达式来导航数据结构。路径语法类似于 JSON 路径:

- `$` 表示 VARIANT 值的根
- `.` 用于访问对象字段
- `[index]` 用于访问数组元素(基于 0 的索引)
- 包含特殊字符(如点)的字段名可以用引号括起来:`$.\"field.name\"`

路径表达式示例:

```plaintext
$                      -- 根元素
$.field                -- 对象字段访问
$.nested.field         -- 嵌套字段访问
$.\"field.with.dots\"    -- 带引号的字段名
$[0]                   -- 第一个数组元素
$.array[1]             -- 数组字段的第二个元素
$.users[0].name        -- 嵌套数组访问
$.config[\"key\"]        -- Map 风格访问
```

## 数据类型转换

当从带有 variant 编码的 Parquet 文件读取数据时,支持以下类型转换:

| Parquet Variant 类型 | StarRocks VARIANT 类型 |
| -------------------- | ------------ |
| INT8, INT16, INT32, INT64 | int8, int16, int32, int64 |
| FLOAT, DOUBLE | float, double |
| BOOLEAN | boolean |
| STRING | string |
| DATE | Date |
| TIMESTAMP, TIMESTAMP_NTZ | Timestamp |
| DECIMAL | Decimal, float, double|
| STRUCT | Object |
| MAP | Object |
| ARRAY | Array |

## 限制和注意事项

- VARIANT 类型目前仅支持从带有 variant 编码的 Parquet 格式的 Iceberg 表中读取数据。
- VARIANT 值的大小限制为 16 MB。
- 当前仅支持读取未分片的 variant 值。
- 嵌套结构的最大深度取决于底层 Parquet 文件结构。
