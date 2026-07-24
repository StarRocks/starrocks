---
displayed_sidebar: docs
description: "VARIANT 类型仅支持 Iceberg Catalog 中的表。"
---

# VARIANT

:::important
VARIANT 类型仅支持 Iceberg Catalog 中的表，不支持 StarRocks 原生表。
:::

从 v4.1 版本起，StarRocks 支持 VARIANT 数据类型，用于查询 Parquet 格式的 Iceberg 表中的半结构化数据。本文介绍 VARIANT 的基本概念，以及 StarRocks 如何查询 VARIANT 类型数据并通过 VARIANT 函数对其进行处理。

## 什么是 VARIANT

VARIANT 是一种半结构化数据类型，可以存储不同数据类型的值，包括基本类型（整数、浮点数、字符串、布尔值、日期、时间戳）和复杂类型（结构体、映射、数组）。VARIANT 数据以二进制格式编码，以实现高效的存储和查询。

VARIANT 类型在处理使用 Parquet 格式并采用 variant 编码的 Apache Iceberg 表数据时尤为有用，它支持灵活的 Schema 演进以及异构数据的高效存储。

有关 Parquet variant 编码格式的更多信息，请参阅[Parquet Variant 编码规范](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)。

## 使用 VARIANT 数据

### 从 Iceberg 表查询 VARIANT 类型数据

StarRocks 支持从以 Parquet 格式存储的 Iceberg 表中查询 VARIANT 类型数据。在查询使用 Parquet variant 编码的 Iceberg 表时，VARIANT 列会被自动识别。

查询包含 VARIANT 列的 Iceberg 表示例：

```SQL
-- 查询包含 VARIANT 列的表
SELECT
    id,
    variant_col
FROM iceberg_catalog.db.table_with_variants;
```

### 从 VARIANT 数据中提取值

StarRocks 提供了多个函数，用于从 VARIANT 数据中提取带类型的值。

示例 1：使用带类型的 getter 函数提取基本类型值。

```SQL
SELECT
    get_variant_int(variant_col, '$') AS int_value,
    get_variant_string(variant_col, '$') AS string_value,
    get_variant_bool(variant_col, '$') AS bool_value,
    get_variant_double(variant_col, '$') AS double_value
FROM iceberg_catalog.db.table_with_variants;
```

示例 2：使用 JSON 路径表达式导航嵌套结构。

```SQL
SELECT
    get_variant_string(variant_col, '$.user.name') AS user_name,
    get_variant_int(variant_col, '$.user.age') AS user_age,
    get_variant_string(variant_col, '$.address.city') AS city
FROM iceberg_catalog.db.table_with_variants;
```

示例 3：访问数组元素。

```SQL
SELECT
    get_variant_int(variant_col, '$.scores[0]') AS first_score,
    get_variant_int(variant_col, '$.scores[1]') AS second_score
FROM iceberg_catalog.db.table_with_variants;
```

示例 4：查询嵌套 variant 数据并返回 VARIANT 类型。

```SQL
SELECT
    variant_query(variant_col, '$.metadata') AS metadata,
    variant_query(variant_col, '$.items[0]') AS first_item
FROM iceberg_catalog.db.table_with_variants;
```

示例 5：检查 VARIANT 值的类型。

```SQL
SELECT
    variant_typeof(variant_col) AS root_type,
    variant_typeof(variant_query(variant_col, '$.data')) AS data_type
FROM iceberg_catalog.db.table_with_variants;
```

### 将 JSON 转换为 VARIANT

StarRocks 支持将 JSON 值转换为 VARIANT。如果输入为 STRING，请先将其转换为 JSON。

```SQL
SELECT
    CAST(parse_json('{"id": 1, "flags": {"active": true}, "scores": [1.5, null]}') AS VARIANT) AS variant_value;
```

```SQL
SELECT
    CAST(json_col AS VARIANT) AS variant_value
FROM db.table_with_json;
```

### 将 VARIANT 数据转换为 SQL 类型

您可以使用 CAST 函数将 VARIANT 数据转换为标准 SQL 类型：

```SQL
SELECT
    CAST(variant_query(variant_col, '$.count') AS INT) AS count,
    CAST(variant_query(variant_col, '$.price') AS DECIMAL(10, 2)) AS price,
    CAST(variant_query(variant_col, '$.active') AS BOOLEAN) AS is_active,
    CAST(variant_query(variant_col, '$.name') AS STRING) AS name
FROM iceberg_catalog.db.table_with_variants;
```

复杂类型也可以从 VARIANT 进行转换：

```SQL
SELECT
    CAST(variant_col AS STRUCT<id INT, name STRING>) AS user_struct,
    CAST(variant_col AS MAP<STRING, INT>) AS config_map,
    CAST(variant_col AS ARRAY<DOUBLE>) AS values_array
FROM iceberg_catalog.db.table_with_variants;
```

### 将 SQL 类型转换为 VARIANT

您可以将 SQL 值转换为 VARIANT。支持的输入类型包括 BOOLEAN、整数类型、FLOAT/DOUBLE、DECIMAL、STRING/CHAR/VARCHAR、JSON、DATE/DATETIME/TIME 以及复杂类型（ARRAY、MAP、STRUCT）。对于 MAP，键在编码时会被转换为字符串。不支持 HLL、BITMAP、PERCENTILE 和 VARBINARY 等类型。

```SQL
SELECT
    CAST(123 AS VARIANT) AS v_int,
    CAST(3.14 AS VARIANT) AS v_double,
    CAST(CAST('12.34' AS DECIMAL(10, 2)) AS VARIANT) AS v_decimal,
    CAST('hello' AS VARIANT) AS v_string,
    CAST(PARSE_JSON('{"k":1}') AS VARIANT) AS v_json;
```

## VARIANT 函数

VARIANT 函数可用于查询和提取 VARIANT 列中的数据。有关详细信息，请参阅各函数文档：

- [variant_query](../../sql-functions/variant-functions/variant_query.md)：查询 VARIANT 值中的路径并返回 VARIANT
- [get_variant](../../sql-functions/variant-functions/get_variant.md)：从 VARIANT 中提取带类型的值（int、bool、double、string）
- [variant_typeof](../../sql-functions/variant-functions/variant_typeof.md)：返回 VARIANT 值的类型名称

## VARIANT 路径表达式

VARIANT 函数使用 JSON 路径表达式来导航数据结构。路径语法与 JSON 路径类似：

- `$` 表示 VARIANT 值的根
- `.` 用于访问对象字段
- `[index]` 用于访问数组元素（从 0 开始索引）
- 包含特殊字符（如点号）的字段名可以加引号：`$."field.name"`

路径表达式示例：

```plaintext
$                      -- Root element
$.field                -- Object field access
$.nested.field         -- Nested field access
$."field.with.dots"    -- Quoted field name
$[0]                   -- First array element
$.array[1]             -- Second element of array field
$.users[0].name        -- Nested array access
$.config["key"]        -- Map-style access
```

## 数据类型转换

从具有 variant 编码的 Parquet 文件读取数据时，支持以下类型转换：

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

## 限制与注意事项

- VARIANT 支持从 Parquet 格式的 Iceberg 表中读取具有 variant 编码的数据，以及使用 StarRocks 文件写入器写入 Parquet 文件（非分片 variant 编码）。
- VARIANT 值的大小限制为 16 MB。
- 目前读写均仅支持非分片 variant 值。
- VARIANT 可通过从 JSON 值或支持的 SQL 类型（包括 ARRAY、MAP 和 STRUCT）进行类型转换来创建。
- 嵌套结构的最大深度取决于底层 Parquet 文件结构。
