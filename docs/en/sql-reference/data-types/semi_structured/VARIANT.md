---
displayed_sidebar: docs
---

# VARIANT

Since version 4.0, StarRocks supports the VARIANT data type for querying semi-structured data from Iceberg tables in Parquet format. This article introduces the basic concepts of VARIANT, and how StarRocks queries VARIANT-type data and processes it through variant functions.

## What is VARIANT

VARIANT is a semi-structured data type that can store values of different data types, including primitive types (integers, floating-point numbers, strings, booleans, dates, timestamps), and complex types (structs, maps, arrays). VARIANT data is encoded in binary format for efficient storage and querying.

The VARIANT type is particularly useful when working with data from Apache Iceberg tables that use the Parquet format with variant encoding, which allows for flexible schema evolution and efficient storage of heterogeneous data.

For more information on the Parquet variant encoding format, see the [Parquet Variant Encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).

## Using VARIANT Data

### Querying VARIANT-Type Data from Iceberg Tables

StarRocks supports querying VARIANT-type data from Iceberg tables stored in Parquet format. VARIANT columns are automatically recognized when querying Iceberg tables that use Parquet's variant encoding.

Example of querying an Iceberg table with VARIANT columns:

```SQL

-- Query a table with VARIANT columns
SELECT
    id,
    variant_col
FROM iceberg_catalog.db.table_with_variants;
```

### Extracting Values from VARIANT Data

StarRocks provides several functions to extract typed values from VARIANT data:

Example 1: Extract primitive values using typed getter functions.

```SQL
SELECT
    get_variant_int(variant_col, '$') AS int_value,
    get_variant_string(variant_col, '$') AS string_value,
    get_variant_bool(variant_col, '$') AS bool_value,
    get_variant_double(variant_col, '$') AS double_value
FROM iceberg_catalog.db.table_with_variants;
```

Example 2: Navigate nested structures using JSON path expressions.

```SQL
SELECT
    get_variant_string(variant_col, '$.user.name') AS user_name,
    get_variant_int(variant_col, '$.user.age') AS user_age,
    get_variant_string(variant_col, '$.address.city') AS city
FROM iceberg_catalog.db.table_with_variants;
```

Example 3: Access array elements.

```SQL
SELECT
    get_variant_int(variant_col, '$.scores[0]') AS first_score,
    get_variant_int(variant_col, '$.scores[1]') AS second_score
FROM iceberg_catalog.db.table_with_variants;
```

Example 4: Query nested variant data and return VARIANT type.

```SQL
SELECT
    variant_query(variant_col, '$.metadata') AS metadata,
    variant_query(variant_col, '$.items[0]') AS first_item
FROM iceberg_catalog.db.table_with_variants;
```

Example 5: Check the type of a VARIANT value.

```SQL
SELECT
    variant_typeof(variant_col) AS root_type,
    variant_typeof(variant_query(variant_col, '$.data')) AS data_type
FROM iceberg_catalog.db.table_with_variants;
```

### Casting JSON to VARIANT

StarRocks supports casting JSON values to VARIANT. If your input is a STRING, convert it to JSON first.

```SQL
SELECT
    CAST(parse_json('{"id": 1, "flags": {"active": true}, "scores": [1.5, null]}') AS VARIANT) AS variant_value;
```

```SQL
SELECT
    CAST(json_col AS VARIANT) AS variant_value
FROM db.table_with_json;
```

### Casting VARIANT Data to SQL Types

You can use the CAST function to convert VARIANT data to standard SQL types:

```SQL
SELECT
    CAST(variant_query(variant_col, '$.count') AS INT) AS count,
    CAST(variant_query(variant_col, '$.price') AS DECIMAL(10, 2)) AS price,
    CAST(variant_query(variant_col, '$.active') AS BOOLEAN) AS is_active,
    CAST(variant_query(variant_col, '$.name') AS STRING) AS name
FROM iceberg_catalog.db.table_with_variants;
```

Complex types can also be cast from VARIANT:

```SQL
SELECT
    CAST(variant_col AS STRUCT<id INT, name STRING>) AS user_struct,
    CAST(variant_col AS MAP<STRING, INT>) AS config_map,
    CAST(variant_col AS ARRAY<DOUBLE>) AS values_array
FROM iceberg_catalog.db.table_with_variants;
```

### Casting SQL Types to VARIANT

You can cast SQL values to VARIANT. Supported input types include BOOLEAN, integer types, FLOAT/DOUBLE, DECIMAL, STRING/CHAR/VARCHAR, JSON, and DATE/DATETIME/TIME. Types such as HLL, BITMAP, PERCENTILE, VARBINARY, and complex types (ARRAY, MAP, STRUCT) are not supported.

```SQL
SELECT
    CAST(123 AS VARIANT) AS v_int,
    CAST(3.14 AS VARIANT) AS v_double,
    CAST(DECIMAL(10, 2) '12.34' AS VARIANT) AS v_decimal,
    CAST('hello' AS VARIANT) AS v_string,
    CAST(PARSE_JSON('{"k":1}') AS VARIANT) AS v_json;
```

## VARIANT Functions

VARIANT functions can be used to query and extract data from VARIANT columns. For detailed information, please refer to the individual function documentation:

- [variant_query](../../sql-functions/variant-functions/variant_query.md): Queries a path in a VARIANT value and returns a VARIANT
- [get_variant](../../sql-functions/variant-functions/get_variant.md): Extracts typed values (int, bool, double, string) from a VARIANT
- [variant_typeof](../../sql-functions/variant-functions/variant_typeof.md): Returns the type name of a VARIANT value

## VARIANT Path Expressions

VARIANT functions use JSON path expressions to navigate through the data structure. The path syntax is similar to JSON path:

- `$` represents the root of the VARIANT value
- `.` is used to access object fields
- `[index]` is used to access array elements (0-based indexing)
- Field names with special characters (like dots) can be quoted: `$."field.name"`

Examples of path expressions:

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

## Data Type Conversion

When data is read from Parquet files with variant encoding, the following type conversions are supported:

| Parquet Variant Type | StarRocks VARIANT Type |
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

## Limitations and Considerations

- VARIANT is supported for reading data from Iceberg tables in Parquet format with variant encoding, and for writing Parquet files using StarRocks file writers (unshredded variant encoding).
- The size of a VARIANT value is limited to 16 MB.
- Currently only unshredded variant values are supported for both read and write.
- Currently, VARIANT can only be created by casting from JSON values (for example, `CAST(parse_json(...) AS VARIANT)`).
- The maximum depth of nested structures depends on the underlying Parquet file structure.
