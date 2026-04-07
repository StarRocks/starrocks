---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# Benchmark catalog

A benchmark catalog is a built-in external catalog that generates data on the fly for standard benchmark suites. It lets you run queries against TPC-H, TPC-DS, and SSB schemas without loading data.

> **NOTE**
>
> All data is generated in-flight at query time and is not persisted. Do not use this catalog as a benchmark source.

## Create a benchmark catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### Parameters

#### `catalog_name`

The name of the benchmark catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### `comment`

The description of the benchmark catalog. This parameter is optional.

#### `PROPERTIES`

The properties of the benchmark catalog. `PROPERTIES` must include the following parameters:

| Parameter | Required | Default value | Description |
| --------- | -------- | ------------- | ----------- |
| type      | Yes      | None          | The type of the data source. Set the value to `benchmark`. |
| scale     | No       | `1`           | Scale factor for generated data. The value must be greater than 0. Non-integer values are supported. |

### Example

```SQL
CREATE EXTERNAL CATALOG bench
PROPERTIES ("type" = "benchmark", "scale" = "1.0");
```

## Query benchmark data

The benchmark catalog exposes the following databases:

- `tpcds`: TPC-DS schema.
- `tpch`: TPC-H schema.
- `ssb`: Star Schema Benchmark schema.

The following example shows how to switch to the catalog and query the SSB schema:

```SQL
SET CATALOG bench;
SHOW DATABASES;
USE ssb;
SHOW TABLES;
SELECT COUNT(*) FROM date;
```

## Usage notes

- The schemas and tables are fixed to the built-in benchmark definitions, so you cannot create, alter, or drop objects in this catalog.
- The data is generated in-flight at query time and is not stored in any external system.
