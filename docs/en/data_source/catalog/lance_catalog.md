---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Lance catalog

<Beta />

StarRocks supports Lance catalogs as external catalogs. You can use a Lance catalog to query existing Lance datasets without loading data into StarRocks.

## Usage notes

- The current implementation supports read-only table scans.
- The supported catalog type is `directory`. StarRocks discovers Lance datasets from a warehouse directory.
- Lance vector index search and KNN query acceleration are not supported.
- Each Lance dataset directory must end with `.lance`.

## Directory layout

StarRocks maps Lance datasets under the warehouse path to databases and tables.

| Dataset path | StarRocks object |
| --- | --- |
| `<warehouse>/<table>.lance` | Table `<table>` in database `default` |
| `<warehouse>/default/<table>.lance` | Table `<table>` in database `default` |
| `<warehouse>/<db>/<table>.lance` | Table `<table>` in database `<db>` |

The `default` database always exists. Other databases are subdirectories under the warehouse path, excluding directories whose names end with `.lance`.

## Data type mapping

| Lance/Arrow type | StarRocks type |
| --- | --- |
| `Int(8)` | `TINYINT` |
| `Int(16)` | `SMALLINT` |
| `Int(32)` | `INT` |
| `Int(64)` | `BIGINT` |
| `FloatingPoint(SINGLE)` | `FLOAT` |
| `FloatingPoint(DOUBLE)` | `DOUBLE` |
| `Bool` | `BOOLEAN` |
| `Utf8`, `LargeUtf8` | `STRING` |
| `Binary`, `LargeBinary`, `FixedSizeBinary` | `VARBINARY` |
| `Date` | `DATE` |
| `Timestamp` | `DATETIME` |
| `Decimal` | `DECIMAL(precision, scale)` |
| `List`, `LargeList`, `FixedSizeList` | `ARRAY<element_type>` |
| `Map` | `MAP<key_type, value_type>` |
| `Struct` | `STRUCT<field1:type1, ...>` |

## Create a Lance catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "<warehouse_path>",
    StorageCredentialParams
);
```

### Parameters

#### catalog_name

The name of the Lance catalog. The name is case-sensitive and can contain letters, digits, and underscores. It must start with a letter.

#### type

The type of your data source. Set the value to `lance`.

#### lance.catalog.type

The type of Lance catalog. Set the value to `directory`. If this property is not specified, StarRocks uses `directory`.

#### lance.catalog.warehouse

The warehouse path that stores Lance datasets. Example: `s3://bucket/path/to/lance_warehouse`.

#### StorageCredentialParams

The storage credential parameters used to access the warehouse. For HDFS or local file systems, you usually do not need to configure storage credentials.

For AWS S3 or S3-compatible storage, use the same `aws.s3.*` properties as Hive and Iceberg catalogs. For example:

```SQL
"aws.s3.use_instance_profile" = "false",
"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secret_key>",
"aws.s3.endpoint" = "<endpoint>",
"aws.s3.region" = "<region>"
```

For Aliyun OSS, use the `aliyun.oss.*` properties. For example:

```SQL
"aliyun.oss.access_key" = "<access_key>",
"aliyun.oss.secret_key" = "<secret_key>",
"aliyun.oss.endpoint" = "<endpoint>",
"aliyun.oss.region" = "<region>"
```

You can pass raw Lance object store options by prefixing the option name with `lance.option.`. StarRocks removes this prefix before passing the option to the Lance SDK. For example:

```SQL
"lance.option.aws_allow_http" = "true"
```

## Examples

### Query Lance datasets on S3-compatible storage

```SQL
CREATE EXTERNAL CATALOG lance_catalog
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.warehouse" = "s3://example-bucket/lance",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.region" = "us-west-2"
);

SHOW DATABASES FROM lance_catalog;
SHOW TABLES FROM lance_catalog.default;
SELECT * FROM lance_catalog.default.my_table LIMIT 10;
```

### Query Lance datasets on HDFS

```SQL
CREATE EXTERNAL CATALOG lance_hdfs
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.warehouse" = "hdfs://namenode:8020/user/lance"
);

SELECT count(*) FROM lance_hdfs.default.my_table;
```
