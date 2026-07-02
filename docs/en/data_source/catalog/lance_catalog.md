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
- If Lance datasets are stored on a local file system, the FE and all BE/CN nodes that may run the scan must be able to access the same path.
- If Lance datasets are stored on object storage, we recommend using an S3-compatible path in the format `s3://<bucket>/<prefix>` and passing the object store options required by the Lance SDK through `lance.option.*`.

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

For Lance datasets on OSS, we recommend using an `s3://` warehouse and `aws.s3.*` properties, and accessing OSS as S3-compatible object storage. The StarRocks FE uses Hadoop S3A to list the warehouse directory, so it uses `aws.s3.endpoint` and `aws.s3.enable_path_style_access`. The Lance SDK uses `lance.option.aws_endpoint` and `lance.option.aws_virtual_hosted_style_request` when it opens and scans a dataset. These properties configure different clients. On OSS, the Lance SDK usually needs the bucket endpoint and virtual-hosted-style requests. For an example, see [Query Lance datasets on Aliyun OSS](#query-lance-datasets-on-aliyun-oss).

You can pass raw Lance object store options by prefixing the option name with `lance.option.`. StarRocks removes this prefix before passing the option to the Lance SDK. For example:

```SQL
"lance.option.aws_allow_http" = "true"
```

## Examples

### Query Lance datasets on a local file system

Assume that your local directory layout is as follows:

```Plain
/data/lance_warehouse/default/smoke.lance/
  _transactions/
  _versions/
  data/
```

Create a catalog:

```SQL
CREATE EXTERNAL CATALOG lance_local
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "file:///data/lance_warehouse"
);

SHOW DATABASES FROM lance_local;
SHOW TABLES FROM lance_local.`default`;
DESC lance_local.`default`.smoke;
SELECT * FROM lance_local.`default`.smoke LIMIT 10;
```

If your deployment has multiple BE/CN nodes, `file:///data/lance_warehouse` must be accessible on every node that may run the scan, and the path contents must be consistent. Otherwise, the FE may discover the table, but the BE/CN scan can fail because the data files cannot be found.

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
SHOW TABLES FROM lance_catalog.`default`;
SELECT * FROM lance_catalog.`default`.my_table LIMIT 10;
```

### Query Lance datasets on Aliyun OSS

The OSS directory must use the Lance directory catalog layout. Example:

```Plain
oss://olap-beijing/path/to/lance_warehouse/default/smoke.lance/
  _transactions/
  _versions/
  data/
```

We recommend using an `s3://` warehouse to access OSS and separating the endpoints used by the FE/Hadoop path and the Lance SDK path:

- `aws.s3.endpoint`: used by the StarRocks FE through Hadoop S3A to list the warehouse directory. Use the regional endpoint, such as `https://oss-cn-beijing-internal.aliyuncs.com`.
- `aws.s3.enable_path_style_access`: controls how the FE/Hadoop S3A client accesses OSS. When you use an OSS regional endpoint, set it to `false`.
- `lance.option.aws_endpoint`: used by the Lance SDK to open and scan a specific Lance dataset. For OSS, use the bucket endpoint, such as `https://olap-beijing.oss-cn-beijing-internal.aliyuncs.com`.
- `lance.option.aws_virtual_hosted_style_request`: controls how the Lance SDK accesses OSS. When you use an OSS bucket endpoint, set it to `true`.

Do not set `lance.option.aws_endpoint` to `https://oss-cn-beijing-internal.aliyuncs.com` together with `lance.option.aws_virtual_hosted_style_request=false`. That combination makes the Lance SDK access OSS in path-style mode and can make dataset opening fail.

Example:

```SQL
CREATE EXTERNAL CATALOG lance_oss
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "s3://olap-beijing/path/to/lance_warehouse",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>",
    "aws.s3.endpoint" = "https://oss-cn-beijing-internal.aliyuncs.com",
    "aws.s3.region" = "cn-beijing",
    "aws.s3.enable_path_style_access" = "false",
    "lance.option.aws_endpoint" = "https://olap-beijing.oss-cn-beijing-internal.aliyuncs.com",
    "lance.option.aws_region" = "cn-beijing",
    "lance.option.aws_virtual_hosted_style_request" = "true"
);

SHOW DATABASES FROM lance_oss;
SHOW TABLES FROM lance_oss.`default`;
DESC lance_oss.`default`.smoke;
SELECT * FROM lance_oss.`default`.smoke LIMIT 10;
```

If you use a public endpoint, replace `oss-cn-beijing-internal.aliyuncs.com` in the example with `oss-cn-beijing.aliyuncs.com`.

Avoid using path-style access against a regular OSS second-level endpoint, such as `https://oss-cn-beijing.aliyuncs.com/<bucket>`, as the OSS service may return `SecondLevelDomainForbidden` and require virtual-hosted-style access.

### Query Lance datasets on HDFS

```SQL
CREATE EXTERNAL CATALOG lance_hdfs
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.warehouse" = "hdfs://namenode:8020/user/lance"
);

SELECT count(*) FROM lance_hdfs.`default`.my_table;
```

## How it works

The Lance catalog read path consists of metadata discovery, query planning, and BE/CN scanning.

### Metadata discovery

When the FE creates a Lance catalog, it reads the catalog properties and creates an `HdfsEnvironment`. For a `directory` catalog, the FE lists the warehouse path:

- `<warehouse>/<table>.lance` is mapped to `default.<table>`.
- `<warehouse>/default/<table>.lance` is mapped to `default.<table>`.
- `<warehouse>/<db>/<table>.lance` is mapped to `<db>.<table>`.

When a user runs `DESC`, queries a table, or the optimizer needs the table schema, the FE opens the dataset with the Lance Java SDK and converts the Arrow schema to StarRocks types.

### Storage option propagation

StarRocks converts catalog properties into Lance SDK storage options:

- `aws.s3.access_key` -> `aws_access_key_id`
- `aws.s3.secret_key` -> `aws_secret_access_key`
- `aws.s3.session_token` -> `aws_session_token`
- `aws.s3.endpoint` -> `aws_endpoint`
- `aws.s3.region` -> `aws_region`
- `aws.s3.enable_path_style_access=true` -> `aws_virtual_hosted_style_request=false`
- `aliyun.oss.*` properties are mapped to the corresponding `aws_*` options.
- `lance.option.<key>` is passed to the Lance SDK as `<key>`.

Therefore, in OSS deployments, you can use `aws.s3.*` for StarRocks and Hadoop directory listing, and use `lance.option.aws_endpoint` and `lance.option.aws_virtual_hosted_style_request` to tune how the Lance SDK accesses object storage. In practice, `aws.s3.endpoint` usually uses an OSS regional endpoint, while `lance.option.aws_endpoint` usually uses a bucket endpoint. Do not mix these two endpoint styles.

### Query planning

The FE Lance scan node uses the Lance Java SDK to read dataset fragments. Each fragment becomes a scan range that contains:

- Dataset URI
- Fragment ID
- Lance storage options

The Lance scan node uses the connector scan scheduler, and StarRocks assigns the scan ranges to available BE/CN nodes.

### BE/CN scanning

After a BE/CN receives a scan range, it enables the Lance native reader. The native reader calls the Lance Rust SDK through Rust FFI and passes the dataset URI, fragment ID, selected columns, and storage options to the Rust-side reader. The Rust-side reader opens the dataset again, reads Arrow batches for the requested fragment and columns, and returns the batches to C++ through the Arrow C Data Interface. The BE/CN then reuses StarRocks' Arrow-to-Column conversion path, writes the values into StarRocks columns, and returns the data to the execution engine.

The current implementation is a read-only scan path. It does not write Lance datasets, run Lance vector index search, or accelerate KNN queries.
