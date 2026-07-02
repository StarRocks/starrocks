---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Lance catalog

<Beta />

StarRocks 支持 Lance catalog 作为 external catalog。您可以通过 Lance catalog 查询已有 Lance 数据集，无需将数据导入 StarRocks。

## 使用注意事项

- 当前实现支持只读表扫描。
- 当前支持的 catalog 类型为 `directory`。StarRocks 从一个 warehouse 目录中发现 Lance 数据集。
- 当前不支持 Lance 向量索引搜索和 KNN 查询加速。
- Lance 数据集目录名必须以 `.lance` 结尾。

## 目录布局

StarRocks 会将 warehouse 路径下的 Lance 数据集映射为数据库和表。

| 数据集路径 | StarRocks 对象 |
| --- | --- |
| `<warehouse>/<table>.lance` | `default` 数据库中的表 `<table>` |
| `<warehouse>/default/<table>.lance` | `default` 数据库中的表 `<table>` |
| `<warehouse>/<db>/<table>.lance` | `<db>` 数据库中的表 `<table>` |

`default` 数据库始终存在。其他数据库对应 warehouse 路径下的子目录，但目录名以 `.lance` 结尾的目录除外。

## 数据类型映射

| Lance/Arrow 类型 | StarRocks 类型 |
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

## 创建 Lance catalog

### 语法

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

### 参数

#### catalog_name

Lance catalog 的名称。名称区分大小写，可以包含字母、数字和下划线，且必须以字母开头。

#### type

数据源类型。设置为 `lance`。

#### lance.catalog.type

Lance catalog 的类型。设置为 `directory`。如果不指定该属性，StarRocks 默认使用 `directory`。

#### lance.catalog.warehouse

存放 Lance 数据集的 warehouse 路径。例如：`s3://bucket/path/to/lance_warehouse`。

#### StorageCredentialParams

用于访问 warehouse 的存储认证参数。如果使用 HDFS 或本地文件系统，通常不需要配置存储认证参数。

如果使用 AWS S3 或兼容 S3 的对象存储，可以使用与 Hive、Iceberg catalog 相同的 `aws.s3.*` 属性。例如：

```SQL
"aws.s3.use_instance_profile" = "false",
"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secret_key>",
"aws.s3.endpoint" = "<endpoint>",
"aws.s3.region" = "<region>"
```

如果使用阿里云 OSS，可以使用 `aliyun.oss.*` 属性。例如：

```SQL
"aliyun.oss.access_key" = "<access_key>",
"aliyun.oss.secret_key" = "<secret_key>",
"aliyun.oss.endpoint" = "<endpoint>",
"aliyun.oss.region" = "<region>"
```

您也可以通过 `lance.option.` 前缀传递原始 Lance object store option。StarRocks 会移除该前缀后将 option 传递给 Lance SDK。例如：

```SQL
"lance.option.aws_allow_http" = "true"
```

## 示例

### 查询 S3 兼容对象存储上的 Lance 数据集

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

### 查询 HDFS 上的 Lance 数据集

```SQL
CREATE EXTERNAL CATALOG lance_hdfs
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.warehouse" = "hdfs://namenode:8020/user/lance"
);

SELECT count(*) FROM lance_hdfs.default.my_table;
```
