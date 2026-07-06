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
- Lance 向量索引搜索仅支持 native reader。
- Lance 数据集目录名必须以 `.lance` 结尾。
- 如果 Lance 数据集存放在本地文件系统上，FE 和所有参与扫描的 BE/CN 都必须能访问相同路径。
- 如果 Lance 数据集存放在对象存储上，建议使用 S3 兼容路径 `s3://<bucket>/<prefix>`，并通过 `lance.option.*` 传递 Lance SDK 需要的 object store 选项。

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

对于 OSS 上的 Lance 数据集，推荐使用 `s3://` warehouse 和 `aws.s3.*` 属性，将 OSS 作为 S3 兼容对象存储访问。StarRocks FE 通过 Hadoop S3A 列举 warehouse 目录，使用 `aws.s3.endpoint` 和 `aws.s3.enable_path_style_access`。Lance SDK 打开和扫描具体 dataset 时使用 `lance.option.aws_endpoint` 和 `lance.option.aws_virtual_hosted_style_request`。这两组配置不是同一个客户端，OSS 场景下通常需要给 Lance SDK 配置 bucket endpoint 并开启 virtual-hosted-style。示例见 [查询阿里云 OSS 上的 Lance 数据集](#查询阿里云-oss-上的-lance-数据集)。

您也可以通过 `lance.option.` 前缀传递原始 Lance object store option。StarRocks 会移除该前缀后将 option 传递给 Lance SDK。例如：

```SQL
"lance.option.aws_allow_http" = "true"
```

## 示例

### 查询本地文件系统上的 Lance 数据集

本地目录布局如下：

```Plain
/data/lance_warehouse/default/smoke.lance/
  _transactions/
  _versions/
  data/
```

创建 catalog：

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

如果部署了多个 BE/CN，`file:///data/lance_warehouse` 必须在所有参与扫描的节点上可访问，并且路径内容一致。否则 FE 可以发现表，但 BE/CN 执行扫描时可能因为找不到数据文件而失败。

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
SHOW TABLES FROM lance_catalog.`default`;
SELECT * FROM lance_catalog.`default`.my_table LIMIT 10;
```

### 查询阿里云 OSS 上的 Lance 数据集

OSS 上的数据目录需要保持 Lance directory catalog 布局。例如：

```Plain
oss://olap-beijing/path/to/lance_warehouse/default/smoke.lance/
  _transactions/
  _versions/
  data/
```

推荐使用 `s3://` warehouse 访问 OSS，并区分 FE/Hadoop 和 Lance SDK 的 endpoint：

- `aws.s3.endpoint`：供 StarRocks FE 使用 Hadoop S3A 文件系统列举 warehouse 目录。建议使用区域 endpoint，例如 `https://oss-cn-beijing-internal.aliyuncs.com`。
- `aws.s3.enable_path_style_access`：控制 FE/Hadoop S3A 的访问方式。使用 OSS 区域 endpoint 时，建议设置为 `false`。
- `lance.option.aws_endpoint`：供 Lance SDK 打开和扫描具体 Lance 数据集。对于 OSS，需要使用 bucket endpoint，例如 `https://olap-beijing.oss-cn-beijing-internal.aliyuncs.com`。
- `lance.option.aws_virtual_hosted_style_request`：控制 Lance SDK 的访问方式。使用 OSS bucket endpoint 时，必须设置为 `true`。

不要把 `lance.option.aws_endpoint` 配成 `https://oss-cn-beijing-internal.aliyuncs.com` 再把 `lance.option.aws_virtual_hosted_style_request` 配成 `false`。该组合会让 Lance SDK 按 path-style 访问 OSS，容易导致 dataset 打开失败。

示例：

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

如果使用公网 endpoint，将示例中的 `oss-cn-beijing-internal.aliyuncs.com` 替换为 `oss-cn-beijing.aliyuncs.com`。

注意，不建议优先使用普通二级 endpoint 的 path-style 配置，例如 `https://oss-cn-beijing.aliyuncs.com/<bucket>`。在 OSS 上，该方式可能返回 `SecondLevelDomainForbidden`，提示必须使用 virtual-hosted-style。

### 查询 HDFS 上的 Lance 数据集

```SQL
CREATE EXTERNAL CATALOG lance_hdfs
PROPERTIES
(
    "type" = "lance",
    "lance.catalog.warehouse" = "hdfs://namenode:8020/user/lance"
);

SELECT count(*) FROM lance_hdfs.`default`.my_table;
```

## Reader 选择

默认情况下，BE/CN 使用 Lance native reader 读取数据。Native reader 通过 Rust FFI 调用 Lance Rust SDK，避免数据扫描阶段走 Java JNI reader。

如需诊断 native reader 问题，或临时回退到 Lance Java SDK reader，可以在会话级别强制使用 JNI reader：

```SQL
SET lance_force_jni_reader = true;
SELECT * FROM lance_oss.`default`.smoke LIMIT 10;
```

恢复默认 native reader：

```SQL
SET lance_force_jni_reader = false;
```

也可以显式设置 native reader：

```SQL
SET lance_force_native_reader = true;
```

如果同时设置 `lance_force_jni_reader=true` 和 `lance_force_native_reader=true`，`lance_force_jni_reader` 优先，查询会走 Java SDK reader。

## 向量索引搜索

Lance 向量索引搜索支持 `ARRAY<FLOAT>` 列上的 `approx_l2_distance`。查询必须按一个 `approx_l2_distance` 表达式升序排序，并使用常量查询向量。

示例：

```SQL
SET lance_force_jni_reader = false;
SET lance_force_native_reader = true;

SELECT id,
       approx_l2_distance(CAST('[1.0,2.0,3.0]' AS ARRAY<FLOAT>), vector) AS score
FROM lance_oss.`default`.sift1m_ivfpq
ORDER BY score
LIMIT 10;
```

可以通过 `ann_params` 调整 Lance 搜索参数。`ann_params` 是 JSON 字符串，Lance 参数名必须使用小写。

```SQL
SET ann_params = '{"lance.nprobes":"32","lance.refine_factor":"4","lance.ef":"128","lance.query_parallelism":"4"}';
```

| 参数 | 取值 | 说明 |
| --- | --- | --- |
| `lance.nprobes` | 正整数 | IVF 分区探测数量。适用于基于 IVF 的 Lance 向量索引。 |
| `lance.refine_factor` | 正整数 | Lance 近似向量搜索后的 refine factor。常用于 IVF_PQ、IVF_HNSW_SQ 等压缩索引。 |
| `lance.ef` | 正整数 | HNSW 搜索的 `ef` 参数。适用于 IVF_HNSW_* Lance 向量索引。 |
| `lance.query_parallelism` | `-1` 或非负整数 | 每个 Lance 向量查询的分区搜索并发度。`0` 使用 Lance 默认策略，`-1` 使用 CPU pool 大小，正数表示指定并发度。 |

当前 Lance 向量搜索有以下限制：

- 仅支持 native reader。如果设置 `lance_force_jni_reader=true`，查询会失败。
- 仅通过 `approx_l2_distance` 支持 L2 距离。
- v1 不支持普通 scan predicate。
- 被选中的 Lance 向量索引必须覆盖 dataset 中所有 live fragment。

## 实现原理

Lance catalog 的读路径分为元数据发现、查询规划和 BE/CN 扫描三个阶段。

### 元数据发现

FE 创建 Lance catalog 时会读取 catalog properties，并构造 `HdfsEnvironment`。对于 `directory` catalog，FE 使用 warehouse 路径列举目录：

- `<warehouse>/<table>.lance` 映射为 `default.<table>`。
- `<warehouse>/default/<table>.lance` 映射为 `default.<table>`。
- `<warehouse>/<db>/<table>.lance` 映射为 `<db>.<table>`。

当用户执行 `DESC`、查询表或优化器需要表结构时，FE 使用 Lance Java SDK 打开具体 dataset，并将 Arrow schema 转换为 StarRocks 类型。

### 存储参数传递

StarRocks 会把 catalog properties 转换为 Lance SDK 的 storage options：

- `aws.s3.access_key` -> `aws_access_key_id`
- `aws.s3.secret_key` -> `aws_secret_access_key`
- `aws.s3.session_token` -> `aws_session_token`
- `aws.s3.endpoint` -> `aws_endpoint`
- `aws.s3.region` -> `aws_region`
- `aws.s3.enable_path_style_access=true` -> `aws_virtual_hosted_style_request=false`
- `aliyun.oss.*` 会映射到对应的 `aws_*` option。
- `lance.option.<key>` 会去掉 `lance.option.` 前缀后原样传递给 Lance SDK。

因此，OSS 场景可以用 `aws.s3.*` 满足 StarRocks/Hadoop 的目录列举，同时用 `lance.option.aws_endpoint`、`lance.option.aws_virtual_hosted_style_request` 调整 Lance SDK 访问对象存储的方式。实践中，`aws.s3.endpoint` 通常使用 OSS 区域 endpoint，而 `lance.option.aws_endpoint` 通常使用 bucket endpoint，两者不要混用。

### 查询规划

FE 的 Lance scan node 使用 Lance Java SDK 读取 dataset fragments。每个 fragment 会生成一个 scan range，scan range 中包含：

- dataset URI
- fragment ID
- Lance storage options

Lance scan node 会走 connector scan scheduler，由 StarRocks 将 scan range 分配给可用 BE/CN。

对于向量索引搜索，FE 会加载 Lance index metadata，校验被选中的向量索引是否覆盖所有 live fragment，并为每个 Lance index segment 生成一个 scan range。BE/CN 随后通过 Lance native reader 执行搜索。`ann_params` 中的 `lance.nprobes`、`lance.refine_factor`、`lance.ef`、`lance.query_parallelism` 等搜索参数会传递给 Lance Rust scanner。

### BE/CN 扫描

BE/CN 收到 scan range 后，默认启用 Lance native reader。Native reader 通过 Rust FFI 调用 Lance Rust SDK，并将 dataset URI、fragment ID、列裁剪结果和 storage options 传给 Rust 侧 reader。Rust 侧 reader 再次打开 dataset，按 fragment 和所需列读取 Arrow batch，并通过 Arrow C Data Interface 将 batch 传回 C++。BE/CN 随后复用 StarRocks 的 Arrow 到 Column 转换逻辑，把数据写入 StarRocks column，最终返回给执行引擎。

当会话变量 `lance_force_jni_reader` 设置为 `true` 时，BE/CN 会改用 Lance Java SDK reader。该模式主要用于兼容性验证和 native reader 问题排查。

当前实现是只读扫描链路，不涉及 Lance 写入。
