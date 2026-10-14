---
sidebar_position: 160
displayed_sidebar: docs
description: 通过只读外部 Catalog 和 Catalog 级对象存储凭据查询 Lance 数据集。
---

# Lance catalog

Lance Connector 为实验性功能。FE 使用 Lance Java SDK 12.0.0 发现数据集并读取 Schema，BE 通过 Lance Rust SDK 和 Arrow C Data Interface 读取数据。部署时需要同时安装 FE 元数据依赖库、BE Connector 和原生读取库。

## 创建 Catalog

配置一个 Directory Catalog，将 Warehouse 指向已有 Lance 数据集所在的目录。Connector 会发现 Warehouse 下的直接子数据集，并从 Lance 格式读取各表的 Schema，包括列的可空属性。例如，以下 Warehouse 中的 `vectors.lance` 对应表 `vectors`：

```sql
CREATE EXTERNAL CATALOG lance_data
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "s3://example-bucket/datasets/",
    "lance.namespace.root_database" = "datasets",
    "aws.s3.access_key" = "<access-key>",
    "aws.s3.secret_key" = "<secret-key>",
    "aws.s3.region" = "us-east-1"
);

SHOW TABLES FROM lance_data.datasets;
SELECT id FROM lance_data.datasets.vectors WHERE id > 10 LIMIT 20;
```

| 参数 | 说明 |
| --- | --- |
| `lance.catalog.type` | Catalog 实现，默认为 `directory`。`rest` 预留用于后续支持，目前会被拒绝。 |
| `lance.catalog.warehouse` | 必填，包含 Lance 数据集的本地绝对路径或对象存储 URI。 |
| `lance.namespace.root_database` | Warehouse 根目录映射到的 SQL 数据库名称，默认为 `default`。该名称是别名，不是子目录。 |

不支持 `table.<name>.uri`、`table.<name>.schema` 以及旧的 `database` 参数。当前 Directory 实现不暴露嵌套 Namespace。Connector 只列出现有数据集，不创建或更新 Namespace Manifest。

使用临时 S3 凭据时，还需要设置 `aws.s3.session_token`。兼容 S3 的端点通过 `aws.s3.endpoint`、`aws.s3.enable_path_style_access` 和 `aws.s3.enable_ssl` 配置。FE 将 Catalog 的云存储配置发送给 BE，由读取器转换为 Lance 存储选项。

如需在 FE 和 BE 上使用原生凭据链，请设置 `aws.s3.use_aws_sdk_default_behavior = true`。此模式使用 Lance 原生对象存储凭据解析机制，而非 Java AWS SDK。不支持 Catalog 中显式配置的 STS 角色扮演、`aws.s3.use_instance_profile` 和 `aws.s3.use_web_identity_token_file`；这些配置会明确报错。

## Azure Data Lake Storage Gen2

使用包含存储账户信息的数据集 URI，以及具有读取和列出数据集文件权限的 SAS Token：

```sql
CREATE EXTERNAL CATALOG lance_azure
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "abfss://data@exampleaccount.dfs.core.windows.net/datasets/",
    "lance.namespace.root_database" = "datasets",
    "azure.adls2.storage_account" = "exampleaccount",
    "azure.adls2.sas_token" = "<sas-token>"
);

SELECT id, count(*)
FROM lance_azure.datasets.events
GROUP BY id;
```

Catalog 云存储配置中的 Azure Shared Key、标准 ADLS2 Managed Identity 和 ADLS2 Service Principal 凭据也会被转换为对应的存储选项。FE 和 BE 主机都必须能够访问存储；使用 Managed Identity 时，两端主机都必须具备该身份。Service Principal 认证目前要求使用 Azure 公有云登录地址。不支持 Workload Identity Token 文件、自定义 Azure 存储端点，以及使用 Catalog 凭据但未包含账户信息的 `az://` URI。不支持或不匹配的配置会报错，不会静默丢弃 Catalog 凭据。

SAS Token 和临时 S3 凭据属于静态 Catalog 配置，Connector 不会从远程凭据服务获取或刷新它们。请在发起新查询前替换即将过期的凭据。不要将凭据放入 SQL 谓词或数据集 URI 的查询参数中。

## 查询行为和限制

- 在 Warehouse 中放置多个 Lance 数据集，即可执行 Join、聚合、过滤和投影。Schema 在解析表时读取，不需要在 Catalog 参数中维护。
- 每次表扫描使用一个覆盖数据集全部 Fragment 的 Scan Range。尚未实现 Fragment 级并行扫描或向 Lance 下推谓词。StarRocks 在解码后的行上计算 SQL 谓词。
- 列裁剪会读取投影列和谓词所需的列。`COUNT(*)` 在存在标量列时保留一个标量列。
- 支持 Arrow Large String 和 Large Binary，其类型从数据集 Schema 推断。无符号整数通过扩宽类型避免溢出：`uint8` → `SMALLINT`、`uint16` → `INT`、`uint32` → `BIGINT`、`uint64` → `DECIMAL(20,0)`。
- 读取器支持标量值、日期、时间戳和列表。日期和时间戳保留 UTC 语义，包括 Unix Epoch 之前的小数时间值。不支持的 Schema 转换会导致查询失败，不会丢弃行。
- Catalog 为只读。本地文件 URI 必须在 FE 和执行扫描的 BE 上以相同路径可访问。
- Linux 上的原生读取库打包为 `be/lib/libstarrocks_lance.so`。BE 扫描不需要 Java 读取器或 JVM。FE 元数据 SDK 在独立 ClassLoader 中使用其自带的原生库。

Lance 使用 Rust SDK 12.0.0，构建时**默认关闭**。默认构建跳过 Cargo、Rust 工具链下载和 Lance SDK 打包。需要为 FE 元数据依赖库以及执行 Lance 扫描的每个 BE/CN 显式启用：

```bash
./build.sh --fe --be --with-connector-lance
```

运行 Connector 的定向测试：

```bash
./run-be-ut.sh --with-connector-lance --build-target connector_lance_test --module connector_lance_test --without-java-ext
```

直接使用 CMake 构建时，设置 `-DWITH_CONNECTOR_LANCE=ON`。使用 `--without-connector-lance` 或设置 `WITH_CONNECTOR_LANCE=OFF` 可显式关闭。命令行中显式指定的开关优先于环境变量。默认构建或显式关闭的构建会从 BE 安装包中排除之前构建的 Lance 动态库。启用后的 FE 安装包包含 `fe/lib/lance-metadata-lib`。默认 FE 安装包不包含这些 SDK 库，因此目录列举和 Schema 发现需要启用了 Lance 的 FE 安装包，查询执行还需要启用了 Lance 的 BE 安装包。

读取器只打开一次数据集，在一次扫描的所有批次中保持同一个不可变快照。FE 尚未在多个独立扫描算子之间固定共同版本，自连接也存在此限制。

原生构建要求 Rust 1.96.1 或更新版本。如果 Linux x86-64 或 AArch64 上没有合适的工具链，构建过程会下载并验证固定版本的官方工具链，将其放入 BE 构建目录。Rust 依赖由 `Cargo.lock` 锁定。可通过 `LANCE_BUILD_JOBS` 限制 Rust 编译并行度，默认为 2。离线构建需要预先安装工具链并准备完整的 Cargo 依赖缓存。
