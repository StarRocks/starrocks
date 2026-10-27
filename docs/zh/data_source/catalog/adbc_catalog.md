---
displayed_sidebar: docs
toc_max_heading_level: 4
description: "使用 ADBC Catalog 无需导入即可查询 Arrow Flight SQL 数据。"
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# ADBC Catalog

<Beta />

ADBC Catalog 是一种 External Catalog，可让 StarRocks 通过原生 [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/) 驱动程序查询外部数据源。初始版本支持查询 Arrow Flight SQL 服务。

FE 使用 ADBC 驱动程序发现 Schema、表和 Arrow 列类型。BE 或 CN 使用同一个驱动程序执行下推 SQL，并通过 Arrow C Data 接口导入返回的 Arrow 数据流。

## 使用限制

- 初始支持的数据源为 Arrow Flight SQL，不保证其他 ADBC 驱动程序可以正常工作。
- ADBC Catalog 仅支持读取。
- StarRocks 将 ADBC `db_schema` 映射为 StarRocks 数据库，不暴露 ADBC Catalog 层级。
- 每次扫描仅使用一个 ADBC 数据流，暂不支持分布式或分区读取。
- 支持以下 Arrow 类型：有符号和无符号整数、浮点数、Decimal、Boolean、UTF-8 字符串、Binary、Date、Timestamp 和 Null。暂不支持复杂 Arrow 类型。

Arrow Date32（天）映射为 StarRocks `DATE`；Date64（毫秒）映射为 `DATETIME`。

## 前提条件

### 安装原生 ADBC 驱动程序

StarRocks 会在安装包中提供自己构建的 `libadbc_driver_jni` 桥接库。使用标准 StarRocks 安装包时，不需要下载或替换此库。

数据源驱动程序不包含在 StarRocks 安装包中。请在每个 FE 和每个 BE 或 CN 节点上安装与 StarRocks ADBC Driver Manager 兼容的 ADBC 驱动程序。FE 和 BE 或 CN 进程都会直接加载该原生驱动程序。

请使用上游 ADBC Driver Manager 的发现机制注册驱动程序。推荐使用系统级驱动程序 Manifest。例如，要以逻辑名称 `adbc_driver_flightsql` 注册 Flight SQL 驱动程序，请在每个节点上创建 `/etc/adbc/drivers/adbc_driver_flightsql.toml`：

```toml
manifest_version = 1
name = "ADBC Flight SQL Driver"

[Driver]
entrypoint = "AdbcDriverInit"
shared = "/opt/adbc/lib/libadbc_driver_flightsql.so"
```

确保运行 StarRocks 的系统用户可以读取 Manifest 和动态库。您也可以使用其他 [ADBC 驱动程序 Manifest 搜索路径](https://arrow.apache.org/adbc/current/format/driver_manifests.html)，例如 `ADBC_DRIVER_PATH` 中列出的目录，但必须确保 FE 和 BE 或 CN 进程继承相同的发现配置。

Catalog 属性 `driver` 是不带 `.toml` 后缀的逻辑 Manifest 名称，而不是动态库路径。StarRocks 会拒绝包含路径分隔符的 `driver` 值。

### 在 FE 中为 Arrow 开放 Java NIO

使用 Java 9 或更高版本时，请在每个 FE 节点的 **fe.conf** 中将以下参数添加到 `JAVA_OPTS`，然后重启所有 FE：

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

例如，保留 FE 已有参数并追加此参数：

```properties
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED <existing_FE_JAVA_OPTS>"
```

FE 使用 Arrow Java 读取 ADBC 元数据，因此需要此参数。BE 或 CN 上的 C++ ADBC Scanner 不需要此 Java 参数。

标准 FE 安装包会将 `adbc_jni_library_path` 设置为 `${STARROCKS_HOME}/lib/adbc_driver_jni`，并将 StarRocks 构建的 JNI 桥接库安装到此目录。仅当您移动了安装包中的桥接库时才需要修改此 FE 配置。不要将其设置为 Flight SQL 驱动程序或其他数据源驱动程序的路径。

## 创建 ADBC Catalog

### 语法

```sql
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### 参数说明

#### `catalog_name`

ADBC Catalog 的名称。命名规则请参见[系统限制](../../sql-reference/System_limit.md)。

#### `comment`

Catalog 的可选描述。

#### `PROPERTIES`

| 参数 | 是否必填 | 说明 |
|------|----------|------|
| `type` | 是 | 固定设置为 `adbc`。 |
| `driver` | 是 | 由 ADBC Driver Manager 解析的逻辑驱动程序名称，例如 `adbc_driver_flightsql`。不要指定文件路径。 |
| `uri` | 是 | 驱动程序支持的数据源 URI，例如 `grpc+tls://flight.example.com:31337`。 |
| `username` | 否 | 传递给 ADBC 驱动程序的用户名。 |
| `password` | 否 | 传递给 ADBC 驱动程序的密码。 |
| `adbc.*` | 否 | ADBC 驱动程序专用属性。完整的 Key 和 Value 会在数据库初始化前传递给驱动程序。 |

除上述顶层属性外，其他属性名称必须以 `adbc.` 开头。

### Flight SQL 示例

以下示例为一个 Flight SQL Endpoint 创建 Catalog，其原生驱动程序已注册为 `adbc_driver_flightsql`：

```sql
CREATE EXTERNAL CATALOG flight_sql
COMMENT "Flight SQL service"
PROPERTIES
(
    "type" = "adbc",
    "driver" = "adbc_driver_flightsql",
    "uri" = "grpc+tls://flight.example.com:31337",
    "username" = "flight_user",
    "password" = "change_me",
    "adbc.flight.sql.rpc.timeout_seconds.connect" = "10"
);
```

请根据 Flight SQL 服务配置所需的 TLS 和认证属性。例如，Flight SQL 驱动程序接受 `adbc.flight.sql.client_option.*` 和 `adbc.flight.sql.rpc.*` 下的属性。

## 查询 ADBC Catalog

列出映射为 StarRocks 数据库的 Schema：

```sql
SHOW DATABASES FROM flight_sql;
```

列出 Schema 中的表：

```sql
SHOW TABLES FROM flight_sql.main;
```

使用完整名称查询表：

```sql
SELECT n_nationkey, n_name
FROM flight_sql.main.nation
WHERE n_nationkey = 24;
```

您也可以先执行 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 和 [USE](../../sql-reference/sql-statements/Database/USE.md)，然后再查询表。

StarRocks 会将列裁剪、支持的谓词和 `LIMIT` 下推到通过 ADBC 驱动程序执行的 SQL 中。

## 查看和删除 ADBC Catalog

查看所有 Catalog 或特定 Catalog 的创建语句：

```sql
SHOW CATALOGS;
SHOW CREATE CATALOG flight_sql;
```

删除 Catalog：

```sql
DROP CATALOG flight_sql;
```

删除 StarRocks Catalog 不会删除外部 ADBC 驱动程序，也不会修改远端数据源。

## 问题排查

### 无法加载驱动程序

如果报错中包含 `Could not load` 或 `dlopen() failed`，请在每个 FE 和 BE 或 CN 节点上检查以下事项：

- Manifest 文件名与 `<driver>.toml` 一致。
- Manifest 位于默认 ADBC 搜索目录或 `ADBC_DRIVER_PATH` 所列目录中。
- Manifest 中的动态库路径存在，并且运行 StarRocks 的系统用户有读取权限。
- 动态库与节点操作系统和 CPU 架构匹配，并且能够加载其所有原生依赖。

### 找不到 StarRocks JNI 桥接库

如果 FE 报告找不到 StarRocks 构建的 JNI 桥接库，请确认标准安装包包含 `lib/adbc_driver_jni/libadbc_driver_jni.so`，并确认 `adbc_jni_library_path` 指向该文件所在目录。此配置用于 JNI 桥接库，而不是外部 ADBC 数据源驱动程序。

### Arrow 无法访问 Java NIO

如果访问 Catalog 元数据时出现 Java 模块访问或 `java.nio` 反射错误，请确认每个 FE 的 `JAVA_OPTS` 中都包含 `--add-opens=java.base/java.nio=ALL-UNNAMED`，并且所有 FE 均已重启。
