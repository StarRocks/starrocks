---
sidebar_position: 70
displayed_sidebar: docs
description: "StarRocks 从 v3.0 起支持 JDBC catalog，无导入直接查询 JDBC 数据源及执行转换导入。"
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'
import JoinPushdown from '../../_assets/commonMarkdown/join_pushdown.mdx'

# JDBC catalog

<Beta />

StarRocks 从 3.0 版本开始支持 JDBC Catalog。

JDBC Catalog 是一种 External Catalog。通过 JDBC Catalog，您不需要执行数据导入就可以直接查询 JDBC 数据源里的数据。

此外，您还可以基于 JDBC Catalog ，结合 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 能力对 JDBC 数据源的数据实现转换和导入。

JDBC Catalog 自 3.0 版本开始支持 MySQL、PostgreSQL，自 3.2.9、3.3.1 版本开始支持 Oracle 和 SQLServer。自 3.3.0 开始支持 ClickHouse（试验性）。

## PostgreSQL 无精度 numeric

PostgreSQL 中未声明精度和小数位数的 `numeric`、`decimal` 列映射为 StarRocks `DECIMAL(38,18)`（Decimal128），不再映射为 `VARCHAR`。该映射最多容纳 20 位整数和 18 位小数。每个值必须能够无损表示：整数溢出、小数第 18 位之后存在非零数字、`NaN` 和无穷值均导致读取报错。多余的小数尾零可以接受，NULL 仍为 NULL。

该变更使这些列的排序、比较、分组及函数解析采用数值语义。明确声明精度和小数位数的 numeric 列保持原有映射。扫描读取无精度 numeric 列时，保守地禁用该扫描的谓词、表达式、聚合和连接下推，先完成读取转换，再由 StarRocks 计算。PostgreSQL 视图和 `native_query` 返回列的元数据如果表示无精度 numeric，也采用此映射。

使用此映射前，应同步升级 FE、BE/CN 和 JDBC bridge。值超出范围时，可以在 PostgreSQL 视图中显式转换成符合业务范围的精度和小数位数；如果需要保留文本表示，可以让视图返回文本。

## 前提条件

- 确保 FE 和 BE（或 CN）可以通过 `driver_url` 指定的下载路径，下载所需的 JDBC 驱动程序。
- BE（或 CN）所在机器的启动脚本 **$BE_HOME/bin/start_be.sh** 中需要配置 `JAVA_HOME`，要配置成 JDK 环境，不能配置成 JRE 环境，比如 `export JAVA_HOME = <JDK 的绝对路径>`。注意需要将该配置添加在 BE（或 CN）启动脚本最开头，添加完成后需重启 BE（或 CN）。

## 创建 JDBC Catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### 参数说明

#### `catalog_name`

JDBC Catalog 的名称。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### `comment`

JDBC Catalog 的描述。此参数为可选。

#### PROPERTIES

JDBC Catalog 的属性，包含如下必填配置项：

| **参数**     | **说明**                                                     |
| ------------ | ------------------------------------------------------------ |
| type         | 资源类型，固定取值为 `jdbc`。                                |
| user         | 目标数据库登录用户名。                                       |
| password     | 目标数据库用户登录密码。                                     |
| jdbc_uri     | JDBC 驱动程序连接目标数据库的 URI。如果使用 MySQL，格式为：`"jdbc:mysql://ip:port"`。如果使用 PostgreSQL，格式为 `"jdbc:postgresql://ip:port/db_name"`。 |
| driver_url   | 用于下载 JDBC 驱动程序 JAR 包的 URL。支持使用 HTTP 协议或者 file 协议，例如`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` 和 `file:///home/disk1/postgresql-42.3.3.jar`。<br />**说明**<br />您也可以把 JDBC 驱动程序部署在 FE 或 BE（或 CN）所在节点上任意相同路径下，然后把 `driver_url` 设置为该路径，格式为 `file:///<path>/to/the/driver`。 |
| driver_class | JDBC 驱动程序的类名称。以下是常见数据库引擎支持的 JDBC 驱动程序类名称：<ul><li>MySQL：`com.mysql.jdbc.Driver`（MySQL 5.x 及之前版本）、`com.mysql.cj.jdbc.Driver`（MySQL 6.x 及之后版本）</li><li>PostgreSQL: `org.postgresql.Driver`</li><li>Oracle: `oracle.jdbc.driver.OracleDriver`</li></ul> |
| schema_resolver | （可选）显式指定要使用的 Schema Resolver。有效值：`postgresql`、`mysql`、`oracle`、`sqlserver`、`clickhouse`。当使用非标准 JDBC 驱动程序且无法通过驱动类名自动检测时，请使用此参数。如果未指定，StarRocks 将根据 `driver_class` 参数自动检测相应的 Resolver。 |

#### 可选 Oracle 属性

当 `driver_class` 设置为 Oracle 时，您可以配置以下可选属性：

| **参数**                        | **默认**    | **描述**                                                                                                       |
| ------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------- |
| oracle.number.default-scale    | 6           | 当 Oracle `NUMBER` 元数据未明确指定精度和规模时，请设置此参数。有效范围：`0` 至 `38`。                                  |
| oracle.temporal.to-datetime    | false       | 控制 Oracle `DATE`、`TIMESTAMP` 和 `TIMESTAMP WITH LOCAL TIME ZONE` 的映射。如果设置为 `true`，这些数据类型将映射到 StarRocks 的 `DATETIME` 类型；否则，`DATE` 保持为 `DATE`，而 `TIMESTAMP` / `TIMESTAMP WITH LOCAL TIME ZONE` 将映射到 `VARCHAR(64)`。 |
| oracle.timestamptz.to-datetime | false       | 控制 Oracle `TIMESTAMP WITH TIME ZONE` 的映射。如果设置为 `true`，则映射为 StarRocks 的 `DATETIME` 类型；否则，则映射为 `VARCHAR(64)`。 |

#### 可选统计信息缓存属性

StarRocks 会缓存从 JDBC 数据源读取的每张表的统计信息，以避免在查询规划阶段产生阻塞。一个缓存条目同时保存该表的行数与数据源提供的列统计信息，二者作为一个整体一并加载和淘汰。这些属性允许您按 Catalog 调整缓存行为。若未设置，则使用全局 FE 配置值。参数名沿用引入列统计信息之前的命名，保持不变。

| **参数**                             | **默认** | **描述**                                                                                                               |
| ----------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------- |
| jdbc_row_count_cache_refresh_sec    | 600      | 后台刷新间隔（秒）。超过此间隔后，立即返回缓存的旧条目，同时在后台异步重新加载。                                           |
| jdbc_row_count_cache_expire_sec     | 1200     | 强制淘汰 TTL（秒）。在此时间窗口内未被访问的缓存条目将被淘汰。必须大于 `jdbc_row_count_cache_refresh_sec`。               |
| jdbc_row_count_cache_max_size       | 10000    | 该 Catalog 统计信息缓存的最大表条目数。                                                                                  |

> **说明**
>
> FE 会在创建 JDBC Catalog 时去获取 JDBC 驱动程序，BE（或 CN）会在第一次执行查询时去获取驱动程序。获取驱动程序的耗时跟网络条件相关。

### PostgreSQL 数组

使用 `org.postgresql.Driver` 时，以下 PostgreSQL 数组列可以读取：

| PostgreSQL 列类型 | StarRocks 类型 |
| --- | --- |
| `boolean[]` | `ARRAY<BOOLEAN>` |
| `smallint[]` | `ARRAY<SMALLINT>` |
| `integer[]` | `ARRAY<INT>` |
| `bigint[]` | `ARRAY<BIGINT>` |
| `real[]` | `ARRAY<FLOAT>` |
| `double precision[]` | `ARRAY<DOUBLE>` |
| `date[]` | `ARRAY<DATE>` |
| `timestamp[]`（不带时区） | `ARRAY<DATETIME>` |
| `text[]`、`varchar[]`、`char(n)[]` | `ARRAY<VARCHAR>` |

一维数组保留元素顺序、UTF-8 字符串、NULL 数组、空数组以及 NULL 元素。`char(n)[]` 映射为 `ARRAY<VARCHAR>` 而非 `ARRAY<CHAR>`，元素保留 PostgreSQL 存储的空格补齐。`date[]` 和 `timestamp[]` 的元素按墙钟值读取，不受会话时区影响；元素若超出 `0001-01-01` 到 `9999-12-31` 的范围（公元前日期或 `infinity`），查询会报错，而不是读成别的值。查询这些列前，需要一起升级 FE、BE/CN、JDBC Bridge 和安装包中的 JDBC 类型映射。

其余数组元素类型都映射为不支持的类型，该列无法被查询，包括 `numeric[]`、`uuid[]`、`time[]`、`bytea[]`、`json[]` 和 `jsonb[]`，也包括 `timestamptz[]`：把 `timestamp with time zone` 读成 `ARRAY<DATETIME>` 会让夏令时回拨的两个时刻塌成同一个墙钟，后续再也无法区分。

StarRocks 数组的位置从 1 开始，不保留 PostgreSQL 的数组下界。例如 PostgreSQL 的 `[0:1]={a,b}` 读取后为 `["a","b"]`，因此在 StarRocks 中 `items[1]` 返回 `a`。这类列上的常量下标（`items[1]`，以及等价的 `element_at(items, 1)`）由 PostgreSQL 计算，只返回该元素，不再把整列数组读回本地再取下标。下标按原样下推，即假定数组下界为 1——PostgreSQL 自行构造的数组下界都是 1。因此，对以其他下界存储的值，下推后的下标会静默地返回与在 StarRocks 本地计算不同的元素：对 `[0:2]={zero,one,two}`，远端 `items[1]` 是 `one`，本地是 `zero`。如需下推结果与本地计算完全一致，请将 [`enable_jdbc_array_lower_bound_correction`](../../sql-reference/System_variable.md#enable_jdbc_array_lower_bound_correction) 设置为 `true`，此时下推的下标会按每个值自身的下界做校正。将 [`enable_jdbc_array_subscript_push_down`](../../sql-reference/System_variable.md#enable_jdbc_array_subscript_push_down) 设置为 `false` 则完全关闭该下推，过滤和投影两条路径一同关闭，回到读回整列数组、在 StarRocks 本地取下标的行为。两个变量只在一个方向上叠加：关闭下推后，`enable_jdbc_array_lower_bound_correction` 不再有任何作用，因为没有下标会发往 PostgreSQL。变量下标（如 `items[id]`）、数组整体的比较、连接、排序以及分组或去重聚合，仍在 StarRocks 中执行。读取多维数组值会返回明确的不支持错误，不会将其展开为一维。但列中存有多维值时，其上的常量下标仍会照常下推：PostgreSQL 对未指明全部维度的下标返回 NULL，因此查询会得到 NULL，而直接读取该列则会报错。`enable_jdbc_array_lower_bound_correction` 的两种取值都不改变这一点——PostgreSQL 不在列上记录维度（`attndims` 不做强制，同一列可以逐行存放不同维度的值），因此没有可供判断的依据。而关闭 `enable_jdbc_array_subscript_push_down` 会改变这一点：该列重新被直接读取，查询会返回不支持的报错，而不是返回 NULL。

### 创建示例

以下示例创建了五个不同的 JDBC Catalog。

```SQL
-- PostgresSQL
CREATE EXTERNAL CATALOG jdbc0
PROPERTIES
(
    "type"="jdbc", 
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
-- MySQL
CREATE EXTERNAL CATALOG jdbc1
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:mysql://127.0.0.1:3306",
    "driver_url"="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar",
    "driver_class"="com.mysql.cj.jdbc.Driver"
);
-- Oracle
CREATE EXTERNAL CATALOG jdbc2
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:oracle:thin:@127.0.0.1:1521:ORCL",
    "driver_url"="https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc10/19.18.0.0/ojdbc10-19.18.0.0.jar",
    "driver_class"="oracle.jdbc.driver.OracleDriver"
);
-- Oracle (with Oracle-specific optional properties)
CREATE EXTERNAL CATALOG jdbc2_ext
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:oracle:thin:@127.0.0.1:1521/ORCLPDB1",
    "driver_url"="https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc10/19.18.0.0/ojdbc10-19.18.0.0.jar",
    "driver_class"="oracle.jdbc.driver.OracleDriver",
    "oracle.number.default-scale"="6",
    "oracle.temporal.to-datetime"="true",
    "oracle.timestamptz.to-datetime"="true"
);
-- SQL Server
CREATE EXTERNAL CATALOG jdbc3
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:sqlserver://127.0.0.1:1433;databaseName=MyDatabase;",
    "driver_url"="https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar",
    "driver_class"="com.microsoft.sqlserver.jdbc.SQLServerDriver"
);
-- ClickHouse
CREATE EXTERNAL CATALOG jdbc4
PROPERTIES
(
    "type"="jdbc",
    "user"="default",
    "jdbc_uri"="jdbc:clickhouse://127.0.0.1:8443",
    "driver_url"="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6.jar",
    "driver_class"="com.clickhouse.jdbc.ClickHouseDriver"
);
-- 使用 schema_resolver 处理非标准驱动
CREATE EXTERNAL CATALOG jdbc5
PROPERTIES
(
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/mydb",
    "driver_url"="file:///path/to/custom-postgresql-driver.jar",
    "driver_class"="com.custom.PostgresDriver",
    "schema_resolver"="postgresql"
);
```

## 查看 JDBC Catalog

您可以通过 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询某个 External Catalog 的创建语句。例如，通过如下命令查询 JDBC Catalog `jdbc0` 的创建语句：

```SQL
SHOW CREATE CATALOG jdbc0;
```

## 删除 JDBC Catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除一个 JDBC Catalog。

例如，通过如下命令删除 JDBC Catalog `jdbc0`：

```SQL
DROP Catalog jdbc0;
```

## 查询 JDBC Catalog 中的表数据

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查看指定 Catalog 所属的集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 切换当前会话生效的 Catalog：

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    再通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话生效的数据库：

    ```SQL
    USE <db_name>;
    ```

    或者，也可以通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接将会话切换到目标 Catalog 下的指定数据库：

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. 通过 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT * FROM <table_name>;
   ```

<JoinPushdown />

### PostgreSQL 日期和时间戳

通过 JDBC Catalog 读取 PostgreSQL 的 `date` 和 `timestamp without time zone` 时，会保留原始年月日和时间字段，不会因 JVM 默认时区而发生偏移；时间戳保留微秒精度。支持的年份范围为公元 0001 至 9999 年。读取到公元前日期、`infinity`、`-infinity` 或超出该范围的值时会报错，避免静默改变年代或数值。

## 使用原生 SQL 查询 JDBC 数据

自 v4.1 起，StarRocks 支持通过 [`native_query`](../../sql-reference/sql-functions/table-functions/native_query.md) 表函数，使用数据库原生 `SELECT` 语句查询 JDBC 数据。

当源数据库需要执行无法通过单张外部表查询表达的 SQL 时，例如源端 Join、预先过滤的子查询或特定数据库方言的 SQL 语法，可以使用 `native_query`。StarRocks 会将透传查询结果作为普通关系暴露出来，您可以继续在 StarRocks 侧执行过滤、Join、聚合和投影。

有关语法、限制和示例，参见 [`native_query`](../../sql-reference/sql-functions/table-functions/native_query.md)。

## 常见问题

系统返回 "Malformed database URL, failed to parse the main URL sections" 报错应该如何处理？

该报错通常是由于 `jdbc_uri` 中传入的 URI 有误而引起的。请检查并确保传入的 URI 是否正确无误。参见本文“[PROPERTIES](#properties)”小节相关的参数说明。
