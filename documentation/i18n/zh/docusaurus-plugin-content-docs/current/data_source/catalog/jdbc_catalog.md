# JDBC catalog

StarRocks 从 3.0 版本开始支持 JDBC Catalog。

JDBC Catalog 是一种 External Catalog。通过 JDBC Catalog，您不需要执行数据导入就可以直接查询 JDBC 数据源里的数据。

此外，您还可以基于 JDBC Catalog ，结合 [INSERT INTO](/sql-reference/sql-statements/data-manipulation/insert.md) 能力对 JDBC 数据源的数据实现转换和导入。

目前 JDBC Catalog 支持 MySQL 和 PostgreSQL。

## 前提条件

- 确保 FE 和 BE 可以通过 `driver_url` 指定的下载路径，下载所需的 JDBC 驱动程序。
- BE 所在机器的启动脚本 **$BE_HOME/bin/start_be.sh** 中需要配置 `JAVA_HOME`，要配置成 JDK 环境，不能配置成 JRE 环境，比如 `export JAVA_HOME = <JDK 的绝对路径>`。注意需要将该配置添加在 BE 启动脚本最开头，添加完成后需重启 BE。

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
| jdbc_uri     | JDBC 驱动程序连接目标数据库的 URI。如果使用 MySQL，格式为：`"jdbc:mysql://ip:port"`。如果使用 PostgreSQL，格式为 `"jdbc:postgresql://ip:port/db_name"`。参见 [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html) 和 [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html) 官网文档。 |
| driver_url   | 用于下载 JDBC 驱动程序 JAR 包的 URL。支持使用 HTTP 协议或者 file 协议，例如`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` 和 `file:///home/disk1/postgresql-42.3.3.jar`。<br />**说明**<br />您也可以把 JDBC 驱动程序部署在 FE 或 BE 所在节点上任意相同路径下，然后把 `driver_url` 设置为该路径，格式为 `file:///<path>/to/the/driver`。 |
| driver_class | JDBC 驱动程序的类名称。以下是常见数据库引擎支持的 JDBC 驱动程序类名称：<ul><li>MySQL：`com.mysql.jdbc.Driver`（MySQL 5.x 及之前版本）、`com.mysql.cj.jdbc.Driver`（MySQL 6.x 及之后版本）</li><li>PostgreSQL: `org.postgresql.Driver`</li></ul> |

> **说明**
>
> FE 会在创建 JDBC Catalog 时去获取 JDBC 驱动程序，BE 会在第一次执行查询时去获取驱动程序。获取驱动程序的耗时跟网络条件相关。

### 创建示例

以下示例创建了两个 JDBC Catalog：`jdbc0` 和 `jdbc1`。

```SQL
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
```

## 查看 JDBC Catalog

您可以通过 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md) 查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](/sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md) 查询某个 External Catalog 的创建语句。例如，通过如下命令查询 JDBC Catalog `jdbc0` 的创建语句：

```SQL
SHOW CREATE CATALOG jdbc0;
```

## 删除 JDBC Catalog

您可以通过 [DROP CATALOG](/sql-reference/sql-statements/data-definition/DROP_CATALOG.md) 删除一个 JDBC Catalog。

例如，通过如下命令删除 JDBC Catalog `jdbc0`：

```SQL
DROP Catalog jdbc0;
```

## 查询 JDBC Catalog 中的表数据

1. 通过 [SHOW DATABASES](/sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) 查看指定 Catalog 所属的集群中的数据库：

   ```SQL
   SHOW DATABASES from <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET_CATALOG.md) 切换当前会话生效的 Catalog：

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    再通过 [USE](../../sql-reference/sql-statements/data-definition/USE.md) 指定当前会话生效的数据库：

    ```SQL
    USE <db_name>;
    ```

    或者，也可以通过 [USE](../../sql-reference/sql-statements/data-definition/USE.md) 直接将会话切换到目标 Catalog 下的指定数据库：

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. 通过 [SELECT](/sql-reference/sql-statements/data-manipulation/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT * FROM <table_name>;
   ```

## 常见问题

系统返回 "Malformed database URL, failed to parse the main URL sections" 报错应该如何处理？

该报错通常是由于 `jdbc_uri` 中传入的 URI 有误而引起的。请检查并确保传入的 URI 是否正确无误。参见本文“[PROPERTIES](#properties)”小节相关的参数说明。
