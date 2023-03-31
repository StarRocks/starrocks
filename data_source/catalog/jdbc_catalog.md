# 【公测中】JDBC catalog

JDBC Catalog 是一种 External Catalog。通过 JDBC Catalog，您不需要执行数据导入就可以直接查询通过 JDBC 访问的数据源里的数据。

此外，您还可以基于 JDBC Catalog ，结合 [INSERT INTO](/sql-reference/sql-statements/data-manipulation/insert.md) 能力来实现数据转换和导入。StarRocks 从 3.0 版本开始支持 JDBC Catalog。

目前 JDBC Catalog 支持 MySQL 和 PostgreSQL。

## 前提条件

确保 FE 和 BE 可以通过 `driver_url` 指定的下载路径，下载所需的 JDBC 驱动程序。

## 创建 JDBC Catalog

使用 [CREATE EXTERNAL CATALOG](/sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md) 创建 JDBC Catalog。

> **说明**
>
> FE 会在创建 JDBC Catalog 时去获取 JDBC 驱动程序，BE 会在第一次执行查询时去获取驱动程序。获取驱动程序的耗时跟网络条件相关。

以下示例创建了两个 JDBC Catalog：`jdbc0` 和 `jdbc1`。

```SQL
CREATE EXTERNAL CATALOG jdbc0
properties
(
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);

CREATE EXTERNAL CATALOG jdbc1
properties
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:mysql://127.0.0.1:3306",
    "driver_url"="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar",
    "driver_class"="com.mysql.cj.jdbc.Driver"
);
```

`properties` 包含如下必填配置项：

- `type`：资源类型，固定取值为 `jdbc`。

- `user`：目标数据库登录用户名。

- `password`：目标数据库用户登录密码。

- `jdbc_uri`：JDBC 驱动程序连接目标数据库的 URI。如果使用 MySQL，格式为：`"jdbc:mysql://ip:port"`。如果使用 PostgreSQL，格式为 `"jdbc:postgresql://ip:port/db_name"`。参见 [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html) 和 [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html) 官网文档。

  > **说明**
  >
  > 您也可以把 JDBC 驱动程序部署在 FE 或 BE 所在节点上，然后把 `jdbc_uri` 设置为 JDBC 驱动程序安装文件所在的位置。

- `driver_url`：用于下载 JDBC 驱动程序 JAR 包的 URL。支持使用 HTTP 协议或者 file 协议，例如`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` 和 `file:///home/disk1/postgresql-42.3.3.jar`。

  > **说明**
  >
  > 您可以将 JDBC 驱动程序 JAR 包放在 FE 节点和 BE 节点上相同的路径下。

- `driver_class`：JDBC 驱动程序的类名称。

  以下是常见数据库引擎支持的 JDBC 驱动程序类名称：

  - MySQL：**com.mysql.jdbc.Driver**（MySQL 5.x 及之前版本）、**com.mysql.cj.jdbc.Driver**（MySQL 6.x 及之后版本）
  - PostgreSQL: **org.postgresql.Driver**

## 查看 JDBC Catalog

您可以通过 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md) 查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](/sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20CATALOG.md) 查询某个 External Catalog 的创建信息。例如，通过如下命令查询 JDBC Catalog `jdbc0` 的创建信息：

```SQL
SHOW CREATE CATALOG jdbc0;
```

## 删除 JDBC Catalog

您可以通过 [DROP CATALOG](/sql-reference/sql-statements/data-definition/DROP%20CATALOG.md) 删除一个 JDBC Catalog。

例如，通过如下命令删除 JDBC Catalog `jdbc0`：

```SQL
DROP Catalog jdbc0;
```

## 查询 JDBC Catalog 中的表数据

1. 通过 [SHOW DATABASES](/sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md) 查询 JDBC Catalog 中的所有数据库。

   例如，通过如下命令查询 JDBC Catalog `jdbc0` 中的所有数据库：

   ```SQL
   SHOW DATABASES from jdbc0;
   ```

2. 进入 JDBC Catalog 中的目标数据库。

   例如，通过如下命令进入目标数据库 `database0`：

   ```SQL
   USE jdbc0.database0;
   ```

3. 通过 [SELECT](/sql-reference/sql-statements/data-manipulation/SELECT.md) 查询目标数据库中的目标表。

   例如，通过如下命令查询表 `table0` 的数据：

   ```SQL
   SELECT * FROM table0;
   ```
