---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# JDBC catalog

StarRocks supports JDBC catalogs from v3.0 onwards.

A JDBC catalog is a kind of external catalog that enables you to query data from data sources accessed through JDBC without ingestion.

Also, you can directly transform and load data from JDBC data sources by using [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) based on JDBC catalogs.

JDBC catalogs support MySQL and PostgreSQL from v3.0 onwards, and Oracle and SQLServer since v3.2.9 and v3.3.1.

## Prerequisites

- The FEs and BEs or CNs in your StarRocks cluster can download the JDBC driver from the download URL specified by the `driver_url` parameter.
- `JAVA_HOME` in the **$BE_HOME/bin/start_be.sh** file on each BE or CN node is properly configured as a path in the JDK environment instead of a path in the JRE environment. For example, you can configure `export JAVA_HOME = <JDK_absolute_path>`. You must add this configuration at the beginning of the script and restart the BE or CN for the configuration to take effect.

## Create a JDBC catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### Parameters

#### `catalog_name`

The name of the JDBC catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### `comment`

The description of the JDBC catalog. This parameter is optional.

#### `PROPERTIES`

The properties of the JDBC Catalog. `PROPERTIES` must include the following parameters:

| **Parameter**     | **Description**                                                     |
| ----------------- | ------------------------------------------------------------ |
| type              | The type of the resource. Set the value to `jdbc`.           |
| user              | The username that is used to connect to the target database. |
| password          | The password that is used to connect to the target database. |
| jdbc_uri          | The URI that the JDBC driver uses to connect to the target database. For MySQL, the URI is in the `"jdbc:mysql://ip:port"` format. For PostgreSQL, the URI is in the `"jdbc:postgresql://ip:port/db_name"` format. For more information: [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html). |
| driver_url        | The download URL of the JDBC driver JAR package. An HTTP URL or file URL is supported, for example, `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` and `file:///home/disk1/postgresql-42.3.3.jar`.<br />**NOTE**<br />You can also put the JDBC driver to any same path on the FE and BE or CN nodes and set `driver_url` to that path, which must be in the `file:///<path>/to/the/driver` format. |
| driver_class      | The class name of the JDBC driver. The JDBC driver class names of common database engines are as follows:<ul><li>MySQL: `com.mysql.jdbc.Driver` (MySQL v5.x and earlier) and `com.mysql.cj.jdbc.Driver` (MySQL v6.x and later)</li><li>PostgreSQL: `org.postgresql.Driver`</li></ul> |

> **NOTE**
>
> The FEs download the JDBC driver JAR package at the time of JDBC catalog creation, and the BEs or CNs download the JDBC driver JAR package at the time of the first query. The amount of time taken for the download varies depending on network conditions.

### Examples

The following example creates two JDBC catalogs: `jdbc0` and `jdbc1`.

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
       
```

## View JDBC catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a JDBC catalog named `jdbc0`:

```SQL
SHOW CREATE CATALOG jdbc0;
```

## Drop a JDBC catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop a JDBC catalog.

The following example drops a JDBC catalog named `jdbc0`:

```SQL
DROP Catalog jdbc0;
```

## Query a table in a JDBC catalog

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the databases in your JDBC-compatible cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) to switch to the destination catalog in the current session:

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    Then, use [USE](../../sql-reference/sql-statements/Database/USE.md) to specify the active database in the current session:

    ```SQL
    USE <db_name>;
    ```

    Or, you can use [USE](../../sql-reference/sql-statements/Database/USE.md) to directly specify the active database in the destination catalog:

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT * FROM <table_name>;
   ```

## FAQ

What do I do if an error suggesting "Malformed database URL, failed to parse the main URL sections" is thrown?

If you encounter such an error, the URI that you passed in `jdbc_uri` is invalid. Check the URI that you pass and make sure it is valid. For more information, see the parameter descriptions in the "[PROPERTIES](#properties)" section of this topic.
