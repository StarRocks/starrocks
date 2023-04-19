# [Preview] JDBC catalog

A JDBC catalog is a kind of external catalog that enables you to query data from data sources accessed through JDBC without ingestion.

Also, you can directly transform and load data from JDBC by using [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/insert.md) based on JDBC catalogs. StarRocks supports JDBC catalogs from v3.0 onwards.

JDBC catalogs currently support MySQL and PostgreSQL.

## Prerequisites

The FEs and BEs in your StarRocks cluster can download the JDBC driver from the download URL specified by the `driver_url` parameter.

## Create a JDBC catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

For more information, see [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md).

### Parameters

#### `catalog_name`

The name of the JDBC catalog. The naming conventions are as follows:

- The name can contain letters, digits 0 through 9, and underscores (_) and must start with a letter.
- The name cannot exceed 64 characters in length.

#### `comment`

The description of the JDBC catalog. This parameter is optional.

#### `PROPERTIES`

The properties of the JDBC Catalog. `PROPERTIES` must include the following parameters:

| **Parameter**     | **Description**                                                     |
| ----------------- | ------------------------------------------------------------ |
| type              | the type of the resource. Set the value to `jdbc`.           |
| user              | the username that is used to connect to the target database. |
| password          | the password that is used to connect to the target database. |
| jdbc_uri          | the URI that the JDBC driver uses to connect to the target database. For MySQL, the URI is in the `"jdbc:mysql://ip:port"` format. For PostgreSQL, the URI is in the `"jdbc:postgresql://ip:port/db_name"` format. For more information, visit the official websites of [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html) and [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html). |
| driver_url        | the download URL of the JDBC driver JAR package. An HTTP URL or file URL is supported, for example, `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` and `file:///home/disk1/postgresql-42.3.3.jar`.<br>**NOTE**<br>You can also put the JDBC driver to any same path on the FE and BE nodes and set `driver_url` to that path, which must be in the `file://<path>/to/the/dirver` format. |
| driver_class      | the class name of the JDBC driver. The JDBC driver class names of common database engines are as follows:<ul><li>MySQL: com.mysql.jdbc.Driver (MySQL v5.x and earlier) and com.mysql.cj.jdbc.Driver (MySQL v6.x and later)</li><li>PostgreSQL: org.postgresql.Driver</li></ul> |

> **NOTE**
>
> The FEs download the JDBC driver JAR package at the time of JDBC catalog creation, and the BEs download the JDBC driver JAR package at the time of the first query. The amount of time taken for the download varies depending on network conditions.

### Examples

The following example creates two JDBC catalogs: one named `jdbc0`, and the other named `jdbc1`.

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

## View JDBC catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a JDBC catalog named `jdbc0`:

```SQL
SHOW CREATE CATALOG jdbc0;
```

## Drop a JDBC catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP%20CATALOG.md) to drop a JDBC catalog.

The following example drops a JDBC catalog named `jdbc0`:

```SQL
DROP Catalog jdbc0;
```

## Query a table in a JDBC catalog

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md) to view the databases in your JDBC-compatible cluster.

   ```SQL
   SHOW DATABASES <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET%20CATALOG.md) to switch to the destination catalog in the current session:

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    Then, use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to specify the active database in the current session:

    ```SQL
    USE <db_name>;
    ```

    Or, you can use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to directly specify the active database in the destination catalog:

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. Use [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT * FROM <table_name>;
   ```
