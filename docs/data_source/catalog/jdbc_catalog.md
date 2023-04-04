# [Preview] JDBC catalog

A JDBC catalog is a kind of external catalog that enables you to query data from data sources accessed through JDBC without ingestion.

Also, you can directly transform and load data from JDBC by using [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/insert.md) based on JDBC catalogs. StarRocks supports JDBC catalogs from v3.0 onwards.

JDBC catalogs currently support MySQL and PostgreSQL.

## Prerequisites

The FEs and BEs in your StarRocks cluster can download the JDBC driver from the download URL specified by the `driver_url` parameter.

## Create a JDBC catalog

You can use [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md) to create a JDBC catalog.

> **NOTE**
>
> The FEs download the JDBC driver JAR package at the time of JDBC catalog creation, and the BEs download the JDBC driver JAR package at the time of the first query. The amount of time taken for the download varies depending on network conditions.

The following example creates two JDBC catalogs: one named `jdbc0`, and the other named `jdbc1`.

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

The required parameters in `properties` are as follows:

- `type`: the type of the resource. Set the value to `jdbc`.

- `user`: the username that is used to connect to the target database.

- `password`: the password that is used to connect to the target database.

- `jdbc_uri`: the URI that the JDBC driver uses to connect to the target database. For MySQL, the URI is in the `"jdbc:mysql://ip:port"` format. For PostgreSQL, the URI is in the `"jdbc:postgresql://ip:port/db_name"` format. For more information, visit the official websites of [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html) and [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html).

  > **NOTE**
  >
  > You can also deploy the JDBC driver on the FE and BE nodes and set the `jdbc_uri` to the installation path of the JDBC driver.

- `driver_url`: the download URL of the JDBC driver JAR package. An HTTP URL or file URL is supported, for example, `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` and `file:///home/disk1/postgresql-42.3.3.jar`.

  > **NOTE**
  >
  > You can place the JDBC driver JAR package in the same path on the FE and BE nodes.

- `driver_class`: the class name of the JDBC driver.

  The JDBC driver class names of common database engines are as follows:

  - MySQL: **com.mysql.jdbc.Driver** (MySQL 5.x and earlier) and **com.mysql.cj.jdbc.Driver** (MySQL 6.x and later)
  - PostgreSQL: **org.postgresql.Driver**

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

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md) to query the databases within the JDBC catalog.

   The following example queries the databases within a JDBC catalog named `jdbc0`:

   ```SQL
   SHOW DATABASES from jdbc0;
   ```

2. Open the target database within the JDBC catalog.

   The following example opens a database named `database0`:

   ```SQL
   USE jdbc0.database0;
   ```

3. Use [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) to query the target table in the target database.

   The following example queries a table named `table0`:

   ```SQL
   SELECT * FROM table0;
   ```
