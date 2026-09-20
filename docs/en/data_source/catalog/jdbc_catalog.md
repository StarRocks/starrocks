---
sidebar_position: 70
displayed_sidebar: docs
toc_max_heading_level: 4
description: "StarRocks supports JDBC catalogs from v3.0 onwards."
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'
import JoinPushdown from '../../_assets/commonMarkdown/join_pushdown.mdx'

# JDBC catalog

<Beta />

StarRocks supports JDBC catalogs from v3.0 onwards.

A JDBC catalog is a kind of external catalog that enables you to query data from data sources accessed through JDBC without ingestion.

Also, you can directly transform and load data from JDBC data sources by using [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) based on JDBC catalogs.

JDBC catalogs support MySQL and PostgreSQL from v3.0 onwards, Oracle and SQLServer since v3.2.9 and v3.3.1, and ClickHouse (Experimental) since v3.3.0.

## PostgreSQL unconstrained numeric

PostgreSQL `numeric` and `decimal` columns declared without precision and scale are mapped to StarRocks `DECIMAL(38,18)` (Decimal128), instead of `VARCHAR`. This mapping supports up to 20 integer digits and 18 fractional digits. Values must be exactly representable: overflow, nonzero fractional digits beyond scale 18, `NaN`, and infinities cause a read error. Additional trailing fractional zeros are accepted. NULL remains NULL.

This changes these columns from string semantics to numeric semantics, including sorting, comparisons, grouping, and function resolution. Numeric columns with explicitly declared precision and scale retain their existing mapping. When a scan reads an unconstrained numeric column, predicate, expression, aggregation, and join pushdown are conservatively disabled for that scan so the numeric conversion occurs before evaluation in StarRocks. Columns returned by PostgreSQL views and `native_query` use the same mapping when their metadata reports unconstrained numeric.

Upgrade the FE, BE/CN, and JDBC bridge together before using this mapping. If a value does not fit, expose an explicitly typed PostgreSQL view with a business-appropriate precision and scale, or expose the value as text when preserving its textual representation is required.

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
| driver_class      | The class name of the JDBC driver. The JDBC driver class names of common database engines are as follows:<ul><li>MySQL: `com.mysql.jdbc.Driver` (MySQL v5.x and earlier) and `com.mysql.cj.jdbc.Driver` (MySQL v6.x and later)</li><li>PostgreSQL: `org.postgresql.Driver`</li><li>Oracle: `oracle.jdbc.driver.OracleDriver`</li></ul> |
| schema_resolver   | (Optional) Explicitly specifies the schema resolver to use. Valid values: `postgresql`, `mysql`, `oracle`, `sqlserver`, `clickhouse`. Use this parameter when working with non-standard JDBC drivers that cannot be auto-detected by driver class name. If not specified, StarRocks will auto-detect the appropriate resolver based on the `driver_class` parameter. |

#### Optional Oracle properties

When `driver_class` is set to Oracle, you can configure the following optional properties:

| **Parameter**                  | **Default** | **Description**                                                                                               |
| ------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------- |
| oracle.number.default-scale    | 6           | Set it when Oracle `NUMBER` metadata does not provide explicit precision and scale. Valid range: `0` to `38`. |
| oracle.temporal.to-datetime    | false       | Controls Oracle `DATE`, `TIMESTAMP`, and `TIMESTAMP WITH LOCAL TIME ZONE` mapping. If it is set to `true`, these data types are mapped to StarRocks' `DATETIME` type; otherwise, `DATE` remains `DATE`, and `TIMESTAMP` / `TIMESTAMP WITH LOCAL TIME ZONE` are mapped to `VARCHAR(64)`. |
| oracle.timestamptz.to-datetime | false       | Controls Oracle `TIMESTAMP WITH TIME ZONE` mapping. If it is set to `true`, it is mapped to StarRocks' `DATETIME` type; otherwise, it is mapped to `VARCHAR(64)`. |

#### Optional statistics cache properties

StarRocks caches per-table statistics read from JDBC sources to avoid blocking query planning. One cache entry holds a table's row count together with the per-column statistics the source reports, and both are loaded and expired as a unit. These properties let you tune the cache behavior per catalog. If not set, the global FE configuration values are used. The parameter names predate column statistics and are unchanged.

| **Parameter**                       | **Default** | **Description**                                                                                                                                             |
| ----------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jdbc_row_count_cache_refresh_sec    | 600         | Background refresh interval (seconds). After this interval, the cached entry is returned immediately while a reload runs asynchronously in the background.  |
| jdbc_row_count_cache_expire_sec     | 1200        | Hard eviction TTL (seconds). Cache entries not accessed within this window are evicted. Must be greater than `jdbc_row_count_cache_refresh_sec`.            |
| jdbc_row_count_cache_max_size       | 10000       | Maximum number of table entries in the statistics cache for this catalog.                                                                                  |

> **NOTE**
>
> The FEs download the JDBC driver JAR package at the time of JDBC catalog creation, and the BEs or CNs download the JDBC driver JAR package at the time of the first query. The amount of time taken for the download varies depending on network conditions.

### PostgreSQL string arrays

Using `org.postgresql.Driver`, PostgreSQL `text[]` and `varchar[]` columns map to `ARRAY<VARCHAR>`. One-dimensional values preserve element order, UTF-8 strings, NULL arrays, empty arrays, and NULL elements. Upgrade the FE, BE/CN, JDBC bridge, and packaged JDBC type mappings together before querying these columns.

StarRocks arrays use positions starting at 1. PostgreSQL lower bounds are not retained: for example, PostgreSQL `[0:1]={a,b}` becomes `["a","b"]`, so `items[1]` returns `a` in StarRocks. A constant subscript over such a column -- `items[1]`, or the equivalent `element_at(items, 1)` -- is evaluated by PostgreSQL and only the element is returned, instead of the whole array being read back and indexed locally. The subscript is pushed down as written, assuming a lower bound of 1, which is the lower bound of every array PostgreSQL builds for itself. On a value stored with another lower bound, the pushed-down subscript therefore returns a different element than the same subscript evaluated in StarRocks, silently: on `[0:2]={zero,one,two}`, `items[1]` is `one` remotely and `zero` locally. Set [`enable_jdbc_array_lower_bound_correction`](../../sql-reference/System_variable.md#enable_jdbc_array_lower_bound_correction) to `true` to have the pushed-down subscript corrected for each value's own lower bound, so that it answers exactly what StarRocks answers. Set [`enable_jdbc_array_subscript_push_down`](../../sql-reference/System_variable.md#enable_jdbc_array_subscript_push_down) to `false` to turn the push-down off altogether, on the filter and the projection path alike, and go back to reading the whole array column and taking the subscript in StarRocks. The two variables compose in one direction only: with the push-down off, `enable_jdbc_array_lower_bound_correction` has nothing left to govern, because no subscript reaches PostgreSQL. A variable subscript such as `items[id]`, comparisons, joins, ordering, and grouping or distinct aggregation on whole array values execute in StarRocks. Reading a multidimensional value fails with an explicit unsupported-array error; such values are not flattened. A constant subscript over a column that holds one is the exception: it is pushed down like any other, and PostgreSQL answers NULL for a subscript that does not address every dimension, so the query returns NULL where reading the column would have failed. Neither setting of `enable_jdbc_array_lower_bound_correction` changes that -- PostgreSQL records no dimension on the column (`attndims` is not enforced and a column may hold values of different dimensions row by row), so there is nothing to gate on. Turning `enable_jdbc_array_subscript_push_down` off does change it: the column is read directly again, and the query fails with the unsupported-array error instead of returning NULL. Other PostgreSQL array element types remain unsupported.

### Examples

The following example creates five different JDBC catalogs.

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
-- Using schema_resolver for non-standard driver
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

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT * FROM <table_name>;
   ```

<JoinPushdown />

### PostgreSQL date and timestamp values

PostgreSQL `date` and `timestamp without time zone` values retain their calendar fields when read through a JDBC catalog. The JVM's default time zone does not shift these values, and timestamps retain microsecond precision. The supported year range is 0001 through 9999 AD. Reading a BC date, `infinity`, `-infinity`, or a value outside this range fails with an error instead of changing its era or value.

## Query JDBC data with native SQL

From v4.1 onwards, StarRocks supports querying JDBC data with database-native `SELECT` statements by using the [`native_query`](../../sql-reference/sql-functions/table-functions/native_query.md) table function.

`native_query` is useful when the source database must run SQL that cannot be expressed as a single external table query, such as source-side joins, pre-filtered subqueries, or vendor-specific SQL syntax. StarRocks exposes the pass-through query result as a normal relation, so you can continue to apply StarRocks-side filters, joins, aggregations, and projections.

For syntax, limitations, and examples, see [`native_query`](../../sql-reference/sql-functions/table-functions/native_query.md).

## FAQ

What do I do if an error suggesting "Malformed database URL, failed to parse the main URL sections" is thrown?

If you encounter such an error, the URI that you passed in `jdbc_uri` is invalid. Check the URI that you pass and make sure it is valid. For more information, see the parameter descriptions in the "[PROPERTIES](#properties)" section of this topic.
