# SHOW TABLES

## Description

Displays all tables in a StarRocks database or a database in an external data source, for example, Hive, Iceberg, Hudi, or Delta Lake.

> **NOTE**
>
> To view tables in an external data source, you must have the USAGE privilege on the external catalog that corresponds to that data source.

## Syntax

```sql
SHOW TABLES [FROM <catalog_name>.<db_name>]
```

## Parameters

 **Parameter**          | **Required** | **Description**                                                     |
| ----------------- | -------- | ------------------------------------------------------------ |
| catalog_name | No       | The name of the internal catalog or an external catalog.<ul><li>If you do not specify this parameter or set it to `default_catalog`, tables in StarRocks databases are returned.</li><li>If you set this parameter to the name of an external catalog, tables in databases of an external data source are returned.</li></ul> You can run [SHOW CATALOGS](SHOW_CATALOGS.md) to view internal and external catalogs.|
| db_name | No       | The database name. If not specified, the current database is used by default. |

## Examples

Example 1: View tables in database `example_db` of the `default_catalog` after connecting to the StarRocks cluster. The following two statements are equivalent.

```plain
show tables from example_db;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+

show tables from default_catalog.example_db;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+
```

Example 2: View tables in the current database `example_db` after connecting to this database.

```plain
show tables;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+
```

Example 2: View tables in database `hudi_db` of the external catalog `hudi_catalog`.

```plain
show tables from hudi_catalog.hudi_db;
+----------------------------+
| Tables_in_hudi_db          |
+----------------------------+
| hudi_sync_mor              |
| hudi_table1                |
+----------------------------+
```

Alternatively, you can run SET CATALOG to switch to the external catalog `hudi_catalog` and then run `SHOW TABLES FROM hudi_db;`.

## References

- [SHOW CATALOGS](SHOW_CATALOGS.md): Views all catalogs in a StarRocks cluster.
- [SHOW DATABASES](SHOW_DATABASES.md): Views all databases in the internal catalog or an external catalog.
- [SET CATALOG](../data-definition/SET_CATALOG.md): Switches between catalogs.
