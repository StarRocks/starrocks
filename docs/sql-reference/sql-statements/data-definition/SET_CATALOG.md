# SET CATALOG

Switches to a specified catalog in the current session.

This command is supported from v3.0 onwards.

> **NOTE**
>
<<<<<<< HEAD
> For a newly deployed cluster, you must have the USAGE privilege on the destination external catalog if you want to run [SET CATALOG](../sql-reference/sql-statements/data-definition/SET_CATALOG.md) to switch to that catalog. You can use [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the required privileges. For a cluster upgraded from an earlier version, you can run SET CATALOG with the inherited privilege.
=======
> For a newly deployed StarRocks v3.1 cluster, you must have the USAGE privilege on the destination external catalog if you want to run SET CATALOG to switch to that catalog. You can use [GRANT](../account-management/GRANT.md) to grant the required privileges. For a v3.1 cluster upgraded from an earlier version, you can run SET CATALOG with the inherited privilege.
>>>>>>> 273c0ad72f ([Doc] fix broken links (#32532))

## Syntax

```SQL
SET CATALOG <catalog_name>
```

## Parameter

`catalog_name`: the name of the catalog to use in the current session. You can switch to an internal or external catalog. If the catalog that you specify does not exist, an exception is thrown.

## Examples

Run the following command to switch to a Hive catalog named `hive_metastore` in the current session:

```SQL
SET CATALOG hive_metastore;
```

Run the following command to switch to the internal catalog `default_catalog` in the current session:

```SQL
SET CATALOG default_catalog;
```
