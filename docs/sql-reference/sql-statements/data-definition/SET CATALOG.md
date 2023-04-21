# SET CATALOG

Switches to a specified catalog in the current session.

This command is supported from v3.0 onwards.

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
