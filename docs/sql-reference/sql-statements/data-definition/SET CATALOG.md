# SET CATALOG

Sets the current catalog. After the current catalog is set, partially and unqualified identifiers for tables, functions, and views that are referenced by SQL statements are resolved from the current catalog.

Setting the catalog also resets the current schema to `default`.

## Syntax

```SQL
SET CATALOG [ catalog_name | 'catalog_name' ]
```

## Parameter

`catalog_name`: the name of the catalog to use. If the catalog does not exist, an exception is thrown.

## Examples

Run one of the following commands to set a Hive catalog named `hive_metastore` as the current catalog:

```SQL
SET CATALOG hive_metastore;
```

Or

```SQL
SET CATALOG 'hive_metastore';
```
