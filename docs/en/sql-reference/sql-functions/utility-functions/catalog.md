# catalog

## Description

Returns the name of the current catalog. The catalog can be a StarRocks internal catalog or an external catalog that is mapped to an external data source. For more information about catalogs, see [Catalog overview](../../../data_source/catalog/catalog_overview.md).

If no catalog is selected, the StarRocks internal catalog `default_catalog` is returned.

## Syntax

```Haskell
catalog()
```

## Parameters

This function does not require parameters.

## Return value

Returns the name of the current catalog as a string.

## Examples

Example 1: The current catalog is StarRocks internal catalog `default_catalog`.

```plaintext
select catalog();
+-----------------+
| CATALOG()       |
+-----------------+
| default_catalog |
+-----------------+
1 row in set (0.01 sec)
```

Example 2: The current catalog is an external catalog `hudi_catalog`.

```sql
-- Switch to an external catalog.
set catalog hudi_catalog;

-- Return the name of the current catalog.
select catalog();
+--------------+
| CATALOG()    |
+--------------+
| hudi_catalog |
+--------------+
```

## See also

[SET CATALOG](../../sql-statements/data-definition/SET_CATALOG.md): Switches to a destination catalog.
