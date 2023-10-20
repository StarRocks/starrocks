# ALTER RESOURCE

## Description

You can use the ALTER RESOURCE statement to modify the properties of a resource.

## Syntax

```SQL
ALTER RESOURCE 'resource_name' SET PROPERTIES ("key"="value", ...)
```

## Parameters

- `resource_name`: the name of the resource to be modified.

- `PROPERTIES ("key"="value", ...)`: the properties of the resource. You can modify different properties based on resource types. Currently, StarRocks supports modifying the URI of the Hive metastore of the following resources.
  - Apache Iceberg resources support modifying the following properties:
    - `iceberg.catalog-impl`: the fully qualified class name of [custom catalog](../../../data_source/External_table.md#apache-iceberg-external-table).
    - `iceberg.catalog.hive.metastore.uris`: the URI of the Hive metastore.
  - Apache Hive™ resources and Apache Hudi resources support modifying `hive.metastore.uris`, which indicates the URI of the Hive metastore.

## Usage notes

After you reference a resource to create an external table, if you modify the URI of the Hive metastore of this resource, the external table becomes unavailable. If you still want to use the external table to query data, make sure that the new metastore contains a table whose name and schema are the same as that in the original metastore.

## Examples

Modify the URI of the Hive metastore of Hive resource `hive0`.

```SQL
ALTER RESOURCE 'hive0' SET PROPERTIES ("hive.metastore.uris" = "thrift://10.10.44.91:9083")
```
