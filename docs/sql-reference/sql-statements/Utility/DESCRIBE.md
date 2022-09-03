# DESC

## Description

View the schema of a table stored in your StarRocks cluster or external data sources, such as Apache Hive™, Apache Iceberg, or Apache Hudi. Note that you can only use this statement to view the schema of a table in external data sources in StarRocks 2.4 and later versions.

## Syntax

```sql
DESC[RIBE] [db_name.]table_name [ALL];
```

> Note：If ALL is specified, the schema of all indexes(rollup) of the table is displayed

## Examples

Example 1: Show the schema of a base table.

```sql
DESC table_name;
```

Example 2: Show the indexes in a table.

```sql
DESC db1.table_name ALL;
```
