# CREATE DATABASE

## Description

This statement is used to create a database.

## Syntax

```SQL
CREATE DATABASE [IF NOT EXISTS] <database_name>
PROPERTIES ("location"="<database_path>")
```

## Parameters

| **Parameter**      | **Required** | **Description**                                                              |
| ------------------ | ------------ | ---------------------------------------------------------------------------- |
| database_name      | Yes          | The name of the database.                                                    |
| location           | No           | The location of the database. This parameter is supported from v3.1 onwards, and you can specify this parameter only when you create a database within an Elasticsearch catalog. |

## Examples

### Create a database named `db_test`

```SQL
CREATE DATABASE db_test;
```

### Create a database named `iceberg_db`, with its position specified

```SQL
CREATE DATABASE iceberg_db
PROPERTIES ("location"="hdfs://hadoop01:9000/usr/warehouse/iceberg_db.db/");
```
