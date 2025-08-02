---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Redis catalog

<Beta />

A Redis catalog is a kind of external catalog, and you can query data directly from Redis through redis catalog.

In StarRocks, each Redis key/value pair maps to a row, which is split into cells using table definition JSON files.


## Usage notes
The key definition in Redis must follow the format of `dbName:tblName:xx`. StarRocks will retrieve all keys and their values that match `dbName:tblName:*`, and then return these keys/values in the form of StarRocks table data. For example, if a user sets Redis keys as `testdb:test:aa` and `testdb:test:bb`, these two key/value pairs belong to the same database `testdb` and the `test` table. Users can query the Redis data by executing `select * from testdb.test`.

## Create a Redis catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### Parameters

#### `catalog_name`

The name of the Redis catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### `comment`

The description of the Redis catalog. This parameter is optional.

#### `PROPERTIES`

The properties of the Redis Catalog. `PROPERTIES` must include the following parameters:

| **Parameter**               | **Description**                                                                                                                                                     |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                        | The type of the resource. Set the value to `redis`.                                                                                                                 |
| password                    | The password that is used to connect to the target redis instance.                                                                                                  |
| redis_uri                   | Redis connection URI in the format IP:PORT, e.g., `127.0.0.1:6379`.                                                                                                 |
| redis.table-description-dir | A local directory containing multiple JSON files, which must end with `.json` and contain table definition files. Note that this directory is only used for FE nodes. |


### Create a Redis catalog

The following example creates a Redis catalog: `redis_catalog`.

```SQL
CREATE EXTERNAL CATALOG redis_catalog
PROPERTIES
(
    "type"="redis",
    "password"="passwd",
    "redis_uri"="127.0.0.1:6379",
    "redis.table-description-dir"="/path/redis"
);
```

## View Redis catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a Redis catalog named `redis_catalog`:

```SQL
SHOW CREATE CATALOG redis_catalog;
```

## Drop a Redis catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop a Redis catalog.

The following example drops a Redis catalog named `redis_catalog`:

```SQL
DROP Catalog redis_catalog;
```

## Create table definition json files

Because Redis doesn't have a concept of structured metadata, users need to create JSON files with a `.json` extension in the directory specified by the `redis.table-description-dir` parameter. These files must contain the table's metadata definition information.

```json
{
   "tableName": ...,
   "schemaName": ...,
   "key": {
      "dataFormat": ...,
      "fields": [
         {
            "name":...,
            "type":...
         }
      ]
   },
   "value": {
      "dataFormat": ...,
      "fields": [
         {
            "name":...,
            "type":...
         },
         ...
      ]
   }
}
```

| **Parameter** | **Description**                                                                                                           |
|---------------|---------------------------------------------------------------------------------------------------------------------------|
| tableName     | StarRocks table name defined by this file.                                                                                |
| schemaName    | Schema name which contains the table.                                                                                     |
| key           | A json object used to define redis key.                                                                                   |
| value         | A json object used to define redis value.                                                                                 |
| dataFormat    | Determine to use which row decoder for the key or value.                                                                  |
| fields        | Field definitions used to create new columns in the StarRocks table.                                                      |
| name          | The column names mapped to StarRocks.                                                                                     |
| type          | StarRocks type of the column. Now, only `BIGINT` and `VARCHAR` are supported. More types will be supported in the future. |


Currently, only two Redis data types are supported for Redis values: **String** and **Hash**. Redis keys only support the **String** type.

> **NOTE**
> 
>Here are four different row decoders for users to interpret Redis data based on its data type, enabling conversion into a StarRocks table data representation.
>
>For Redis keys, there is only one `raw` decoder method.
>- `raw`:  The data will not be interpreted. Indicates the original String information. Only be used for Redis key.
>
>For Redis values, there are three decoder methods.
>- `json`: The String-type data is parsed as JSON, the values of JSON serve as StarRocks table data. 
>- `csv`:  The String-type data is parsed into a comma-separated csv format, the values of csv serve as StarRocks table data.
>- `hash`: The hash-type data is parsed hash format, and the values of hash serve as StarRocks table data.

### Examples
Based on the three decoder methods of Redis values, here are three examples. Assume that the directory specified by the parameter `redis.table-description-dir` is `/path/redis`

- `json` row decoder

Write two lines of String type data in JSON format using the Redis CLI client:
 ```redis
SET testdb:testjson:bb "{\"id\":123,\"name\":\"Alice\",\"email\":\"alice@example.com\"}"
SET testdb:testjson:aa "{\"id\":456,\"name\":\"Lily\",\"email\":\"lily@example.com\"}"
```
Create the table definition json file `/path/redis/test_json.json`:
 ```json
{
   "tableName": "testjson",
   "schemaName": "testdb",
   "key": {
       "dataFormat": "raw",
       "fields": [
            {
                "name":"redis_key",
                "type":"VARCHAR"
           }
       ]
   },
   "value": {
       "dataFormat": "json",
       "fields": [
           {
                "name":"id",
                "type":"BIGINT"
           },
           {
                "name":"name",
                "type":"VARCHAR"
           },
           {
                "name":"email",
                "type":"VARCHAR"
           }
      ]
    }
}
```

Query the Redis table `redis_catalog.testdb.testjson` data through StarRocks:
```SQL
mysql> select * from redis_catalog.testdb.testjson;
+--------------------+------+-------+-------------------+
| redis_key          | id   | name  | email             |
+--------------------+------+-------+-------------------+
| testdb:testjson:aa |  456 | Lily  | lily@example.com  |
| testdb:testjson:bb |  123 | Alice | alice@example.com |
+--------------------+------+-------+-------------------+
```

- `csv` row decoder

Write two lines of String type data in CSV format using the Redis CLI client. Note that the CSV format currently only supports comma-separated value.
 ```redis
SET testdb:testcsv:aa  "123,Alice,alice@example.com"
SET testdb:testcsv:bb  "456,Lily,lily@example.com"
```

Create the table definition json file `/path/redis/test_csv.json`:
```json
{
   "tableName": "testcsv",
   "schemaName": "testdb",
   "key": {
       "dataFormat": "raw",
              "fields": [
            {
                "name":"redis_key",
                "type":"VARCHAR"
           }
       ]
   },
   "value": {
       "dataFormat": "csv",
       "fields": [
           {
                "name":"id",
                "type":"BIGINT"
           },

           {
                "name":"name",
                "type":"VARCHAR"
           },
           {
                "name":"email",
                "type":"VARCHAR"
           }
       ]
    }
}
```

Query the Redis table `redis_catalog.testdb.testcsv` data through StarRocks:
```SQL
mysql> select * from redis_catalog.testdb.testcsv;
+-------------------+------+-------+-------------------+
| redis_key         | id   | name  | email             |
+-------------------+------+-------+-------------------+
| testdb:testcsv:aa | 123  | Alice | alice@example.com |
| testdb:testcsv:bb | 456  | Lily  | lily@example.com  |
+-------------------+------+-------+-------------------+
```

- `hash` row decoder
Write two lines of hash type data using the Redis CLI client.
 
```redis
HMSET testdb:testhash:aa id 123  name "Alice" email "alice@example.com"
HMSET testdb:testhash:bb id 456 name "Lily" email "lily@example.com"
```

Create the table definition json file `/path/redis/test_hash.json`:
```json
{
   "tableName": "testhash",
   "schemaName": "testdb",
   "key": {
       "dataFormat": "raw",
              "fields": [
            {
                "name":"redis_key",
                "type":"VARCHAR",
                "hidden":"false"
           }
       ]
   },
   "value": {
       "dataFormat": "hash",
       "fields": [

           {
                "name":"id",
                "type":"BIGINT"
           },

           {
                "name":"name",
                "type":"VARCHAR"
           },

           {
                "name":"email",
                "type":"VARCHAR"
           }
       ]
    }
}
```

Query the Redis table `redis_catalog.testdb.testhash` data through StarRocks:
```SQL
mysql> select * from redis_catalog.testdb.testhash;
+--------------------+------+-------+-------------------+
| redis_key          | id   | name  | email             |
+--------------------+------+-------+-------------------+
| testdb:testhash:aa |  123 | Alice | alice@example.com |
| testdb:testhash:bb |  456 | Lily  | lily@example.com  |
+--------------------+------+-------+-------------------+
```

## Query a table in a Redis catalog

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the databases in your Redis-compatible cluster:

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

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT * FROM <table_name>;
   ```