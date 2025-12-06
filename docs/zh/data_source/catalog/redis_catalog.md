---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Redis catalog

<Beta />

Redis catalog是一种External Catalog，您可以通过该目录直接从 Redis 中查询数据。

在 StarRocks 中，每个 Redis 的键/值对都对应一条记录，这些记录会通过表定义的 JSON 文件被拆分成单元格。

## 使用说明
Redis 中的关键定义必须遵循`dbName：tblName：xx`的格式。StarRocks 会检索所有与`dbName：tblName：*`相匹配的Redis键及其值，然后以 StarRocks 表数据的形式返回这些键/值。例如，如果用户将 Redis 键设置为`testdb:test:aa`和`testdb:test:bb`，这两个键/值对属于同一个数据库`testdb`和`test`表。用户可以通过执行`select * from testdb.test`来查询 Redis 数据。

## 创建Redis catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### 参数说明

#### `catalog_name`

Redis Catalog 的名称。命名要求如下

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- Catalog 名称大小写敏感并且总长度不能超过 1023 个字符。

#### `comment`

Redis Catalog 的描述。此参数为可选。

#### `PROPERTIES`

Redis Catalog 的属性，包含如下必填配置项：

| **参数**                      | **说明**                                                                 |
|-----------------------------|------------------------------------------------------------------------|
| type                        | 资源类型，固定取值为 redis。                                                      |
| password                    | 目标Redis实例登录密码。                                                         |
| redis_uri                   | “IP:PORT”格式的 Redis 连接 URI 。如`127.0.0.1:6379`.                          |
| redis.table-description-dir | 一个包含多个 JSON 文件的本地目录，这些 JSON 文件必须以`.json`结尾，并且包含表定义文件。请注意，此目录仅用于FE节点使用。 |


### Create a Redis catalog

以下示例创建了一个名为`redis_catalog`的 Redis catalog。

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

## 查看 Redis Catalog

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询某个 External Catalog 的创建语句。例如，通过如下命令查询 Redis Catalog `redis_catalog` 的创建语句：

```SQL
SHOW CREATE CATALOG redis_catalog;
```

## 删除 Redis catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除一个 Redis Catalog。

例如，通过如下命令删除 Redis Catalog `redis_catalog`：

```SQL
DROP Catalog redis_catalog;
```

## 创建表定义json文件

因为Redis没有结构化的元数据概念，所以用户需要在参数`redis.table-description-dir`指定的目录下创建以`.json`结尾的json文件，这些文件需包含表的元数据定义信息。

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

| **参数**     | **说明**                                                |
|------------|-------------------------------------------------------|
| tableName  | 此JSON文件中定义的 StarRocks 表名。                             |
| schemaName | 包含JSON定义的表的库名称。                                       |
| key        | 定义redis key的json对象。                                   |
| value      | 定义redis value的json对象。                                 |
| dataFormat | 决定使用哪一个行解码器来处理Redis key或value。                        |
| fields     | 用于在 StarRocks 表中创建新列的字段定义。                            |
| name       | 映射到StarRocks表中的列名称。                                   |
| type       | 列的数据类型。目前仅支持 `BIGINT` 和 `VARCHAR` 这两种类型。未来还会有更多类型被支持。 |


目前，Redis 中用于存储值的数据类型仅有两种：**String**和**hash**。而 Redis 的键仅支持**String**类型。

> **注意**
> 
> 以下是四种不同的行解码器，用户可依据数据类型使用它们来处理 Redis 中的数据，从而将其转换为 StarRocks 表的数据形式。
>
> 对于 Redis keys而言，只有一种`raw`解码方法。
>- `raw`：该数据不会被进行解析。表示原始的String字符串信息。仅适用于 Redis key。
>
>对于 Redis 中的values，有三种解码方法。
>- `json`: String字符串类型的Redis value数据会被解析为 JSON 格式，JSON 中的值将作为 StarRocks 表的数据。
>- `csv`:  String字符串类型的Redis value数据会被解析为以逗号分隔的 csv 格式，csv 中的值将作为 StarRocks 表的数据。
>- `hash`: hash类型的数据采用哈希格式进行解析，而哈希值则作为 StarRocks 表的数据。

### 创建示例
Based on the three decoder methods of Redis values, here are three examples. Assume that the directory specified by the parameter `redis.table-description-dir` is `/path/redis`
基于 Redis values的三种解码方法，以下是三个示例。假设参数`redis.table-description-dir`指定的目录为`/path/redis`。

- `json` row decoder

使用 Redis CLI 客户端以 JSON 格式写入两行String字符串类型的数据：
 ```redis
SET testdb:testjson:bb "{\"id\":123,\"name\":\"Alice\",\"email\":\"alice@example.com\"}"
SET testdb:testjson:aa "{\"id\":456,\"name\":\"Lily\",\"email\":\"lily@example.com\"}"
```
创建表定义的 JSON 文件 `/path/redis/test_json.json` ：
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

通过 StarRocks 查询 Redis 表 `redis_catalog.testdb.testjson` 中的数据：
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

使用 Redis CLI 客户端以 CSV 格式写入两行String字符串类型的数据。请注意，当前 CSV 格式仅支持逗号分隔值。

 ```redis
SET testdb:testcsv:aa  "123,Alice,alice@example.com"
SET testdb:testcsv:bb  "456,Lily,lily@example.com"
```

创建表定义的 JSON 文件 `/path/redis/test_csv.json` ：
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

通过 StarRocks 查询 Redis 表 `redis_catalog.testdb.testcsv` 中的数据：
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
- 使用 Redis CLI 客户端写入两行哈希类型的数据。
 
```redis
HMSET testdb:testhash:aa id 123  name "Alice" email "alice@example.com"
HMSET testdb:testhash:bb id 456 name "Lily" email "lily@example.com"
```

创建表定义的 JSON 文件 `/path/redis/test_hash.json` ：
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

通过 StarRocks 查询 Redis 表 `redis_catalog.testdb.testhash` 中的数据：
```SQL
mysql> select * from redis_catalog.testdb.testhash;
+--------------------+------+-------+-------------------+
| redis_key          | id   | name  | email             |
+--------------------+------+-------+-------------------+
| testdb:testhash:aa |  123 | Alice | alice@example.com |
| testdb:testhash:bb |  456 | Lily  | lily@example.com  |
+--------------------+------+-------+-------------------+
```

## 查询 Redis Catalog 中的表数据

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查看指定 Catalog 所属的集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 切换当前会话生效的 Catalog：

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    再通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话生效的数据库：

    ```SQL
    USE <db_name>;
    ```

    或者，也可以通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接将会话切换到目标 Catalog 下的指定数据库：

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. 通过 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT * FROM <table_name>;
   ```
