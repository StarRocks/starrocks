---
displayed_sidebar: "English"
---

# JSON

StarRocks starts to support the JSON data type since v2.2.0. This topic describes the basic concepts of JSON. It also describes how to create a JSON column, load JSON data, query JSON data, and use JSON functions and operators to construct and process JSON data.

## What is JSON

JSON is a lightweight, data-interchange format that is designed for semi-structured data. JSON presents data in a hierarchical tree structure, which is flexible and easy to read and write in a wide range of data storage and analytics scenarios. JSON supports `NULL` values and the following data types: NUMBER, STRING, BOOLEAN, ARRAY, and OBJECT.

For more information about JSON, visit the [JSON website](http://www.json.org/?spm=a2c63.p38356.0.0.50756b9fVEfwCd). For information about the input and output syntax of JSON, see JSON specifications at [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4).

StarRocks supports both storage and efficient querying and analytics of JSON data. StarRocks does not directly store the input text. Instead, it stores JSON data in a binary format to reduce the cost of parsing and increase query efficiency.

## Use JSON data

### Create a JSON column

When you create a table, you can use the `JSON` keyword to specify the `j` column as a JSON column.

```sql
CREATE TABLE `tj` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  JSON NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);
```

### Load data and store the data as JSON data

StarRocks provides the following three methods for you to load data and store the data as JSON data:

- Method 1: Use `INSERT INTO` to write data to a JSON column of a table. In the following example, a table named `tj` is used, and the `j` column of the table is a JSON column.

```plaintext
INSERT INTO tj (id, j) VALUES (1, parse_json('{"a": 1, "b": true}'));
INSERT INTO tj (id, j) VALUES (2, parse_json('{"a": 2, "b": false}'));
INSERT INTO tj (id, j) VALUES (3, parse_json('{"a": 3, "b": true}'));
INSERT INTO tj (id, j) VALUES (4, json_object('a', 4, 'b', false)); 
```

> The parse_json function can interpret STRING data as JSON data. The json_object function can construct a JSON object or convert an existing table to a JSON file. For more information, see [parse_json](../../sql-functions/json-functions/json-constructor-functions/parse_json.md) and [json_object](../../sql-functions/json-functions/json-constructor-functions/json_object.md).

- Method 2: Use Stream Load to load a JSON file and store the file as JSON data. For more information, see [Load JSON data](../../../loading/StreamLoad.md#load-json-data).

  - If you want to load a root JSON object, set `jsonpaths` to `$`.
  - If you want to load specific values of a JSON object, set `jsonpaths` to `$.a`, in which `a` specifies a key. For more information about JSON path expressions supported in StarRocks, see [JSON path](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md#json-path-expressions).

- Method 3: Use Broker Load to load a Parquet file and store the file as JSON data. For more information, see [Broker Load](../data-manipulation/BROKER_LOAD.md).

StarRocks supports the following data type conversions at Parquet file loading.

| Data type of Parquet file                                    | JSON data type |
| ------------------------------------------------------------ | -------------- |
| INTEGER (INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, and UINT64) | NUMBER         |
| FLOAT and DOUBLE                                             | NUMBER         |
| BOOLEAN                                                      | BOOLEAN        |
| STRING                                                       | STRING         |
| MAP                                                          | OBJECT         |
| STRUCT                                                       | OBJECT         |
| LIST                                                         | ARRAY          |
| Other data types such as UNION and TIMESTAMP                 | Not supported  |

### Query and process JSON data

StarRocks supports the querying and processing of JSON data and the use of JSON functions and operators.

In the following examples, a table named `tj` is used, and the `j` column of the table is specified as the JSON column.

```plaintext
mysql> select * from tj;
+------+----------------------+
| id   |          j           |
+------+----------------------+
| 1    | {"a": 1, "b": true}  |
| 2    | {"a": 2, "b": false} |
| 3    | {"a": 3, "b": true}  |
| 4    | {"a": 4, "b": false} |
+------+----------------------+
```

Example 1: Filter the data of the JSON column to retrieve the data that meets the `id=1` filter condition.

```plaintext
mysql> select * from tj where id = 1;
+------+---------------------+
| id   |           j         |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
+------+---------------------+
```

Example 2: Filter data of the JSON column `j` to retrieve the data that meets the specified filter condition.

> `j->'a'` returns JSON data. You can use the first example to compare data (Note that implicit conversion is performed in this example). Alternatively, you can convert JSON data to INT by using the CAST function and then compare the data.

```plaintext
mysql> select * from tj where j->'a' = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |


mysql> select * from tj where cast(j->'a' as INT) = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |
+------+---------------------+
```

Example 3: Use the CAST function to convert the values in the JSON column of the table to BOOLEAN values. Then, filter the data of the JSON column to retrieve the data that meets the specified filter condition.

```plaintext
mysql> select * from tj where cast(j->'b' as boolean);
+------+---------------------+
|  id  |          j          |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
| 3    | {"a": 3, "b": true} |
+------+---------------------+
```

Example 4: Use the CAST function to convert the values in the JSON column of the table to BOOLEAN values. Then, filter the data of the JSON column to retrieve the data that meets the specified filter condition, and perform arithmetic operations on the data.

```plaintext
mysql> select cast(j->'a' as int) from tj where cast(j->'b' as boolean);
+-----------------------+
|  CAST(j->'a' AS INT)  |
+-----------------------+
|          3            |
|          1            |
+-----------------------+

mysql> select sum(cast(j->'a' as int)) from tj where cast(j->'b' as boolean);
+----------------------------+
| sum(CAST(j->'a' AS INT))  |
+----------------------------+
|              4             |
+----------------------------+
```

Example 5: Sort the data of the table by using the JSON column as a sort key.

```plaintext
mysql> select * from tj
    ->        where j->'a' <= parse_json('3')
    ->        order by cast(j->'a' as int);
+------+----------------------+
| id   | j                    |
+------+----------------------+
|    1 | {"a": 1, "b": true}  |
|    2 | {"a": 2, "b": false} |
|    3 | {"a": 3, "b": true}  |
|    4 | {"a": 4, "b": false} |
+------+----------------------+
4 rows in set (0.05 sec)
```

## JSON functions and operators

You can use JSON functions and operators to construct and process JSON data. For more information, see [Overview of JSON functions and operators](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md).

## Limits and usage notes

- The maximum length of a JSON value is 16 MB.

- The ORDER BY, GROUP BY, and JOIN clauses do not support references to JSON columns. If you want to create references to JSON columns, use the CAST function to convert JSON columns to SQL columns before you create the references. For more information, see [cast](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md).

- JSON columns are supported in Duplicate Key, Primary Key, and Unique Key tables. They are not supported in Aggregate tables.

- JSON columns cannot be used as partition keys, bucketing keys, or dimension columns of DUPLICATE KEY, PRIMARY KEY, and UNIQUE KEY tables. They cannot be used in ORDER BY, GROUP BY, and JOIN clauses.

- StarRocks allows you to use the following JSON comparison operators to query JSON data: `<`, `<=`, `>`, `>=`, `=`, and `!=`. It does not allow you to use `IN` to query JSON data.
