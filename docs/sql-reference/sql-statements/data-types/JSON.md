# JSON

StarRocks has started to support JSON since v2.2.0.  This topic describes the basic concepts of JSON. This topic also describes how to create a JSON column, load JSON data, query JSON data, and use JSON functions and operators to construct and process JSON data.

## What is JSON

JSON is a lightweight data-interchange format that is designed for semi-structured data. JSON presents data in a hierarchical tree structure, which is flexible and easy to read and write in a wide range of data storage and analytics scenarios. JSON supports `NULL` values and the following data types: NUMBER, STRING, BOOLEAN, ARRAY, and OBJECT.

For more information about JSON, visit the [JSON website](http://www.json.org/?spm=a2c63.p38356.0.0.50756b9fVEfwCd). For information about the input and output syntax of JSON, see the JSON specification at [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4).

StarRocks supports both storage and efficient querying and analytics of JSON data. StarRocks does not directly store the input text. Instead, StarRocks stores JSON data in a binary encoding format to reduce the cost of parsing and increase query efficiency.

## Use JSON data

### Create a JSON column

When you create a table, you can use the `JSON` keyword to specify the `j` column of the table as a JSON column.

```Plain%20Text
CREATE TABLE `tj` (

    `id` INT(11) NOT NULL COMMENT "",

    `j`  JSON NULL COMMENT ""

) ENGINE=OLAP

DUPLICATE KEY(`id`)

COMMENT "OLAP"

DISTRIBUTED BY HASH(`id`) BUCKETS 1

PROPERTIES (

    "replication_num" = "1",

    "in_memory" = "false",

    "storage_format" = "DEFAULT"

);
```

### Load data and store the data as JSON data

StarRocks provides the following three methods for you to load data and store the data as JSON data:

- Method 1: Use `INSERT INTO` to write data to a JSON column of a table. In the following example, a table named `tj` is used, and the `j` column of the table is specified as the JSON column.

```Plain%20Text
INSERT INTO tj (id, j) VALUES (1, parse_json('{"a": 1, "b": true}'));

INSERT INTO tj (id, j) VALUES (2, parse_json('{"a": 2, "b": false}'));

INSERT INTO tj (id, j) VALUES (3, parse_json('{"a": 3, "b": true}'));

INSERT INTO tj (id, j) VALUES (4, json_object('a', 4, 'b', false)); 
```

> The parse_json function can interpret STRING data as JSON data. The json_object function can construct a JSON object or convert an existing table to a JSON file. For more information, see [parse_json](../../sql-functions/json-functions/json-constructor-functions/parse_json.md) and [json_object](../../sql-functions/json-functions/json-constructor-functions/json_object.md).

- Method 2: Use Stream Load to import a JSON file and store the file as JSON data. For more information, see [Load JSON data](../../../loading/StreamLoad.md).

  - If you want to import the specified values of a JSON object in a JSON file and store the values as JSON data, set `jsonpaths` to `$.a`, in which `a` specifies a key.

  - If you want to import a JSON object from a JSON file and store the JSON object as JSON data, set `jsonpaths` to `$`.

- Method 3: Use Broker Load to import a Parquet file and store the file as JSON data. For more information, see [Broker Load](../../../loading/BrokerLoad.md).

StarRocks supports the following data type conversions at Parquet file import.

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

```Plain%20Text
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

Example 1: Filter the data of the JSON column in the table to retrieve the data that meets the `id=1` filter condition.

```Plain%20Text
mysql> select * from tj where id = 1;

+------+---------------------+

| id   |           j         |

+------+---------------------+

| 1    | {"a": 1, "b": true} |

+------+---------------------+
```

Example 2: Filter the data of the JSON column in the table to retrieve the data that meets the specified filter condition.

> `j->'a'` returns JSON data. You can interpret SQL data as JSON data by using the PARSE_JSON function and then compare the data. Alternatively, you can convert JSON data to INT data by using the CAST function and then compare the data.

```Plain%20Text
mysql> select * from tj where j->'a' = parse_json('1');

+------+---------------------+

|   id |         j           |

+------+---------------------+

|   1  | {"a": 1, "b": true} |

+------+---------------------+



mysql> select * from tj where cast(j->'a' as INT) = 1; 

+------+---------------------+

|   id |         j           |

+------+---------------------+

|   1  | {"a": 1, "b": true} |

+------+---------------------+
```

Example 3: Use the CAST function to convert the values in the JSON column of the table to BOOLEAN values. Then, filter the data of the JSON column to retrieve the data that meets the specified filter condition.

```Plain%20Text
mysql> select * from tj where cast(j->'b' as boolean);

+------+---------------------+

|  id  |          j          |

+------+---------------------+

| 1    | {"a": 1, "b": true} |

| 3    | {"a": 3, "b": true} |

+------+---------------------+
```

Example 4: Use the CAST function to convert the values in the JSON column of the table to BOOLEAN values. Then, filter the data of the JSON column to retrieve the data that meets the specified filter condition, and perform arithmetic operations on the data.

```Plain%20Text
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

```Plain%20Text
mysql> select * from tj

       where j->'a' <= parse_json('3')

       order by cast(j->'a' as int);

+------+----------------------+

| id   |           j          |

+------+----------------------+

|  1   | {"a": 1, "b": true}  |

|  2   | {"a": 2, "b": false} |

|  3   | {"a": 3, "b": true}  |

+------+----------------------+
```

## JSON functions and operators

You can use JSON functions and operators to construct and process JSON data. For more information, see [Overview of JSON functions and operators](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md).

## Limits and usage notes

- The maximum length per JSON value is the same as the maximum length per STRING value.

- The ORDER BY, GROUP BY, and JOIN clauses do not support references to JSON columns. If you want to create references to JSON columns, use the CAST function to convert JSON columns to SQL columns before you create the references. For more information, see [cast](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md).

- JSON columns are supported only in tables that use the Duplicate Key model. Such JSON columns cannot be defined as sort keys in the tables to which they belong.

- JSON columns cannot be used as partition keys or bucket keys.

- StarRocks allows you to use the following JSON operators to query JSON data: `<`, `<=`, `>`, `>=`, `=`, and `!=`. StarRocks does not support the `IN` operator.
