---
displayed_sidebar: docs
---

# JSON

Since version 2.2.0, StarRocks supports JSON. This article introduces the basic concepts of JSON, and how StarRocks creates JSON-type columns, loads, and queries JSON data, and constructs and processes JSON data through JSON functions and operators.

## What is JSON

JSON is a lightweight data interchange format. JSON-type data is semi-structured and supports a tree structure. JSON data is hierarchical, flexible, easy to read and process, and is widely used in data storage and analysis scenarios. JSON supports data types such as NUMBER, STRING, BOOLEAN, ARRAY, OBJECT, and NULL values.

For more information on JSON, please refer to the [JSON official website](https://www.json.org/json-en.html). For JSON data input and output syntax, please refer to the JSON specification [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4).

StarRocks supports storing and efficiently querying and analyzing JSON data. StarRocks uses binary format encoding to store JSON data instead of storing the input text directly, which reduces parsing costs during data computation and queries, thereby improving query efficiency.

## Using JSON Data

### Creating JSON-Type Columns

When creating a table, specify the column `j` as JSON type using the keyword `JSON`.

```SQL
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

### Loading Data and Storing as JSON Type

StarRocks supports the following methods to load data and store it as JSON type.

- Method 1: Use `INSERT INTO` to write data into a JSON-type column (e.g., column `j`).

```SQL
INSERT INTO tj (id, j) VALUES (1, parse_json('{"a": 1, "b": true}'));
INSERT INTO tj (id, j) VALUES (2, parse_json('{"a": 2, "b": false}'));
INSERT INTO tj (id, j) VALUES (3, parse_json('{"a": 3, "b": true}'));
INSERT INTO tj (id, j) VALUES (4, json_object('a', 4, 'b', false)); 
```

> The PARSE_JSON function can construct JSON-type data based on string-type data. The JSON_OBJECT function can construct JSON object-type data, allowing existing tables to be converted to JSON type. For more information, please refer to [PARSE_JSON](../../sql-functions/json-functions/json-constructor-functions/parse_json.md) and [JSON_OBJECT](../../sql-functions/json-functions/json-constructor-functions/json_object.md).

- Method 2: Use Stream Load to import JSON files and store them as JSON type. For import methods, please refer to [Import JSON Data](../../../loading/StreamLoad.md).
  - To import and store the JSON object at the root node of a JSON file as JSON type, set `jsonpaths` to `$`.
  - To import and store the value of a JSON object in a JSON file as JSON type, set `jsonpaths` to `$.a` (where `a` represents the key). For more JSON path expressions, refer to [JSON path](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md#json-path).

- Method 3: Use Broker Load to import Parquet files and store them as JSON type. For import methods, please refer to [Broker Load](../../sql-statements/loading_unloading/BROKER_LOAD.md).

Data type conversion is supported during import as follows:

| Data Type in Parquet File                                     | Converted JSON Data Type |
| ------------------------------------------------------------- | ------------------------ |
| Integer types (INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64) | JSON Number              |
| Floating-point types (FLOAT, DOUBLE)                          | JSON Number              |
| BOOLEAN                                                       | JSON Boolean             |
| STRING                                                        | JSON String              |
| MAP                                                           | JSON Object              |
| STRUCT                                                        | JSON Object              |
| LIST                                                          | JSON Array               |
| UNION, TIMESTAMP, and other types                             | Not supported            |

- Method 4: Use [Routine Load](../../../loading/Loading_intro.md) to continuously consume JSON format data from Kafka and import it into StarRocks.

### Querying and Processing JSON-Type Data

StarRocks supports querying and processing JSON-type data and supports using JSON functions and operators.

This example uses the table `tj` for illustration.

```SQL
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

Example 1: Filter data in the JSON-type column that meets the condition `id=1`.

```SQL
mysql> select * from tj where id = 1;
+------+---------------------+
| id   |           j         |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
+------+---------------------+
```

Example 2: Filter data in the table based on the JSON-type column.

> In the following example, `j->'a'` returns JSON-type data. You can compare it with the first example, which performs implicit conversion on the data; or use the CAST function to construct JSON-type data as INT for comparison.

```SQL
mysql> select * from tj where j->'a' = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |
+------+---------------------+

mysql> select * from tj where cast(j->'a' as INT) = 1; 
+------+---------------------+
|   id |         j           |
+------+---------------------+
|   1  | {"a": 1, "b": true} |
+------+---------------------+
```

Example 3: Filter data in the table based on the JSON-type column (you can use the CAST function to construct the JSON-type column as BOOLEAN type).

```SQL
mysql> select * from tj where cast(j->'b' as boolean);
+------+---------------------+
|  id  |          j          |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
| 3    | {"a": 3, "b": true} |
+------+---------------------+
```

Example 4: Filter data in the JSON-type column that meets the condition and perform numerical operations.

```SQL
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

Example 5: Sort based on the JSON-type column.

```SQL
mysql> select * from tj
       where j->'a' <= 3
       order by cast(j->'a' as int);
+------+----------------------+
| id   | j                    |
+------+----------------------+
|    1 | {"a": 1, "b": true}  |
|    2 | {"a": 2, "b": false} |
|    3 | {"a": 3, "b": true}  |
|    4 | {"a": 4, "b": false} |
+------+----------------------+
```

## JSON Functions and Operators

JSON functions and operators can be used to construct and process JSON data. For detailed information, please refer to [JSON Functions and Operators](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md).

## JSON Array

JSON can contain nested data, such as Objects, Arrays, or other JSON data types nested within an Array. StarRocks provides a rich set of functions and operators to handle these complex nested JSON data structures. The following will introduce how to handle JSON data containing arrays.

Suppose there is a JSON field `event_data` in the `events` table, with the following content:
```
{
  "user": "Alice",
  "actions": [
    {"type": "click", "timestamp": "2024-03-17T10:00:00Z", "quantity": 1},
    {"type": "view", "timestamp": "2024-03-17T10:05:00Z", "quantity": 2},
    {"type": "purchase", "timestamp": "2024-03-17T10:10:00Z", "quantity": 3}
  ]
}
```

The following examples demonstrate several common JSON array analysis scenarios:

1. Extract array elements: Extract specific fields such as type, timestamp, etc., from the actions array and perform projection operations.
2. Array expansion: Use the `json_each` function to expand the nested JSON array into a multi-row, multi-column table structure for subsequent analysis.
3. Array computation: Use Array Functions to filter, transform, and aggregate array elements, such as counting the number of specific types of operations.

### 1. Extract Elements from JSON Array

To extract a nested element from a JSON Array, you can use the following syntax:
- The return type is still a JSON Array, and you can use the CAST expression for type conversion.
```
MySQL > SELECT json_query(event_data, '$.actions[*].type') as json_array FROM events;
+-------------------------------+
| json_array                    |
+-------------------------------+
| ["click", "view", "purchase"] |
+-------------------------------+

MySQL > SELECT cast(json_query(event_data, '$.actions[*].type') as array<string>) array_string FROM events;
+-----------------------------+
| array_string                |
+-----------------------------+
| ["click","view","purchase"] |
+-----------------------------+
```

### 2. Expand Using json_each
StarRocks provides the `json_each` function to expand JSON arrays, converting them into multiple rows of data. For example:
```
MySQL > select value from events, json_each(event_data->'actions');
+--------------------------------------------------------------------------+
| value                                                                    |
+--------------------------------------------------------------------------+
| {"quantity": 1, "timestamp": "2024-03-17T10:00:00Z", "type": "click"}    |
| {"quantity": 2, "timestamp": "2024-03-17T10:05:00Z", "type": "view"}     |
| {"quantity": 3, "timestamp": "2024-03-17T10:10:00Z", "type": "purchase"} |
+--------------------------------------------------------------------------+
```

To extract the type and timestamp fields separately:
```
MySQL > select value->'timestamp', value->'type' from events, json_each(event_data->'actions');
+------------------------+---------------+
| value->'timestamp'     | value->'type' |
+------------------------+---------------+
| "2024-03-17T10:00:00Z" | "click"       |
| "2024-03-17T10:05:00Z" | "view"        |
| "2024-03-17T10:10:00Z" | "purchase"    |
+------------------------+---------------+
```

After this, the JSON Array data becomes a familiar relational model, allowing the use of common functions for analysis.

### 3. Use Array Functions for Filtering and Calculation
StarRocks also supports ARRAY-related functions, which can be used in conjunction with JSON functions for more efficient queries. By combining these functions, you can filter, transform, and aggregate JSON array data. The following example demonstrates how to use these functions:
```
MySQL > 
WITH step1 AS (
 SELECT cast(event_data->'actions' as ARRAY<JSON>) as docs
   FROM events
)
SELECT array_filter(doc -> get_json_string(doc, 'type') = 'click', docs) as clicks
FROM step1
+---------------------------------------------------------------------------+
| clicks                                                                    |
+---------------------------------------------------------------------------+
| ['{"quantity": 1, "timestamp": "2024-03-17T10:00:00Z", "type": "click"}'] |
+---------------------------------------------------------------------------+
```

Furthermore, you can combine other ARRAY Functions to perform aggregation calculations on array elements:
```
MySQL > 
WITH step1 AS (
 SELECT cast(event_data->'actions' as ARRAY<JSON>) as docs
   FROM events
), step2 AS (
    SELECT array_filter(doc -> get_json_string(doc, 'type') = 'click', docs) as clicks
    FROM step1
)
SELECT array_sum(
            array_map(doc -> get_json_double(doc, 'quantity'), clicks)
            ) as click_amount
FROM step2
+--------------+
| click_amount |
+--------------+
| 1.0          |
+--------------+
```

## Limitations and Considerations

- The maximum supported length for JSON-type data is currently 16 MB.
- ORDER BY, GROUP BY, and JOIN clauses do not support referencing JSON-type columns. If you need to reference them, you can use the CAST function in advance to convert JSON-type columns to other SQL types. For specific conversion methods, please refer to [JSON Type Conversion](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md).
- JSON-type columns can exist in Duplicate Key tables, Primary Key tables, and Unique Key tables, but not in Aggregate tables.
- JSON-type columns are not supported as partition keys, bucketing keys, or dimension columns in Duplicate Key tables, Primary Key tables, and Unique Key tables, and cannot be used in JOIN, GROUP BY, or ORDER BY clauses.
- StarRocks supports using `<`, `<=`, `>`, `>=`, `=`, `!=` operators to query JSON data but does not support the IN operator.
