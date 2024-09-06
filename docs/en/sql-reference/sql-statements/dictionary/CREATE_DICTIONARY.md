---
displayed_sidebar: docs
---

# CREATE DICTIONARY



Creates a dictionary object based on an original object. The dictionary object organizes the key-value mappings from the original object in the form of a hash table and is cached in the memory of all BE nodes. It can be viewed as a cached table.

**Advantages**

- **Richer original objects for dictionary objects**: When using  `dictionary_get()` to query dictionary objects, the original object can be a table of any type, asynchronous materialized view, or logical view. However, when using `dict_mapping()` to query dictionary tables, the dictionary tables can only be primary key tables.
- **Fast query speed**: Since the dictionary object is a hash table and fully cached in the memory of all BE nodes, querying the dictionary object to get the mapping is realized by looking up the hash table in memory. Therefore, the query speed is very fast.
- **Supports multiple value columns**: Internally, the dictionary object encodes multiple value columns into a single STRUCT type column. For  queries based on a key, multiple values are returned together. Therefore, the dictionary object can serve as a dimension table where each key (usually a unique identifier) corresponds to multiple values (descriptive attributes).
- **Ensures consistent snapshot reads**: The dictionary snapshot obtained within the same transaction is consistent, ensuring that the query results from the dictionary object do not change during the same query or load process.

## Syntax

```SQL
CREATE DICTIONARY <dictionary_object_name> USING <dictionary_source>
(
    column_name KEY, [..., column_name KEY,]
    column_name VALUE[, ..., column_name VALUE]
)
[PROPERTIES ("key"="value", ...)];
```

## Parameters

- `dictionary_object_name`: The name of the dictionary object. The dictionary object is effective globally and does not belong to a specific database.
- `dictionary_source`: The name of the original object on which the dictionary object is based. The original object can be a table of any type, asynchronous materialized view, or logical view.
- Definition of columns in the dictionary object: To preserve the key-value mapping maintained in the dictionary table, you need to use the `KEY` and `VALUE` keywords in the dictionary object's columns to specify the keys and their mapped values.
  - The column names `column_name` in the dictionary object must be consistent with those in the dictionary table.
  - The data types for key and value columns in the dictionary object are limited to boolean, integer, string, and date types.
  - The key column in the original object must ensure uniqueness.
- Related properties of dictionary objects (`PROPERTIES`):
  - `dictionary_warm_up`: The method to cache data into the dictionary object on each BE node. Valid values: `TRUE` (default) or `FALSE`. If the parameter is set to `TRUE`, data is automatically cached into the dictionary object after its creation; if the parameter is set to `FALSE`, you need to manually refresh the dictionary object to cache the data.
  - `dictionary_memory_limit`: The maximum memory the dictionary object can occupy on each BE node. Unit: bytes. Default value: 2,000,000,000 bytes (2 GB).
  - `dictionary_refresh_interval`: The interval for periodically refreshing the dictionary object. Unit: seconds. Default value: `0`. A value `<=0` means no automatic refresh.
  - `dictionary_read_latest`: Whether to only query the latest dictionary object, mainly affecting the dictionary object queried during refresh. Valid values: `TRUE` or `FALSE` (default). If the parameter is set to `TRUE`, the dictionary object cannot be queried during refresh because the latest dictionary object is still being refreshed. If the parameter is set to `FALSE`, the previously successfully cached dictionary object can be queried during refresh.
  - `dictionary_ignore_failed_refresh`: Whether to automatically roll back to the last successfully cached dictionary object if the refresh fails. Valid values: `TRUE` or `FALSE` (default). If the parameter is set to `TRUE`, it automatically rolls back to the last successfully cached dictionary object when the refresh fails. If the parameter is set to `FALSE`, the dictionary object status is set to `CANCELLED` when the refresh fails.

## Usage notes

- The dictionary object is fully cached in the memory of each BE node, so it consumes relatively more memory.
- Even if the original object is deleted, the dictionary object created based on it still exists. You need to manually DROP the dictionary object.

## Examples

**Example 1: Create a simple dictionary object to replace the original dictionary table.**

Take the following dictionary table as an example and insert test data.

```Plain
MySQL > CREATE TABLE dict (
    order_uuid STRING,
    order_id_int BIGINT AUTO_INCREMENT 
)
PRIMARY KEY (order_uuid)
DISTRIBUTED BY HASH (order_uuid);
Query OK, 0 rows affected (0.02 sec)
MySQL > INSERT INTO dict (order_uuid) VALUES ('a1'), ('a2'), ('a3');
Query OK, 3 rows affected (0.12 sec)
{'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
MySQL > SELECT * FROM dict;
+------------+--------------+
| order_uuid | order_id_int |
+------------+--------------+
| a1         |            1 |
| a2         |            2 |
| a3         |            3 |
+------------+--------------+
3 rows in set (0.01 sec)
```

Create a dictionary object based on the mappings in this dictionary table.

```Plain
MySQL > CREATE DICTIONARY dict_obj USING dict
    (order_uuid KEY,
     order_id_int VALUE);
Query OK, 0 rows affected (0.00 sec)
```

For future queries of the mappings in the dictionary table, you can directly query the dictionary object instead of the dictionary table. For example, query the value mapped by key `a1`.

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```

**Example 2: Create a dictionary object to replace the original dimension table**

Take the following dimension table as an example and insert test data.

```Plain
MySQL > CREATE TABLE ProductDimension (
    ProductKey BIGINT AUTO_INCREMENT,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50),
    SubCategory VARCHAR(50),
    Brand VARCHAR(50),
    Color VARCHAR(20),
    Size VARCHAR(20)
)
PRIMARY KEY (ProductKey)
DISTRIBUTED BY HASH (ProductKey);
MySQL > INSERT INTO ProductDimension (ProductName, Category, SubCategory, Brand, Color, Size)
VALUES
    ('T-Shirt', 'Apparel', 'Shirts', 'BrandA', 'Red', 'M'),
    ('Jeans', 'Apparel', 'Pants', 'BrandB', 'Blue', 'L'),
    ('Running Shoes', 'Footwear', 'Athletic', 'BrandC', 'Black', '10'),
    ('Jacket', 'Apparel', 'Outerwear', 'BrandA', 'Green', 'XL'),
    ('Baseball Cap', 'Accessories', 'Hats', 'BrandD', 'White', 'OneSize');
Query OK, 5 rows affected (0.48 sec)
{'label':'insert_e938481f-181e-11ef-a6a9-00163e19e14e', 'status':'VISIBLE', 'txnId':'50'}
MySQL > SELECT * FROM ProductDimension;
+------------+---------------+-------------+-------------+--------+-------+---------+
| ProductKey | ProductName   | Category    | SubCategory | Brand  | Color | Size    |
+------------+---------------+-------------+-------------+--------+-------+---------+
|          1 | T-Shirt       | Apparel     | Shirts      | BrandA | Red   | M       |
|          2 | Jeans         | Apparel     | Pants       | BrandB | Blue  | L       |
|          3 | Running Shoes | Footwear    | Athletic    | BrandC | Black | 10      |
|          4 | Jacket        | Apparel     | Outerwear   | BrandA | Green | XL      |
|          5 | Baseball Cap  | Accessories | Hats        | BrandD | White | OneSize |
+------------+---------------+-------------+-------------+--------+-------+---------+
5 rows in set (0.02 sec)
```

Create a dictionary object to replace the original dimension table.

```Plain
MySQL > CREATE DICTIONARY dimension_obj USING ProductDimension 
    (ProductKey KEY,
     ProductName VALUE,
     Category VALUE,
     SubCategory VALUE,
     Brand VALUE,
     Color VALUE,
     Size VALUE);
Query OK, 0 rows affected (0.00 sec)
```

For future queries of dimension values, you can directly query the dictionary object instead of the dimension table to obtain dimension values. For example, query the value mapped by key `1`.

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```