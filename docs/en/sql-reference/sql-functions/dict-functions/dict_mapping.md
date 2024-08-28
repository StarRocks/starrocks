---
displayed_sidebar: docs
---

# dict_mapping

## Description

Returns the value mapped to the specified key in a dictionary table.

This function is mainly used to simplify the application of a global dictionary table. During data loading into a target table, StarRocks automatically obtains the value mapped to the specified key from the dictionary table by using the input parameters in this function, and then loads the value into the target table.

Since v3.2.5, StarRocks supports this function. Also, note that currently StarRocks's shared-data mode does not support this function.

## Syntax

```SQL
dict_mapping("[<db_name>.]<dict_table>", key_column_expr_list [, <value_column> ] [, <strict_mode>] )

key_column_expr_list ::= key_column_expr [, key_column_expr ... ]

key_column_expr ::= <column_name> | <expr>
```

## Parameters

- Required parameters:
  - `[<db_name>.]<dict_table>`: The name of the dictionary table, which needs to be a Primary Key table. The supported data type is VARCHAR.
  - `key_column_expr_list`: The expression list for key columns in the dictionary table, including one or multiple `key_column_exprs`. The `key_column_expr` can be the name of a key column in the dictionary table, or a specific key or key expression.

    This expression list needs to include all Primary Key columns of the dictionary table, which means the total number of expressions needs to match the total number of Primary Key columns in the dictionary table. So when the dictionary table uses Composite Primary Key, the expressions in this list needs to correspond to the Primary Key columns defined in the table schema by sequence. Multiple expressions in this list are separated by commas (`,`). And if a `key_column_expr` is a specific key or key expression, its type must match the type of the corresponding Primary Key column in the dictionary table.

- Optional parameters:
  - `<value_column>`: The name of the value column, which is also the mapping column. If the value column is not specified, the default value column is the AUTO_INCREMENT column of the dictionary table.  The value column can also be defined as any column in the dictionary table excluding auto-incremented columns and primary keys. The column's data type has no restrictions.
  - `<strict_mode>`: Whether to enable strict mode, that is, whether to return an error when the value mapped to the specified key is not found. If the parameter is set to `TRUE`,  an error is returned. If the parameter is set to `FALSE` (default),  `NULL` is returned.

## Return Value

The data type of the returned values remains consistent with the data type of the value column. If the value column is the auto-incremented column of the dictionary table, the data type of the returned values is BIGINT.

However, when the value mapped to the specified key is not found, if the `strict_mode` parameter is set to `FALSE` (default),  `NULL` is returned. If the parameter is set to `TRUE`,  an error `ERROR 1064 (HY000): In strict mode, query failed if the record does not exist in the dict table` is returned.

## Example

**Example 1: Directly query the value mapped to a key from a dictionary table.**

1. Create a dictionary table and load simulated data.

      ```SQL
      MySQL [test]> CREATE TABLE dict (
          order_uuid STRING,
          order_id_int BIGINT AUTO_INCREMENT 
      )
      PRIMARY KEY (order_uuid)
      DISTRIBUTED BY HASH (order_uuid);
      Query OK, 0 rows affected (0.02 sec)
      
      MySQL [test]> INSERT INTO dict (order_uuid) VALUES ('a1'), ('a2'), ('a3');
      Query OK, 3 rows affected (0.12 sec)
      {'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
      
      MySQL [test]> SELECT * FROM dict;
      +------------+--------------+
      | order_uuid | order_id_int |
      +------------+--------------+
      | a1         |            1 |
      | a3         |            3 |
      | a2         |            2 |
      +------------+--------------+
      3 rows in set (0.01 sec)
      ```

      > **NOTICE**
      >
      > Currently the `INSERT INTO` statement does not support partial updates. So please make sure that the values inserted into the `dict`'s key column are not duplicated. Otherwise, inserting the same key column value in the dictionary table multiple times causes its mapped value in the value column to change.

2. Query the value mapped to key `a1` in the dictionary table.

    ```SQL
    MySQL [test]> SELECT dict_mapping('dict', 'a1');
    +----------------------------+
    | dict_mapping('dict', 'a1') |
    +----------------------------+
    |                          1 |
    +----------------------------+
    1 row in set (0.01 sec)
    ```

**Example 2: The mapping column in the table is configured as a generated column using the `dict_mapping` function. So StarRocks can automatically obtain the values mapped to the keys when loading data into this table.**

1. Create a data table and configure the mapping column as a generated column by using `dict_mapping('dict', order_uuid)`.

    ```SQL
    CREATE TABLE dest_table1 (
        id BIGINT,
        -- This column records the STRING type order number, corresponding to the order_uuid column in the dict table in Example 1.
        order_uuid STRING, 
        batch int comment 'used to distinguish different batch loading',
        -- This column records the BIGINT type order number which mapped with the order_uuid column.
        -- Because this column is a generated column configured with dict_mapping, the values in this column are automatically obtained from the dict table in Example 1 during data loading.
        -- Subsequently, this column can be directly used for deduplication and JOIN queries.
        order_id_int BIGINT AS dict_mapping('dict', order_uuid)
    )
    DUPLICATE KEY (id, order_uuid)
    DISTRIBUTED BY HASH(id);
    ```

2. When loading simulated data into this table where the `order_id_int` column is configured as `dict_mapping('dict', 'order_uuid')`, StarRocks automatically loads values into the `order_id_int` column based on the mapping relationship between keys and values in the `dict` table.

      ```SQL
      MySQL [test]> INSERT INTO dest_table1(id, order_uuid, batch) VALUES (1, 'a1', 1), (2, 'a1', 1), (3, 'a3', 1), (4, 'a3', 1);
      Query OK, 4 rows affected (0.05 sec) 
      {'label':'insert_e191b9e4-8a98-11ee-b29c-00163e03897d', 'status':'VISIBLE', 'txnId':'72'}
      
      MySQL [test]> SELECT * FROM dest_table1;
      +------+------------+-------+--------------+
      | id   | order_uuid | batch | order_id_int |
      +------+------------+-------+--------------+
      |    1 | a1         |     1 |            1 |
      |    4 | a3         |     1 |            3 |
      |    2 | a1         |     1 |            1 |
      |    3 | a3         |     1 |            3 |
      +------+------------+-------+--------------+
      4 rows in set (0.02 sec)
      ```

    The usage of `dict_mapping` in this example can accelerate [deduplication calculations and JOIN queries](../../../using_starrocks/query_acceleration_with_auto_increment.md). Compared to the previous solutions for building a global dictionary to accelerate precise deduplication, the  solution by using `dict_mapping` is more flexible and user-friendly. Because the mapping values are directly obtained from the dictionary table at the stage "loading mapping relationships between keys and values into the table". You do not need to write statements to join the dictionary table to obtain mapping values. Additionally, this solution supports various data loading methods.<!--For detailed usage, please refer to xxx.-->

**Example 3: If the mapping column in the table is not configured as a generated column, you need to explicitly configure the `dict_mapping` function for the mapping column when loading data into the table, obtain the values mapped to the keys.**

> **NOTICE**
>
> The difference between Example 3 and Example 2 is that when importing into the data table, you need to modify the import command to explicitly configure the `dict_mapping` expression for the mapping column.

1. Create a table.

    ```SQL
    CREATE TABLE dest_table2 (
        id BIGINT,
        order_uuid STRING,
        order_id_int BIGINT NULL,
        batch int comment 'used to distinguish different batch loading'
    )
    DUPLICATE KEY (id, order_uuid, order_id_int)
    DISTRIBUTED BY HASH(id);
    ```

2. When simulated data loads into this table, you obtain the mapped values from the dictionary table by configuring `dict_mapping`.

    ```SQL
    MySQL [test]> INSERT INTO dest_table2 VALUES (1, 'a1', dict_mapping('dict', 'a1'), 1);
    Query OK, 1 row affected (0.35 sec)
    {'label':'insert_19872ab6-8a96-11ee-b29c-00163e03897d', 'status':'VISIBLE', 'txnId':'42'}

    MySQL [test]> SELECT * FROM dest_table2;
    +------+------------+--------------+-------+
    | id   | order_uuid | order_id_int | batch |
    +------+------------+--------------+-------+
    |    1 | a1         |            1 |     1 |
    +------+------------+--------------+-------+
    1 row in set (0.02 sec)
    ```

**Example 4: Enable strict mode**

When strict mode is enabled and the value mapped to the key that doesn't exist in the dictionary table is queried , an error, instead of `NULL`, is returned . It makes sure that a data row's key is first loaded into the dictionary table and its mapped value (dictionary ID) is generated before that data row is loaded into the target table.

```SQL
MySQL [test]>  SELECT dict_mapping('dict', 'b1', true);
ERROR 1064 (HY000): In strict mode, query failed if record not exist in dict table.
```

**Example 5: If the dictionary table uses composite primary keys, all primary keys must be specified when querying.**

1. Create a dictionary table with Composite Primary Keys and load simulated data into it.

      ```SQL
      MySQL [test]> CREATE TABLE dict2 (
          order_uuid STRING,
          order_date DATE, 
          order_id_int BIGINT AUTO_INCREMENT
      )
      PRIMARY KEY (order_uuid,order_date)  -- Composite Primary Key
      DISTRIBUTED BY HASH (order_uuid,order_date)
      ;
      Query OK, 0 rows affected (0.02 sec)
      
      MySQL [test]> INSERT INTO dict2 VALUES ('a1','2023-11-22',default), ('a2','2023-11-22',default), ('a3','2023-11-22',default);
      Query OK, 3 rows affected (0.12 sec)
      {'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
      
      
      MySQL [test]> select * from dict2;
      +------------+------------+--------------+
      | order_uuid | order_date | order_id_int |
      +------------+------------+--------------+
      | a1         | 2023-11-22 |            1 |
      | a3         | 2023-11-22 |            3 |
      | a2         | 2023-11-22 |            2 |
      +------------+------------+--------------+
      3 rows in set (0.01 sec)
      ```

2. Query the value mapped to the key in the dictionary table. Because the dictionary table has Composite Primary Keys, all primary keys need to be specified in `dict_mapping`.

      ```SQL
      SELECT dict_mapping('dict2', 'a1', cast('2023-11-22' as DATE));
      ```

   Note that an error occurs when only one Primary Key is specified.

      ```SQL
      MySQL [test]> SELECT dict_mapping('dict2', 'a1');
      ERROR 1064 (HY000): Getting analyzing error. Detail message: dict_mapping function param size should be 3 - 5.
      ```
