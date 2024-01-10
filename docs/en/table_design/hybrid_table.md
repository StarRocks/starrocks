---
displayed_sidebar: "English"
---

# Hybrid row/column-oriented tables

As OLAP database, StarRocks originally stores data in tables in the columnar format, which can enhance the performance of complex queries, such as aggregate queries. Since v3.2.3, StarRocks also supports hybrid row/column-oriented tables whose data is stored in both row-by-row and column-by-column fashions. This hybrid row/column storage is well suited for various scenario such as primary key-based high-concurrency, low-latency point queries and partial column updates, while delivering efficient analytical capabilities comparable to columnar storage. Additionally, hybrid row/column-oriented tables support [prepared statements](../), which enhances query performance and security.

## Comparisons between column-oriented tables and hybrid row/column-oriented tables 

| **Storage format**         | **Storage method**                                           | **Scenarios**                                                |
| -------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Hybrid row/column-oriented | Data is stored in both row-by-row and column-by-column fashions. Simply put, a hybrid row/column-oriented table contains an additional, hidden binary-type column `__row`. When data is written to the table, all values from the value columns of a row are encoded and written to the `__row` column (as shown below).![img](https://starrocks.feishu.cn/space/api/box/stream/download/asynccode/?code=OTRkNGJiMzViOTQ1MTQ5ZDQ2NjRlYjY0M2FmZGVlZTlfT3p4RHA5S2R4YXNNeGxEU1J3cU5ON0Ywd0w4SlBoeE5fVG9rZW46THhXZWI0QWxRb0hybzR4U0ZaRmNMYlhnbjhiXzE3MDQ4NzA4Mjg6MTcwNDg3NDQyOF9WNA) | Suitable for scenarios such as primary key-based point queries and partial column updates, because this storage method can help greatly reduce random IO and read-write amplification in these scenarios.Point queries, which are primary key-based simple queries that scan and return small amounts of data.Queries against most or all of the fields from tables that consist of a small number of fields.Partial column updates.Prepared statements can be run on hybrid row/column-oriented tables, which can enhance query performance by saving the overhead of parsing SQL statements, and also prevent SQL injection attacks.Also suitable for complex data analysis. |
| Column-oriented            | [Data is stored in a column-by-column fashion]()![img](https://starrocks.feishu.cn/space/api/box/stream/download/asynccode/?code=MDZlZWViMDdkNjVlNWQ1NDU2MjQ3ZTk4ZmYxODY1ZWZfaWtNdXN2cDhEM2lFTkRxUkNpbmZjWlJLelpRTlFlRkFfVG9rZW46UDc1b2I2dXhBb1czRGV4cDZpWWNyemxNbmpoXzE3MDQ4NzA4Mjg6MTcwNDg3NDQyOF9WNA) | Complex or ad-hoc queries on massive data.Tables (such as wide tables) consist of many fields. Queries involve only a few columns. |

## Basic usages  

### Create a hybrid row/column-oriented table

Specify `"STORE_TYPE" = "column_with_row"` in the `PROPERTIES` at table creation.

:::note

\- The table must be a Primary Key table.

\- The length of the `__row` column cannot exceed 1 MB.

\- Columns cannot be of data types like ARRAY, MAP, and STRUCT.

·:::

```SQL
create table users (
  id bigint not null,
  country string,
  city string,
  revenue bigint
)
primary key (id)
distributed by hash(id)
properties ("store_type" = "column_with_row");
```

### Insert, delete, and update data

Similar to column-oriented tables, you can insert, delete, and update data from a hybrid row/column-oriented table using data loading and DML statements. This section demonstrates how to run DML statements on the above hybrid row/column-oriented table.

1. Insert a data row.

   1. ```SQL
      INSERT INTO users (id, country, city, revenue)
      VALUES 
        (1, 'USA', 'New York', 5000),
        (2, 'UK', 'London', 4500),
        (3, 'France', 'Paris', 6000),
        (4, 'Germany', 'Berlin', 4000),
        (5, 'Japan', 'Tokyo', 7000),
        (6, 'Australia', 'Sydney', 7500);
      ```

2. Delete a data row.

   1. ```SQL
      DELETE FROM users WHERE id = 6;
      ```

3. Update a data row.

   1. ```SQL
      UPDATE users SET revenue = 6500 WHERE id = 4;
      ```

### Query data

This section uses a point query as an example. Point queries take a short path, directly querying data in row storage, which can improve query performance.

The following example still uses the above hybrid row/column-oriented table. After the table creation and data modification operations mentioned above, the table stores the data as follows:

```SQL
MySQL [example_db]> SELECT * FROM users ORDER BY id;
+------+---------+----------+---------+
| id   | country | city     | revenue |
+------+---------+----------+---------+
|    1 | USA     | New York |    5000 |
|    2 | UK      | London   |    4500 |
|    3 | France  | Paris    |    6000 |
|    4 | Germany | Berlin   |    6500 |
|    5 | Japan   | Tokyo    |    7000 |
+------+---------+----------+---------+
5 rows in set (0.03 sec)
```

1. Make sure that the system enables the short path for queries. Once the short path for queries is enabled, queries that meet certain criteria (to evaluate whether the query is a point query) take the short path to scan data in row storage.

   1. ```SQL
      SHOW VARIABLES LIKE '%enable_short_circuit%';
      ```

   2.  If the short path for queries is not enabled, run the `SET enable_short_circuit = true;` command to set the variable [`enable_short_circuit`](xxx)  to `true`.

2. Query data. If the query meets the criteria that conditional columns in the WHERE clause include all primary key columns, and the operators in the WHERE clause are `=` or `IN`, the query takes the shortcut. 

   1.  ::: note The conditional columns in the WHERE clause can include additional columns beyond all primary key columns. :::

   2. ```SQL
      SELECT * FROM users WHERE id=1;
      ```

   3.  Check the query plan to verify whether the query can use the shortcut. If the query plan includes `Short Circuit Scan: true`, the query can take the shortcut.

   4. ```SQL
      MySQL [example_db]> EXPLAIN SELECT * FROM users WHERE id=1;
      +---------------------------------------------------------+
      | Explain String                                          |
      +---------------------------------------------------------+
      | PLAN FRAGMENT 0                                         |
      |  OUTPUT EXPRS:1: id | 2: country | 3: city | 4: revenue |
      |   PARTITION: RANDOM                                     |
      |                                                         |
      |   RESULT SINK                                           |
      |                                                         |
      |   0:OlapScanNode                                        |
      |      TABLE: users                                       |
      |      PREAGGREGATION: OFF. Reason: null                  |
      |      PREDICATES: 1: id = 1                              |
      |      partitions=1/1                                     |
      |      rollup: users                                      |
      |      tabletRatio=1/6                                    |
      |      tabletList=10184                                   |
      |      cardinality=-1                                     |
      |      avgRowSize=0.0                                     |
      |      Short Circuit Scan: true                           | -- The query can use the shortcut.
      +---------------------------------------------------------+
      17 rows in set (0.00 sec)
      ```

### Use prepared statements

You can use the [prepared statements](../sql-reference/sql-statements/prepared_statement.md#use-prepared-statements) to query data in hybrid row/column-oriented tables.

```SQL
-- Prepare the statements for execution.
PREPARE select_all_stmt FROM 'SELECT * FROM users';
PREPARE select_by_id_stmt FROM 'SELECT * FROM users WHERE id = ?';

-- Declare variables in the statements.
SET @id1 = 1, @id2 = 2;

-- Use the declared variables to execute the statements.
-- Query data with ID 1 or 2 separately.
EXECUTE select_by_id_stmt USING @id1;
EXECUTE select_by_id_stmt USING @id2;
```

## Limits

- Currently, the hybrid row/column-oriented tables cannot be altered by using [ALTER TABLE]().
- The short path for queries is currently only suitable for queries after scheduled batch data loading. Because mutual exclusion of indexes may be incurred when the short path query happens at the apply stage of the data writing process, data writing may block short path queries, affecting the response time of point queries during data writing.
- Hybrid row/column-oriented tables may significantly increase storage consumption because storage overhead. This is because data is stored in both row and column formats, and the data compression ratio of row storage may not be as high as that of column storage.
- Hybrid row/column-oriented tables can increase the time and resource consumption during data loading.
- Hybrid row/column-oriented tables can be a viable solution to online services, but the performance of this type of table may not compete with mature OLTP databases.
- Hybrid row/column-oriented tables do not support features that rely on columnar storage, such as partial updates in column mode.
- Hybrid row/column-oriented tables need to be Primary Key tables.