---
displayed_sidebar: "English"
---

# Load and query data

This QuickStart tutorial will lead you step by step in loading data into the table you created (see [Create a table](../quick_start/Create_table.md) for more instruction), and running a query on the data.

StarRocks supports loading data from a rich wealth of data sources, including some major cloud services, local files, or a streaming data system. You can see [Ingestion Overview](../loading/Loading_intro.md) for more information. The following steps will show you how to insert data into StarRocks by using the INSERT INTO statement, and run queries on the data.

> **NOTE**
>
> You can complete this tutorial by using an existing StarRocks instance, database, table, user, and your own data. However, for simplicity, we recommend that you use the schema and data the tutorial provides.

## Step 1: Load data with INSERT

You can insert additional rows of data using INSERT. See [INSERT](../sql-reference/sql-statements/data-manipulation/insert.md) for detailed instruction.

Log in to StarRocks via your MySQL client, and execute the following statements to insert the following rows of data into the `sr_member` table you have created.

```SQL
use sr_hub
INSERT INTO sr_member
WITH LABEL insertDemo
VALUES
    (001,"tom",100000,"2022-03-13",true),
    (002,"johndoe",210000,"2022-03-14",false),
    (003,"maruko",200000,"2022-03-14",true),
    (004,"ronaldo",100000,"2022-03-15",false),
    (005,"pavlov",210000,"2022-03-16",false),
    (006,"mohammed",300000,"2022-03-17",true);
```

If the loading transaction succeeds, the following message will be returned.

```Plain
Query OK, 6 rows affected (0.07 sec)
{'label':'insertDemo', 'status':'VISIBLE', 'txnId':'5'}
```

> **NOTE**
>
> Loading data via INSERT INTO VALUES merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment. To load mass data into StarRocks, see [Ingestion Overview](../loading/Loading_intro.md) for other options that suit your scenarios.

## Step 2: Query the data

StarRocks is compatible with SQL-92.

- Run a simple query to list all rows of data in the table.

  ```SQL
  SELECT * FROM sr_member;
  ```

  The returned results are as follows:

  ```Plain
  +-------+----------+-----------+------------+----------+
  | sr_id | name     | city_code | reg_date   | verified |
  +-------+----------+-----------+------------+----------+
  |     3 | maruko   |    200000 | 2022-03-14 |        1 |
  |     1 | tom      |    100000 | 2022-03-13 |        1 |
  |     4 | ronaldo  |    100000 | 2022-03-15 |        0 |
  |     6 | mohammed |    300000 | 2022-03-17 |        1 |
  |     5 | pavlov   |    210000 | 2022-03-16 |        0 |
  |     2 | johndoe  |    210000 | 2022-03-14 |        0 |
  +-------+----------+-----------+------------+----------+
  6 rows in set (0.05 sec)
  ```

- Run a standard query with a specified condition.

  ```SQL
  SELECT sr_id, name 
  FROM sr_member
  WHERE reg_date <= "2022-03-14";
  ```

  The returned results are as follows:

  ```Plain
  +-------+----------+
  | sr_id | name     |
  +-------+----------+
  |     1 | tom      |
  |     3 | maruko   |
  |     2 | johndoe  |
  +-------+----------+
  3 rows in set (0.01 sec)
  ```

- Run a query on a specified partition.

  ```SQL
  SELECT sr_id, name 
  FROM sr_member 
  PARTITION (p2);
  ```

  The returned results are as follows:

  ```Plain
  +-------+---------+
  | sr_id | name    |
  +-------+---------+
  |     3 | maruko  |
  |     2 | johndoe |
  +-------+---------+
  2 rows in set (0.01 sec)
  ```

## What to do next

To learn more about the data ingestion methods of StarRocks, see [Ingestion Overview](../loading/Loading_intro.md). In addition to a huge number of built-in functions, StarRocks also supports [Java UDFs](../sql-reference/sql-functions/JAVA_UDF.md), which allows you to create your own data processing functions that suit your business scenarios.

You can also learn how to:

- Perform [ETL when loading](../loading/Etl_in_loading.md).
- Create an [external table](../data_source/External_table.md) to access external data sources.
- [Analyze the query plan](../administration/Query_planning.md) to learn how to optimize the query performance.
