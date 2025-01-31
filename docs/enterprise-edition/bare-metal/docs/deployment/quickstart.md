---
sidebar_position: 60
---
# QuickStart

This QuickStart tutorial guides you through the procedures to insert some test data into your CelerData cluster, and query the data.

<!--CeleryData employs the catalog system to manage the data within a CelerData cluster and the data from an external data source. Data loaded into a CelerData cluster is managed in the default catalog. You can create an external catalog to access an external data source, such as Apache Hive™, Apache Iceberg, or Apache Hudi, and query the data within. See [Catalog overview](../data_source/catalog/catalog_overview.md) for more instructions.-->

## Step 1: Create a database and a table

After you deploy and connect to your CelerData cluster with a MySQL client, you can proceed to create a database and then a table into which you can import the test data.

1. Execute the following SQL statement to create a database named `celerdata`:

   ```SQL
   CREATE DATABASE IF NOT EXISTS celerdata;
   ```

2. Switch to the database you have created, and create a table named `example` in it.

   ```SQL
   USE celerdata;
   CREATE TABLE IF NOT EXISTS example (
       celerdata_id     INT,
       name             STRING,
       city_code        INT,
       reg_date         DATE,
       verified         BOOLEAN
   )
   DISTRIBUTED BY HASH(city_code);
   ```

> **NOTE**
>
> - To create a table in a CelerData cluster, you MUST strategize the data distribution plan of the table by specifying the `DISTRIBUTED BY HASH` clause. By default, the data is distributed to 10 tablets.<!-- For more detailed instructions, see [Data Distribution]().-->
>
> - By default, THREE data replicas are created for a table in a CelerData cluster. Each replica resides on a distinct BE node. Therefore, if you have created more or less BE nodes in your cluster, you need to specify the table property `replication_num` by adding `PROPERTIES("replication_num" = "<be_node_count>")` at the end of the statement. For more detailed instructions, see [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).
>
> - If no [table type](../table_design/table_types/table_types.md) is specified, the table is created based on Duplicate Key table by default. For more information, see [Duplicate Key table](../table_design/table_types/duplicate_key_table.md).
>
> - **To guarantee the cluster's high performance in production, we strongly recommend that you strategize a DYNAMIC data partitioning plan for the table using the `PARTITION BY` clause.**<!-- For more detailed instructions, see [Design partitioning and bucketing rules]().-->

## Step 2: Load data using INSERT

You can load test data with the familiar SQL statement - [INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md). The fields of test data can exactly map onto the columns in the table `example`.

Execute the following SQL statement to load the test data into the table `example`:

```SQL
INSERT INTO example
WITH LABEL test_insert
VALUES
    (001,"tom",100000,"2022-03-13",true),
    (002,"johndoe",210000,"2022-03-14",false),
    (003,"maruko",200000,"2022-03-14",true),
    (004,"ronaldo",100000,"2022-03-15",false),
    (005,"pavlov",210000,"2022-03-16",false),
    (006,"mohammed",300000,"2022-03-17",true);
```

If the loading transaction succeeds, the following message is returned:

```Plain
Query OK, 6 rows affected (0.04 sec)
{'label':'test_insert', 'status':'VISIBLE', 'txnId':'5'}
```

> **NOTE**
>
> Loading data via INSERT INTO VALUES merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment.

## Step 3: Run queries

With the test data loaded into CelerData, you can then run some queries on the data.

> **NOTE**
>
> CelerData is compatible with SQL-92.

1. Run a simple query to list all rows in the table `example`:

   ```SQL
   SELECT * FROM example;
   ```

   The returned results are as follows:

   ```Plain
   +--------------+----------+-----------+------------+----------+
   | celerdata_id | name     | city_code | reg_date   | verified |
   +--------------+----------+-----------+------------+----------+
   |            1 | tom      |    100000 | 2022-03-13 |        1 |
   |            4 | ronaldo  |    100000 | 2022-03-15 |        0 |
   |            2 | johndoe  |    210000 | 2022-03-14 |        0 |
   |            5 | pavlov   |    210000 | 2022-03-16 |        0 |
   |            6 | mohammed |    300000 | 2022-03-17 |        1 |
   |            3 | maruko   |    200000 | 2022-03-14 |        1 |
   +--------------+----------+-----------+------------+----------+
   6 rows in set (0.00 sec)
   ```

2. Run a standard query with a specified condition:

   ```SQL
   SELECT celerdata_id, name 
   FROM example
   WHERE reg_date <= "2022-03-14";
   ```

   The returned results are as follows:

   ```Plain
   +--------------+---------+
   | celerdata_id | name    |
   +--------------+---------+
   |            3 | maruko  |
   |            1 | tom     |
   |            2 | johndoe |
   +--------------+---------+
   3 rows in set (0.01 sec)
   ```

## What's next

In addition to the features this tutorial has demonstrated, CelerData also supports:

- A variety of [data types](../sql-reference/data-types/numeric/BIGINT.md)
- Multiple [table types](../table_design/table_types/table_types.md)
- Flexible partitioning strategies
- Classic database query indexes, including [bitmap index](../table_design/indexes/Bitmap_index.md) and [bloom filter index](../table_design/indexes/Bloomfilter_index.md)
- [Automatic Materialized view](../using_starrocks/auto_materialized_view.md)
