---
displayed_sidebar: "English"
---

# Create a table

This QuickStart tutorial will walk you through the necessary steps to create a table in StarRocks, and introduce some basic features of StarRocks.

After the StarRocks instance is deployed (see [Deploy StarRocks](../quick_start/deploy_with_docker.md) for details), you need to create a database and a table to [load and query data](../quick_start/Import_and_query.md). Creating databases and tables requires corresponding [user privilege](../administration/User_privilege.md). In this QuickStart tutorial, you can perform the following steps with the default `root` user, which has the highest privileges on the StarRocks instance.

> **NOTE**
>
> You can complete this tutorial by using an existing StarRocks instance, database, table, and user privilege. However, for simplicity, we recommend that you use the schema and data the tutorial provides.

## Step 1: Log in to StarRocks

Log in to StarRocks via your MySQL client. You can log in with the default user `root`, and the password is empty by default.

```Plain
mysql -h <fe_ip> -P<fe_query_port> -uroot
```

> **NOTE**
>
> - Change the `-P` value accordingly if you have assigned a different FE MySQL server port (`query_port`, Default: `9030`).
> - Change the `-h` value accordingly if you have specified the configuration item `priority_networks` in the FE configuration file.

## Step 2: Create a database

Create a database named `sr_hub` by referring to [CREATE DATABASE](../sql-reference/sql-statements/data-definition/CREATE_DATABASE.md).

```SQL
CREATE DATABASE IF NOT EXISTS sr_hub;
```

You can view all databases in this StarRocks instance by executing [SHOW DATABASES](../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) SQL.

## Step 3: Create a table

Run `USE sr_hub` to switch to the `sr_hub` database and create a table named `sr_member` by referring to [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md).

```SQL
USE sr_hub;
CREATE TABLE IF NOT EXISTS sr_member (
    sr_id            INT,
    name             STRING,
    city_code        INT,
    reg_date         DATE,
    verified         BOOLEAN
)
PARTITION BY RANGE(reg_date)
(
    PARTITION p1 VALUES [('2022-03-13'), ('2022-03-14')),
    PARTITION p2 VALUES [('2022-03-14'), ('2022-03-15')),
    PARTITION p3 VALUES [('2022-03-15'), ('2022-03-16')),
    PARTITION p4 VALUES [('2022-03-16'), ('2022-03-17')),
    PARTITION p5 VALUES [('2022-03-17'), ('2022-03-18'))
)
DISTRIBUTED BY HASH(city_code);
```

> **NOTE**
>
> - To create a table in StarRocks, you MUST specify a bucket key in the `DISTRIBUTED BY HASH` clause to strategize the data distribution plan of the table. By default, the data is distributed to 10 tablets. See [Data Distribution](../table_design/Data_distribution.md#data-distribution) for more information.
> - You need to specify the table property `replication_num`, which represents the number of data replicas, as `1` because the StarRocks instance you deployed has only one BE node.
> - If no [table type](../table_design/table_types/table_types.md) is specified, a Duplicate Key table is created by default. See [Duplicate Key table](../table_design/table_types/duplicate_key_table.md)
> - The columns of the table exactly correspond to the fields of data that you will be loading into StarRocks in the tutorial on [loading and querying data](../quick_start/Import_and_query.md).
> - To guarantee the high performance **in the production environment**, we strongly recommend that you strategize the data partitioning plan for the table by using the `PARTITION BY` clause. See [Design partitioning and bucketing rules](../table_design/Data_distribution.md#design-partitioning-and-bucketing-rules) for more instructions.

After the table is created, you can check the details of the table using the DESC statement, and view all the tables in the database by executing [SHOW TABLES](../sql-reference/sql-statements/data-manipulation/SHOW_TABLES.md). Tables in StarRocks support schema changes. You can see [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) for more information.

## What to do next

To learn more about conceptual details of StarRocks tables, see [StarRocks Table Design](../table_design/StarRocks_table_design.md).

In addition to the features this tutorial has demonstrated, StarRocks also supports:

- A variety of [data types](../sql-reference/sql-statements/data-types/BIGINT.md)
- Multiple [table types](../table_design/table_types/table_types.md)
- Flexible [partitioning strategies](../table_design/Data_distribution.md#dynamic-partition-management)
- Classic database query indexes, including [bitmap index](../using_starrocks/Bitmap_index.md) and [bloom filter index](../using_starrocks/Bloomfilter_index.md)
- [Materialized view](../using_starrocks/Materialized_view.md)
