---
displayed_sidebar: docs
---

# Data cache warmup

Some data lake analytics and shared-data cluster scenarios have high performance requirements for queries, such as BI reports and proof of concept (PoC) performance testing. Loading remote data into local data cache can avoid the need to fetch the same data multiple times, significantly speeding up query execution and minimizing resource usage.

StarRocks v3.3 introduces the Data Cache Warmup feature, which is an enhancement to [Data Cache](./data_cache.md). Data Cache is a process of passively populating the cache, in which data is written to the cache during data querying. Data Cache Warmup, however, is an active process of populating the cache. It proactively fetches the desired data from remote storage in advance.

## Scenarios

- The disk used for data cache has a storage capacity much larger than the amount of data to warm up. If the disk capacity is less than the data to warm up, the expected warmup effect cannot be achieved. For example, if 100 GB data needs to be warmed up but the disk has only 50 GB of space, then only 50 GB data can be loaded to the cache and the previously loaded 50 GB data will be replaced by the 50 GB data that is loaded later.
- Data access on the disk used for data cache is relatively stable. If there is a surge in the access volume, the expected warmup effect cannot be achieved. For example, if 100 GB data needs to be warmed up and the disk has 200 GB of space, then the first condition is met. However, if a large amount of new data (150 GB) is written to the cache during the warmup process, or if an unexpected large cold query needs to load 150 GB data to the cache, it may result in the eviction of the warmed data.

## How it works

StarRocks provides the CACHE SELECT syntax to implement Data Cache Warmup. Before using CACHE SELECT, make sure that the Data Cache feature has been enabled.

Syntax of CACHE SELECT:

```sql
CACHE SELECT <column_name> [, ...]
FROM [<catalog_name>.][<db_name>.]<table_name> [WHERE <boolean_expression>]
[PROPERTIES("verbose"="true")]
```

Parameters:

- `column_name`: The columns to fetch. You can use `*` to fetch all columns in the external table.
- `catalog_name`: The name of the external catalog, required only when querying external tables in data lakes. If you have switched to the external catalog using SET CATALOG, it can be left unspecified.
- `db_name`: The name of the database. If you have switched to that database, it can be left unspecified.
- `table_name`: The name of the table from which to fetch data.
- `boolean_expression`: The filter condition.
- `PROPERTIES`: Currently, only the `verbose` property is supported. It is used to return detailed warmup metrics.

CACHE SELECT is a synchronous process and it can warm up only one table at a time. Upon successful execution, it will return warmup-related metrics.

### Warm up all data in an external table

The following example loads all data from external table `lineitem`:

```plaintext
mysql> cache select * from hive_catalog.test_db.lineitem;
+-----------------+------------------+----------------------+-------------------+
| READ_CACHE_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+-----------------+------------------+----------------------+-------------------+
| 48.2MB          | 3.7GB            | 59ms                 | 96.83%            |
+-----------------+------------------+----------------------+-------------------+
1 row in set (19.56 sec)
```

Return fields:

- `READ_CACHE_SIZE`: The total size of data read from the data cache by all nodes.
- `WRITE_CACHE_SIZE`: The total size of data written to the data cache by all nodes.
- `AVG_WRITE_CACHE_TIME`: The average time taken by each node to write data to the data cache.
- `TOTAL_CACHE_USAGE`: The space usage of the data cache of the entire cluster after this warmup task is complete. This metric can be used to assess whether the data cache has sufficient space.

### Warm up specified columns with filter conditions

You can specify columns and predicates to achieve fine-grained warmup, which helps reduce the amount to data to warm up, reducing disk I/O and CPU consumption.

```plaintext
mysql> cache select l_orderkey from hive_catalog.test_db.lineitem where l_shipdate='1994-10-28';
+-----------------+------------------+----------------------+-------------------+
| READ_CACHE_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+-----------------+------------------+----------------------+-------------------+
| 957MB           | 713.5MB          | 3.6ms                | 97.33%            |
+-----------------+------------------+----------------------+-------------------+
1 row in set (9.07 sec)
```

The following example prefetches a specific column from a cloud-native table `lineorder` in a shared-data cluster:

```plaintext
mysql> cache select lo_orderkey from ssb.lineorder;
+-----------------+------------------+----------------------+-------------------+
| READ_CACHE_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+-----------------+------------------+----------------------+-------------------+
| 118MB           | 558.9MB          | 200.6ms              | 4.66%             |
+-----------------+------------------+----------------------+-------------------+
1 row in set (29.88 sec)
```

### Warm up in verbose mode

By default, the metrics returned by `CACHE SELECT` are metrics combined on multiple BEs. You can append `PROPERTIES("verbose"="true")` at the end of CACHE SELECT to obtain detailed metrics of each BE.

```plaintext
mysql> cache select * from hive_catalog.test_db.lineitem properties("verbose"="true");
+---------------+-----------------+---------------------+------------------+----------------------+-------------------+
| IP            | READ_CACHE_SIZE | AVG_READ_CACHE_TIME | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+---------------+-----------------+---------------------+------------------+----------------------+-------------------+
| 172.26.80.233 | 376MB           | 127.8micros         | 0B               | 0s                   | 3.85%             |
| 172.26.80.231 | 272.5MB         | 121.8micros         | 20.7MB           | 146.5micros          | 3.91%             |
| 172.26.80.232 | 355.5MB         | 147.7micros         | 0B               | 0s                   | 3.91%             |
+---------------+-----------------+---------------------+------------------+----------------------+-------------------+
3 rows in set (0.54 sec)
```

In verbose mode,  an extra metric will be returned:

- `AVG_READ_CACHE_TIME`: the average time for each node to read data when data cache is hit.

## Periodic scheduling of CACHE SELECT tasks

You can use CACHE SELECT with [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) to achieve periodic warmup. For example, the following case warms up the `lineitem` table every 5 minutes:

```plaintext
mysql> submit task always_cache schedule every(interval 5 minute) as cache select l_orderkey
from hive_catalog.test_db.lineitem
where l_shipdate='1994-10-28';
+--------------+-----------+
| TaskName     | Status    |
+--------------+-----------+
| always_cache | SUBMITTED |
+--------------+-----------+
1 row in set (0.03 sec)
```

### Manage CACHE SELECT tasks

#### View created tasks

```plaintext
mysql> select * from default_catalog.information_schema.tasks;
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
| TASK_NAME    | CREATE_TIME         | SCHEDULE                                            | CATALOG       | DATABASE                     | DEFINITION                                                          | EXPIRE_TIME         | PROPERTIES |
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
| always_cache | 2024-04-11 16:01:00 | PERIODICAL START(2024-04-11T16:01) EVERY(5 MINUTES) | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | NULL                |            |
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
1 row in set (0.21 sec)
```

#### View task execution history

```plaintext
mysql> select * from default_catalog.information_schema.task_runs;
+--------------------------------------+--------------+---------------------+---------------------+---------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+---------------+----------+------------------------------------------------------------------------------------------------------------------------+------------+
| QUERY_ID                             | TASK_NAME    | CREATE_TIME         | FINISH_TIME         | STATE   | CATALOG       | DATABASE                     | DEFINITION                                                          | EXPIRE_TIME         | ERROR_CODE | ERROR_MESSAGE | PROGRESS | EXTRA_MESSAGE                                                                                                          | PROPERTIES |
+--------------------------------------+--------------+---------------------+---------------------+---------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+---------------+----------+------------------------------------------------------------------------------------------------------------------------+------------+
| 55b30204-f7da-11ee-b03e-7ea526d0b618 | always_cache | 2024-04-11 16:06:00 | 2024-04-11 16:07:22 | SUCCESS | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | 2024-04-12 16:06:00 |          0 | NULL          | 100%     | AlreadyCachedSize: 15.7GB, AvgReadCacheTime: 1ms, WriteCacheSize: 0B, AvgWriteCacheTime: 0s, TotalCacheUsage: 75.94%   |            |
| a2e3dc7e-f7d9-11ee-b03e-7ea526d0b618 | always_cache | 2024-04-11 16:01:00 | 2024-04-11 16:02:39 | SUCCESS | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | 2024-04-12 16:01:00 |          0 | NULL          | 100%     | AlreadyCachedSize: 15.7GB, AvgReadCacheTime: 1.2ms, WriteCacheSize: 0B, AvgWriteCacheTime: 0s, TotalCacheUsage: 75.87% |            |
+--------------------------------------+--------------+---------------------+---------------------+---------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+---------------+----------+------------------------------------------------------------------------------------------------------------------------+------------+
2 rows in set (0.04 sec)
```

The `EXTRA_MESSAGE` field records metrics of CACHE SELECT.

#### Drop tasks

```sql
DROP TASK <task_name>
```

## Use cases

1. During PoC performance testing, if you want to assess StarRocks' performance without interference from external storage systems, you can use the CACHE SELECT statement to load the data of the table to test into the data cache in advance.

2. The business team need to view BI reports at 8 a.m. every morning. To ensure a relatively stable query performance, you can schedule a CACHE SELECT task to start running at 7 a.m. each day.

   ```sql
   mysql> submit task BI schedule START('2024-02-03 07:00:00') EVERY(interval 1 day)
   AS cache select * from hive_catalog.test_db.lineitem
   where l_shipdate='1994-10-28';
   +--------------+-----------+
   | TaskName     | Status    |
   +--------------+-----------+
   | BI           | SUBMITTED |
   +--------------+-----------+
   1 row in set (0.03 sec)
   ```

3. To minimize system resource consumption during warmup, you can specify session variables in the SUBMIT TASK statement. For example, you can designate a resource group for the CACHE SELECT task, adjust the Degree of Parallelism (DOP), and specify the filter condition in WHERE to reduce the impact of warmup on regular queries.

   ```sql
   mysql> submit task cache_select properties("pipeline_dop"="1", "resource_group"="warmup") schedule EVERY(interval 1 day)
   AS cache select * from hive_catalog.test_db.lineitem where l_shipdate>='1994-10-28';
   +--------------+-----------+
   | TaskName     | Status    |
   +--------------+-----------+
   | cache_select | SUBMITTED |
   +--------------+-----------+
   1 row in set (0.03 sec)
   ```

## Limits and usage notes

- To use CACHE SELECT, you must first enable the Data Cache feature and have the SELECT privilege on the destination table.
- CACHE SELECT supports warming up only a single table and does not support operators like ORDER BY, LIMIT, or GROUP BY.
- CACHE SELECT can be used in both shared-nothing and shared-data clusters.
- CACHE SELECT can warm up remote TEXT, ORC, Parquet files.
- The data warmed up by CACHE SELECT may not be retained in cache forever. The cached data may still be evicted based on the LRU rule of the Data Cache feature.
  - If you are a data lake user, you can check the remaining capacity of the data cache by using `SHOW BACKENDS\G` or `SHOW COMPUTE NODES\G` to assess whether LRU eviction may occur.
  - If you are a shared-data cluster user, you can check the data cache usage by viewing the metrics of the shared-data cluster.
- Currently, the implementation of CACHE SELECT uses the INSERT INTO BLACKHOLE() approach, which warms up the table following the normal query process. Therefore, the performance overhead of CACHE SELECT is similar to that of regular queries. Improvements will be made in the future to enhance the performance.

## What to expect in later versions

In the future, StarRocks will introduce adaptive Data Cache Warmup to ensure a higher cache hit rate.
