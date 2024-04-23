---
displayed_sidebar: "Chinese"
---

# Data Cache 预热

本文介绍如何通过 Data Cache 预热 (Warmup）来提前将远端数据载入 Data Cache。

在数据湖分析过程中，有一些场景对查询有一定的性能要求，比如 BI 报表，性能测试 POC 等。可以提前将远端数据载入 Data Cache，避免查询时还需要从远端拉取数据，从而提供快速、稳定的查询性能。

Data Cache 预热和 [Data Cache](./data_cache.md) 特性的区别：

- Data Cache 是一个被动填充 cache 的过程，相当于在查询时，顺便把数据写入 cache，以便后续查询使用。
- Data Cache 预热是一个主动填充 cache 的过程，提前将想要查询的数据放到 cache 里，是基于 Data Cache 的扩展。

该特性从 3.3 版本开始支持。

## 实现方式

StarRocks 提供 `CACHE SELECT` 语法来实现 Data Cache 预热。使用 `CACHE SELECT` 之前，需要确保已经开启 Data Cache 特性。

`CACHE SELECT` 语法如下：

```sql
CACHE SELECT <column_name> [, ...]
FROM <catalog_name>.<db_name>.<table_name> [WHERE <boolean_expression>]
[PROPERTIES("verbose"="true")]
```
:::tip
上述语法是从 `default_catalog` 载入远端数据的语法。您也可以通过 `SET CATALOG <catalog_name>` 切换到目标 catalog 下，然后对目标表进行数据拉取。
:::

`CACHE_SELECT` 是一个同步过程，且一次只能对一个表进行预热。执行成功后，会返回一些 Cache 的指标。

以下示例载入外表 `customer` 的所有数据：

```sql
mysql> cache select * from customer;
+---------+---------------------+------------------+----------------------+-------------------+
| STATUS  | ALREADY_CACHED_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+---------+---------------------+------------------+----------------------+-------------------+
| SUCCESS | 15.2MB              | 754.7MB          | 88.7ms               | 29.04%            |
+---------+---------------------+------------------+----------------------+-------------------+
1 row in set (36.56 sec)
```

- `STATUS`：预热任务的执行结果。
- `ALREADY_CACHED_SIZE`：Data Cache 中已缓存的数据大小（目前这块统计存在一定误差，后续会改进）。
- `WRITE_CACHE_SIZE`：写入 Data Cache 的大小。
- `AVG_WRITE_CACHE_TIME`：每一个文件写入 Data Cache 的平均耗时。
- `TOTAL_CACHE_USAGE`：本次预热执行完成后 Data Cache 的空间使用率，可以根据这个指标评估 Data Cache 的空间是否充足。

您也可以通过指定谓词和分区进行更加细粒度的预热，以减少 Data Cache 的占用，比如下面这个 case：

```sql
mysql> cache select l_orderkey from lineitem where l_shipdate='1994-10-28';
+---------+---------------------+------------------+----------------------+-------------------+
| STATUS  | ALREADY_CACHED_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+---------+---------------------+------------------+----------------------+-------------------+
| SUCCESS | 1.3GB               | 15.1GB           | 12.2ms               | 75.81%            |
+---------+---------------------+------------------+----------------------+-------------------+
1 row in set (2 min 17.06 sec)
```

默认情况下，`CACHE SELECT` 返回的指标是一个合并后的指标，您可以在 `CACHE SELECT` 末尾添加 `PROPERTIES("verbose"="true")` 获得更加详细的指标。

```sql
mysql> cache select * from lineitem properties("verbose"="true");
+--------------+---------+---------------------+---------------------+------------------+----------------------+-------------------+
| BE_IP        | STATUS  | ALREADY_CACHED_SIZE | AVG_READ_CACHE_TIME | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+--------------+---------+---------------------+---------------------+------------------+----------------------+-------------------+
| 172.26.80.42 | SUCCESS | 115.4MB             | 461.4micros         | 5.2GB            | 1.2s                 | 16.35%            |
| 172.26.80.44 | SUCCESS | 106.5MB             | 2ms                 | 4.8GB            | 837ms                | 15.13%            |
| 172.26.80.43 | SUCCESS | 114.7MB             | 4.9ms               | 5.1GB            | 988.9ms              | 16.10%            |
+--------------+---------+---------------------+---------------------+------------------+----------------------+-------------------+
3 rows in set (42.87 sec)
```

verbose 模式下，会返回每个 BE 的详细预热情况。同时会多返回一个指标：

`AVG_READ_CACHE_TIME`：表示每一个文件在 Data Cache 里的平均查找耗时。

## CACHE SELECT 定时调度

CACHE SELECT 可以和 [SUBMIT TASK](../sql-reference/sql-statements/data-manipulation/SUBMIT_TASK.md) 结合，以实现周期性的预热。

比如下面这个例子，每隔 5 分钟对 `lineitem` 表进行一次预热：

```sql
mysql> submit task always_cache schedule every(interval 5 minute) as cache select l_orderkey
from lineitem
where l_shipdate='1994-10-28';
+--------------+-----------+
| TaskName     | Status    |
+--------------+-----------+
| always_cache | SUBMITTED |
+--------------+-----------+
1 row in set (0.03 sec)
```

## CACHE SELECT 任务管理

### 查看已经创建的任务

```sql
mysql> select * from default_catalog.information_schema.tasks;
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
| TASK_NAME    | CREATE_TIME         | SCHEDULE                                            | CATALOG       | DATABASE                     | DEFINITION                                                          | EXPIRE_TIME         | PROPERTIES |
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
| always_cache | 2024-04-11 16:01:00 | PERIODICAL START(2024-04-11T16:01) EVERY(5 MINUTES) | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | NULL                |            |
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
1 row in set (0.21 sec)
```

### 查看任务的执行历史

```sql
mysql> select * from default_catalog.information_schema.task_runs;
+--------------------------------------+--------------+---------------------+---------------------+---------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+---------------+----------+------------------------------------------------------------------------------------------------------------------------+------------+
| QUERY_ID                             | TASK_NAME    | CREATE_TIME         | FINISH_TIME         | STATE   | CATALOG       | DATABASE                     | DEFINITION                                                          | EXPIRE_TIME         | ERROR_CODE | ERROR_MESSAGE | PROGRESS | EXTRA_MESSAGE                                                                                                          | PROPERTIES |
+--------------------------------------+--------------+---------------------+---------------------+---------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+---------------+----------+------------------------------------------------------------------------------------------------------------------------+------------+
| 55b30204-f7da-11ee-b03e-7ea526d0b618 | always_cache | 2024-04-11 16:06:00 | 2024-04-11 16:07:22 | SUCCESS | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | 2024-04-12 16:06:00 |          0 | NULL          | 100%     | AlreadyCachedSize: 15.7GB, AvgReadCacheTime: 1ms, WriteCacheSize: 0B, AvgWriteCacheTime: 0s, TotalCacheUsage: 75.94%   |            |
| a2e3dc7e-f7d9-11ee-b03e-7ea526d0b618 | always_cache | 2024-04-11 16:01:00 | 2024-04-11 16:02:39 | SUCCESS | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | 2024-04-12 16:01:00 |          0 | NULL          | 100%     | AlreadyCachedSize: 15.7GB, AvgReadCacheTime: 1.2ms, WriteCacheSize: 0B, AvgWriteCacheTime: 0s, TotalCacheUsage: 75.87% |            |
+--------------------------------------+--------------+---------------------+---------------------+---------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+---------------+----------+------------------------------------------------------------------------------------------------------------------------+------------+
2 rows in set (0.04 sec)
```

`EXTRA_MESSAGE` 字段会记录 `CACHE SELECT` 的相关指标。

### 删除任务

```sql
DROP TASK <task_name>
```

## CACHE SELECT 最佳实践

1. 做 POC 性能测试时，如果想抛开外部存储系统的干扰，测试 StarRocks 的性能。可以通过 `CACHE_SELECT` 语句提前把待 POC 的表数据载入 Data Cache。

2. 业务方每天早上 8 点需要查看 BI 报表，希望那时候能够给业务方提供一个相对稳定的查询性能。

   可以提交一个周期性执行的 `CACHE SELECT`，指定每天早上 7 点开始执行。

   ```sql
   mysql> submit task BI schedule START('2024-02-03 07:00:00') EVERY(interval 1 day) AS cache select * from lineitem where l_shipdate='1994-10-28';
   +--------------+-----------+
   | TaskName     | Status    |
   +--------------+-----------+
   | BI           | SUBMITTED |
   +--------------+-----------+
   1 row in set (0.03 sec)
   ```

3. 希望预热时，不要过多的消耗系统资源。

   `SUBMIT TASK` 框架支持指定 Session Variable，你可以让 `CACHE SELECT` 任务在指定的资源组执行，也可以降低预热的 DOP。

   以下示例中，同时指定了预热的 DOP 和资源组，以此减少预热对系统正常查询的影响。

   ```sql
   mysql> submit task cache_select properties("pipeline_dop"="1", "resource_group"="warmup") schedule EVERY(interval 1 day) AS cache select * from lineitem;
   +--------------+-----------+
   | TaskName     | Status    |
   +--------------+-----------+
   | cache_select | SUBMITTED |
   +--------------+-----------+
   1 row in set (0.03 sec)
   ```

## 使用限制和说明

* 需要开启 Data Cache 特性，且拥有对目标 catalog/database/table 的 SELECT 权限。
* `CACHE SELECT` 支持存算分离和存算一体架构的外表查询，支持预热远端的 TEXT, ORC, Parquet 文件。
* `CACHE SELECT` 只支持对单表进行预热，不支持 `ORDER BY`，`LIMIT`，`GROUP BY` 等算子。
* 目前 `CACHE SELECT` 的实现是采用 `INSERT INTO BLACKHOLE()` 的方案，即按照正常的查询流程对表进行预热。所以 `CACHE SELECT` 的性能开销和普通查询的开销是差不多的。这一块后续会做出改进，提升 `CACHE SELECT` 的性能。
* `CACHE SELECT` 预热的数据不会保证一定不被淘汰，Data Cache 底层仍然按照 LRU 规则进行淘汰。用户可以自行通过 `SHOW BACKENDS\G` 查看 Data Cache 的剩余容量，以此判断是否会触发 LRU 淘汰。

## Data Cache 预热后续展望

后续 StarRocks 将会引入自适应的 Data Cache 预热，尽可能保证在用户日常查询的过程中，能有更高的缓存命中率。
