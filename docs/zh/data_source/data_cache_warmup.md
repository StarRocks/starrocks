---
displayed_sidebar: docs
---

# Data Cache 预热

本文介绍如何通过 Data Cache 预热 (Warmup) 来提前将远端数据载入 Data Cache。

## 功能介绍

在数据湖分析以及存算分离场景下，有一些场景对查询有一定的性能要求，比如 BI 报表，性能测试 POC 等。缓存预热功能可以提前将远端数据载入 Data Cache，避免查询时还需要从远端拉取数据，从而提供快速、稳定的查询性能。

Data Cache 预热和 [Data Cache](./data_cache.md) 特性的区别：

- Data Cache 是一个被动填充 cache 的过程，相当于在查询时通过同步或异步的方式把数据写入 cache，以便后续查询使用。
- Data Cache 预热是一个主动填充 cache 的过程，提前将想要查询的数据放到 cache 里，是基于 Data Cache 的扩展。

该特性从 3.3 版本开始支持。

## 适用场景

1. 缓存盘的容量远大于待预热的数据量。如果磁盘容量小于待预热的数据量，可能达不到预期的预热效果。比如预热需要加载 100 GB 数据，而缓存盘只有 50 GB 空间，那么只能预热 50 GB，而且预热加载的后 50 GB 数据会淘汰前 50 GB，达不到预期的预热效果。
2. 缓存盘的数据访问比较稳定。如果预热过程中突发大量数据访问，也会影响预热效果。比如预热需要加载 100 GB 数据，如果缓存盘有 200 GB 空间，虽然满足了条件 1，但如果预热过程中有大量新数据写入（150 GB），或者突然有另一个大冷查询需要加载 150 GB 数据，那么可能会将要预热的数据淘汰，达不到预期的预热效果。

## 使用方式

StarRocks 提供 `CACHE SELECT` 语法来实现 Data Cache 预热。使用 `CACHE SELECT` 之前，需要确保已经开启 Data Cache 特性。

`CACHE SELECT` 语法如下：

```sql
CACHE SELECT <column_name> [, ...]
FROM [<catalog_name>.][<db_name>.]<table_name> [WHERE <boolean_expression>]
[PROPERTIES("verbose"="true")]
```

参数说明：

- `column_name`：需要缓存的列，可以用 `*` 来表示表中所有列。
- `catalog_name`：（仅在数据湖外表分析时使用，存算分离内表不需要）External Catalog 名称。如果已经通过 SET CATALOG 切换到 External Catalog 下，也可以不填。
- `db_name`：数据库名称。如果已经切换到对应数据库下，也可以不填。
- `table_name`：表名称。
- `boolean_expression`: WHERE 中指定的过滤条件。
- `PROPERTIES`：当前仅支持设置 `verbose` 属性，用于返回详细的预热指标。

`CACHE SELECT` 是一个同步过程，且一次只能对一个表进行预热。执行成功后，会返回缓存预热相关的指标。

### 预热远端表所有数据

以下示例拉取外表 `lineitem` 中的所有数据：

```plaintext
mysql> cache select * from hive_catalog.test_db.lineitem;
+-----------------+------------------+----------------------+-------------------+
| READ_CACHE_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+-----------------+------------------+----------------------+-------------------+
| 48.2MB          | 3.7GB            | 59ms                 | 96.83%            |
+-----------------+------------------+----------------------+-------------------+
1 row in set (19.56 sec)
```

返回字段说明：

- `READ_CACHE_SIZE`：所有节点累计从 Data Cache 中读取的数据大小。
- `WRITE_CACHE_SIZE`：所有节点累计写入 Data Cache 的数据大小。
- `AVG_WRITE_CACHE_TIME`：每一个节点将远端数据写入 Data Cache 的平均耗时。
- `TOTAL_CACHE_USAGE`：本次预热执行完成后整个集群 Data Cache 的空间使用率，可以根据这个指标评估 Data Cache 的空间是否充足。

### 根据过滤条件预热指定列

在实际使用中，建议通过指定列名和谓词进行更细粒度的预热，以减少缓存预热过程中拉取无需预热的数据，降低磁盘 IO 以及 CPU 的消耗。举例：

```plaintext
mysql> cache select l_orderkey from hive_catalog.test_db.lineitem where l_shipdate='1994-10-28';
+-----------------+------------------+----------------------+-------------------+
| READ_CACHE_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+-----------------+------------------+----------------------+-------------------+
| 957MB           | 713.5MB          | 3.6ms                | 97.33%            |
+-----------------+------------------+----------------------+-------------------+
1 row in set (9.07 sec)
```

以下示例拉取存算分离内表 `lineorder` 中的部分列：

```plaintext
mysql> cache select lo_orderkey from ssb.lineorder;
+-----------------+------------------+----------------------+-------------------+
| READ_CACHE_SIZE | WRITE_CACHE_SIZE | AVG_WRITE_CACHE_TIME | TOTAL_CACHE_USAGE |
+-----------------+------------------+----------------------+-------------------+
| 118MB           | 558.9MB          | 200.6ms              | 4.66%             |
+-----------------+------------------+----------------------+-------------------+
1 row in set (29.88 sec)
```

### verbose 模式预热

默认情况下，`CACHE SELECT` 返回的指标是一个合并后的指标，您可以在 `CACHE SELECT` 末尾添加 `PROPERTIES("verbose"="true")` 来获取各个 BE 上详细的指标。

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

`verbose` 模式下，会返回每个 BE 的详细预热情况。同时会多返回一个指标：

`AVG_READ_CACHE_TIME`：表示每一个节点在 Data Cache 命中下的平均读取耗时。

## CACHE SELECT 定时调度

CACHE SELECT 可以和 [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) 结合，实现周期性的预热。

每隔 5 分钟对 `lineitem` 表进行一次预热：

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

## CACHE SELECT 任务管理

### 查看已经创建的任务

```plaintext
mysql> select * from default_catalog.information_schema.tasks;
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
| TASK_NAME    | CREATE_TIME         | SCHEDULE                                            | CATALOG       | DATABASE                     | DEFINITION                                                          | EXPIRE_TIME         | PROPERTIES |
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
| always_cache | 2024-04-11 16:01:00 | PERIODICAL START(2024-04-11T16:01) EVERY(5 MINUTES) | emr_hive_test | zz_tpch_sf1000_hive_orc_zlib | cache select l_orderkey from lineitem where l_shipdate='1994-10-28' | NULL                |            |
+--------------+---------------------+-----------------------------------------------------+---------------+------------------------------+---------------------------------------------------------------------+---------------------+------------+
1 row in set (0.21 sec)
```

### 查看任务的执行历史

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

`EXTRA_MESSAGE` 字段会记录 `CACHE SELECT` 的相关指标。

### 删除任务

```sql
DROP TASK <task_name>
```

## CACHE SELECT 最佳实践

1. 做 POC 性能测试时，如果想抛开外部存储系统的干扰，测试 StarRocks 的性能。可以通过 `CACHE SELECT` 语句提前把待 POC 的表数据载入 Data Cache。

2. 业务方每天早上 8 点需要查看 BI 报表，希望那时候能够给业务方提供一个相对稳定的查询性能。

   可以提交一个周期性执行的 `CACHE SELECT`，指定每天早上 7 点开始执行。

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

3. 希望预热时，不要过多的消耗系统资源。

   `SUBMIT TASK` 框架支持指定 Session Variable，你可以让 `CACHE SELECT` 任务在指定的资源组执行，也可以降低预热的 DOP。

   以下示例中，同时指定了预热的 DOP 和资源组，并且添加了 WHERE 谓词进行裁剪，以此减少预热对系统正常查询的影响。

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

## 使用限制和说明

- 需要开启 Data Cache 特性，且拥有对目标表的 SELECT 权限。
- `CACHE SELECT` 只支持对单表进行预热，不支持 `ORDER BY`，`LIMIT`，`GROUP BY` 等算子。
- `CACHE SELECT` 支持存算分离和存算一体架构的外表查询，支持预热远端的 TEXT, ORC, Parquet 文件。
- `CACHE SELECT` 预热的数据也可能会被淘汰，Data Cache 底层仍然按照 LRU 规则进行淘汰。
  - 数据湖用户可以通过 `SHOW BACKENDS\G` 或 `SHOW COMPUTE NODES\G` 查看 Data Cache 的剩余容量，以此判断是否会触发 LRU 淘汰。
  - 存算分离用户可以通过存算分离的监控指标查看 Data Cache 的使用容量，以此判断是否会触发 LRU 淘汰。
- 目前 `CACHE SELECT` 的实现采用 `INSERT INTO BLACKHOLE()` 方案，即按照正常的查询流程对表进行预热。所以 `CACHE SELECT` 的性能开销和普通查询的开销差不多。后续会继续改进，提升 `CACHE SELECT` 性能。

## Data Cache 预热后续展望

后续 StarRocks 将会引入自适应的 Data Cache 预热，保证更高的缓存命中率。
