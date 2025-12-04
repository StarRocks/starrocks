---
displayed_sidebar: docs
---

# SQL query

This topic provides answers to some frequently asked questions about SQL.

## This error "fail to allocate memory." when I build a materialized view

To solve this problem, increase the value of the `memory_limitation_per_thread_for_schema_change` parameter in the **be.conf** file. This parameter refers to the maximum storage that can be allocated for a single task to change the scheme. The default value of the maximum storage is 2 GB.

## Does StarRocks support caching query results?

StarRocks does not directly cache final query results. From v2.5 onwards, StarRocks uses the Query Cache feature to save the intermediate results of first-stage aggregation in the cache. New queries that are semantically equivalent to previous queries can reuse the cached computation results to accelerate computations. Query cache uses BE memory. For more information, see [Query cache](../using_starrocks/caching/query_cache.md).

## When a `Null` is included in the calculation, the calculation results of functions are false except for the ISNULL() function

In standard SQL, every calculation that includes an operand with a `NULL` value returns a `NULL`.

## Does StarRocks support the DECODE function?

StarRocks does not support the DECODE function of the Oracle database. StarRocks is compatible with MySQL, so you can use the CASE WHEN statement.

## Can the latest data be queried immediately after data is loaded into the Primary Key table of StarRocks?

Yes. StarRocks merges data in a way that references Google Mesa. In StarRocks, a BE triggers the data merge and it has two kinds of compaction to merge data. If the data merge is not completed, it is finished during your query. Therefore, you can read the latest data after data loading.

## Do the utf8mb4 characters stored in StarRocks get truncated or appear garbled?

No.

## This error "table's state is not normal" occurs when I run the `alter table` command

This error occurs because the previous alteration has not been completed. You can run the following code to check the status of the previous alteration:

```SQL
show tablet from lineitem where State="ALTER"; 
```

The time spent on the alteration operation relates to the data volume. In general, the alteration can be completed in minutes. We recommend that you stop loading data into StarRocks while you are altering tables because data loading lowers the speed at which alteration completes.

## This error "get hive partition meta data failed: java.net.UnknownHostException:hadooptest" occurs when I query the external tables of Apache Hive

This error occurs when the metadata of Apache Hive partitions cannot be obtained. To solve this problem, copy **core-sit.xml** and **hdfs-site.xml** to the **fe.conf** file and the **be.conf** file.

## This error "planner use long time 3000 remaining task num 1" occurs when I query data

This error occurs usually due to a full garbage collection (full GC), which can be checked by using backend monitoring and the **fe.gc** log. To solve this problem, perform one of the following operations:

- Allows SQL's client to access multiple frontends (FEs) simultaneously to spread the load.
- Change the heap size of Java Virtual Machine (JVM) from 8 GB to 16 GB in the **fe.conf** file to increase memory and reduce the impact of full GC.

## When cardinality of column A is small, the query results of `select B from tbl order by A limit 10` vary each time

SQL can only guarantee that column A is ordered, and it cannot guarantee that the order of column B is the same for each query. MySQL can guarantee the order of column A and column B because it is a standalone database.

StarRocks is a distributed database, of which data stored in the underlying table is in a sharding pattern. The data of column A is distributed across multiple machines, so the order of column B returned by multiple machines may be different for each query, resulting in inconsistent order of B each time. To solve this problem, change `select B from tbl order by A limit 10` to `select B from tbl order by A,B limit 10`.

## Why is there a large gap in column efficiency between SELECT * and SELECT?

To solve this problem, check the profile and see MERGE details:

- Check whether the aggregation on the storage layer takes up too much time.

- Check whether there are too many indicator columns. If so, aggregate hundreds of columns of millions of rows.

```plaintext
MERGE:

    - aggr: 26s270ms

    - sort: 15s551ms
```

## Does DELETE support nested functions?

Nested functions are not supported, such as `to_days(now())` in `DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;`.

## How to improve the usage efficiency of a database when there are hundreds of tables in it?

To improve efficiency, add the `-A` parameter when you connect to MySQL's client server: `mysql -uroot -h127.0.0.1 -P8867 -A`. MySQL's client server does not pre-read database information.

## How to reduce the disk space occupied by the BE log and the FE log?

Adjust the log level and corresponding parameters. For more information, see [Parameter Configuration](../administration/management/BE_configuration.md).

## This error "table *** is colocate table, cannot change replicationNum" occurs when I modify the replication number

When you create colocated tables, you need to set the `group` property. Therefore, you cannot modify the replication number for a single table. You can perform the following steps to modify the replication number for all tables in a group:

1. Set `group_with` to `empty` for all tables in a group.
2. Set a proper `replication_num` for all tables in a group.
3. Set `group_with` back to its original value.

## Does setting VARCHAR to the maximum value affect storage?

VARCHAR is a variable-length data type, which has a specified length that can be changed based on the actual data length. Specifying a different varchar length when you create a table has little impact on the query performance on the same data.

## This error "create partititon timeout" occurs when I truncate a table

To truncate a table, you need to create the corresponding partitions and then swap them. If there are a larger number of partitions that need to be created, this error occurs. In addition, if there are many data load tasks, the lock will be held for a long time during the compaction process. Therefore, the lock cannot be acquired when you create tables. If there are too many data load tasks, set `tablet_map_shard_size` to `512` in the **be.conf** file to reduce the lock contention.

## This error "Failed to specify server's Kerberos principal name" occurs when I access external tables of Apache Hive

Add the following information to **hdfs-site.xml** in the **fe.conf** file and the **be.conf** file:

```HTML
<property>

<name>dfs.namenode.kerberos.principal.pattern</name>

<value>*</value>

</property>
```

## Is "2021-10" a date format in StarRocks?

No.

## Can "2021-10" be used as a partition field?

No, use functions to change "2021-10" to "2021-10-01" and then use "2021-10-01" as a partition field.

## Where can I query the size of a StarRocks database or table?

You can use the [SHOW DATA](../sql-reference/sql-statements/Database/SHOW_DATA.md) command.

`SHOW DATA;` displays the data size and replicas of all tables in the current database.

`SHOW DATA FROM <db_name>.<table_name>;` displays the data size, number of replicas, and number of rows in a specified table of a specified database.

## In StarRocks on ES, when creating an Elasticsearch external table, if the relevant string length is too long, exceeding 256, and Elasticsearch uses dynamic mapping, using a select statement will result in the inability to query that column

In dynamic mapping, Elasticsearch's data type is

```json
          "k4": {
                "type": "text",
                "fields": {
                   "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                   }
                }
             }
```

StarRocks uses the keyword data type to convert the query statement. Since the keyword length of the column exceeds 256, the column cannot be queried.

Solution: Remove the field mapping

```json
            "fields": {
                   "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                   }
                }
```

to use the text type instead.

## How to quickly count the size of StarRocks databases and tables and the disk resources they occupy?

You can use the [SHOW DATA](../sql-reference/sql-statements/Database/SHOW_DATA.md) command to view the storage size of databases and tables.

`SHOW DATA;` displays the data volume and replica count of all tables in the current database.

`SHOW DATA FROM <db_name>.<table_name>;` displays the data volume, replica count, and row count of a specific table in a specified database.

## Why does using a function on a partition key slow down queries?

Using a function on a partition key can lead to inaccurate partition pruning, thereby reducing query performance.

## Why doesn't the DELETE statement support nested functions?

```SQL
mysql > DELETE FROM starrocks.ods_sale_branch WHERE create_time >= concat(substr(202201,1,4),'01') and create_time <= concat(substr(202301,1,4),'12');

SQL Error [1064][42000]: Right expr of binary predicate should be value
```

BINARY predicates must be of the `column op literal` type and cannot be expressions. There are currently no plans to support expressions as comparison values.

## How to name columns with reserved keywords?

Reserved keywords (e.g., `rank`) need to be escaped, such as using `` `rank` ``.

## How to stop an executing SQL?

You can use `show processlist;` to view executing SQL and use `kill <id>;` to terminate the corresponding SQL. You can also view and manage through `SHOW PROC '/current_queries';`.

## How to clean up idle connections?

You can control the timeout for idle connections through the session variable `wait_timeout` (unit: seconds). MySQL automatically cleans up idle connections after about 8 hours by default.

## Are multiple SQL segments in UNION ALL executed in parallel?

Yes, they are executed in parallel.

## What should be done if a SQL causes BE to crash?

1. Based on the `be.out` error stack, find the `query_id` that caused the crash.
2. Find the corresponding SQL in `fe.audit.log` using the `query_id`.

Please collect and send the following information to the support team:

- `be.out` log
- Run `pstack $be_pid > pstack.log` to execute SQL.
- Core Dump file

Steps to collect Core files:

1. Get the corresponding BE process:

   ```Bash
   ps aux| grep be
   ```

2. Set the Core file size limit to unlimited.

   ```Bash
   prlimit -p $bePID --core=unlimited:unlimited
   ```

   Verify if the size limit is unlimited.

   ```Bash
   cat /proc/$bePID/limits
   ```

If it is not `0`, the system will generate a Core file in the root directory of the BE deployment when the process crashes.

## How to use Hints to control table join optimizer behavior?

Supports `broadcast` and `shuffle` Hints. For example:

- `select * from a join [broadcast] b on a.id = b.id;`
- `select * from a join [shuffle] b on a.id = b.id;`

## How to increase SQL query concurrency?

By adjusting the session variable `pipeline_dop`.

## How to check the execution progress of DDL?

- View all column modification tasks in the default database:

   ```SQL
   SHOW ALTER TABLE COLUMN;
   ```

- View the most recent column modification task for a specific table:

   ```SQL
   SHOW ALTER TABLE COLUMN WHERE TableName="table1" ORDER BY CreateTime DESC LIMIT 1;
   ```

## Why does comparing floating-point numbers sometimes result in inconsistent query results?

Directly using floating-point numbers `=` for comparison can lead to instability due to errors. It is recommended to use range checks.

## Why does floating-point calculation result in errors?

FLOAT/DOUBLE types have precision errors in `avg`, `sum`, and other calculations, leading to potentially inconsistent query results. For high precision, use the DECIMAL type, but note that performance will decrease by 2-3 times.

## Why does ORDER BY in a subquery not take effect?

In distributed execution, if ORDER BY is not specified in the outer layer of the subquery, global ordering cannot be guaranteed. This is expected behavior.

## Why is the result of row_number() inconsistent across multiple executions?

If the ORDER BY field has duplicates (e.g., multiple rows with the same `createTime`), SQL standards do not guarantee stable sorting. It is recommended to include a unique field (e.g., `employee_id`) in the ORDER BY to ensure stability.

## What information is needed for SQL optimization or troubleshooting?

- `EXPLAIN COSTS <SQL>` (includes statistics)
- `EXPLAIN VERBOSE <SQL>` (includes data types, nullable, optimization strategies)
- Query Profile (viewable through the FE Web interface at `http://<fe_ip>:<fe_http_port>` and navigating to the Queries Tab)
- Query Dump (obtained via HTTP API)

  ```Bash
  wget --user=${username} --password=${password} --post-file ${query_file} http://${fe_host}:${fe_http_port}/api/query_dump?db=${database} -O ${dump_file}
  ```

Query Dump includes the following information:

- Query statement
- Table schema referenced in the query
- Session variables
- Number of BEs
- Statistics (Min, Max values)
- Exception information (exception stack)

## How to check data skew?

Use `ADMIN SHOW REPLICA DISTRIBUTION FROM <table>` to view the distribution of tablets.

## How to troubleshoot memory-related errors?

There are three common scenarios:

- **Single query memory limit exceeded:**
  - Error: `Mem usage has exceed the limit of single query, You can change the limit by set session variable exec_mem_limit.`
  - Solution: Adjust `exec_mem_limit`
- **Query pool memory limit exceeded:**
  - Error: `Mem usage has exceed the limit of query pool`
  - Solution: Optimize the SQL.
- **BE total memory limit exceeded:**
  - Error: `Mem usage has exceed the limit of BE`
  - Solution: Analyze memory usage.

Memory analysis methods:

```Bash
curl -XGET -s http://BE_IP:BE_HTTP_PORT/metrics | grep "^starrocks_be_.*_mem_bytes\|^starrocks_be_tcmalloc_bytes_in_use"
curl -XGET -s http://BE_IP:BE_HTTP_PORT/mem_tracker
```

---

## What to do when encountering the error `StarRocks planner use long time xxx ms in logical phase`?

1. Analyze `fe.gc.log` to check for Full GC occurrences.
2. If the SQL execution plan is complex, increase `new_planner_optimize_timeout` (unit: ms):

   ```SQL
   set global new_planner_optimize_timeout = 6000;
   ```

## How to troubleshoot Unknown Error?

Try adjusting the following parameters one by one and then re-execute the SQL:

```SQL
set disable_join_reorder = true;
set enable_global_runtime_filter = false;
set enable_query_cache = false;
set cbo_enable_low_cardinality_optimize = false;
```

Then collect EXPLAIN COSTS, EXPLAIN VERBOSE, PROFILE, and Query Dump, and provide them to the support team.

## What time zone does `select now()` return?

It returns the time zone specified by the `time_zone` system variable. FE/BE logs use the machine's local time zone.

## Why does SQL slow down under high concurrency even when resources are normal?

The reason is high network or RPC latency. You can adjust the BE parameter `brpc_connection_type` to `pooled` and then restart BE.

## How to disable statistics collection?

- Disable automatic collection:

  ```SQL
  enable_statistic_collect = false;
  ```

- Disable import-triggered collection:

  ```SQL
  enable_statistic_collect_on_first_load = false;
  ```

- For versions upgraded to v3.3 and above, manually set:

  ```SQL
  set global analyze_mv = "";
  ```