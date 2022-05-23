# SQL

## Fail to create materialized view: fail to allocate memory

Modify memory_limitation_per_thread_for_schema_change in be.conf.

This parameter represents the maximum memory allowed for a single schema change task. The default value is 2G.

## Does StarRocks limit the result cache?

No, StarRocks does not cache results. The speed of query getting slower every time is because the subsequent queries uses pagecache of the operation system.

Pagecache size can be limited by setting the parameter storage_page_cache_limit in be.conf. The default value is 20G.

## When the field is NULL, all computing results are false except is null

Computing results of null and all other expressions in the standard sql are null.

## [Add apostrophes for bigint in Equi-Join Query ]Unwanted data appear

```sql
select cust_id,idno 
from llyt_dev.dwd_mbr_custinfo_dd 
where Pt= ‘2021-06-30’ 
and cust_id = ‘20210129005809043707’ 
limit 10 offset 0;
```

```plain text
+---------------------+-----------------------------------------+
|   cust_id           |      idno                               |
+---------------------+-----------------------------------------+
|  20210129005809436  | yjdgjwsnfmdhjw294F93kmHCNMX39dw=        |
|  20210129005809436  | sdhnswjwijeifme3kmHCNMX39gfgrdw=        |
|  20210129005809436  | Tjoedk3js82nswndrf43X39hbggggbw=        |
|  20210129005809436  | denuwjaxh73e39592jwshbnjdi22ogw=        |
|  20210129005809436  | ckxwmsd2mei3nrunjrihj93dm3ijin2=        |
|  20210129005809436  | djm2emdi3mfi3mfu4jro2ji2ndimi3n=        |
+---------------------+-----------------------------------------+
```

```sql
select cust_id,idno 
from llyt_dev.dwd_mbr_custinfo_dd 
where Pt= ‘2021-06-30’ 
and cust_id = 20210129005809043707 
limit 10 offset 0;
```

```plain text
+---------------------+-----------------------------------------+
|   cust_id           |      idno                               |
+---------------------+-----------------------------------------+
|  20210189979989976  | xuywehuhfuhruehfurhghcfCNMX39dw=        |
+---------------------+-----------------------------------------+
```

**Issue description:**

A lot of irrelevant data is found after adding apostrophes in queries of bigint type in where.

**Solution:**

When comparing strings with int, it is equivalent to case int to double. While comparing integers, please do not add apostrophes because adding apostrophes would result in inaccurate selection of the index.

## Does StarRocks have the decode function?

No, it currently does not support the decode function from Oracle. StarRocks syntax is compatible with MySQL and can use the case when function.

## Does primary key replacement in StarRocks take effect immediately? Or does it need to wait after the backend has merged data?

The backend of StarRocks was merged in reference to Google's mesa model. There are two levels of compaction which will trigger merging based on backend strategy. If merging is not completed, it will continue during queries. Once merging is completed, only the latest version will be saved, thus avoiding the situation where the latest version cannot be read after data is imported.

## Will the utd8mb4 string saved in StarRocks be truncated or garbled?

No, it won't because "utf8mb4" in MySQL is the real "UTF-8".

## [Schema change] When performing alter table, it reports: table's state is not normal

Alter table is asynchronous. If the previous alter table is not completed, you can view its status through:

```sql
show tablet from lineitem where State="ALTER"; 
```

The execution time is affected by the data volume. The time unit is usually minutes. It is recommended that users stop data import during alter, as it will slow the alter speed.

## [query issue related to hive external table] query on hive external table reports an error: failure to partition

**Issue description:**

The detailed error report is as follows: get partition detail failed: org.apache.doris.common.DdlException: get hive partition meta data failed: java.net.UnknownHostException:hadooptest（the name of specific hdfs-ha）

**Solution:**

Please copy and paste the core-site.xml and hdfs-site.xml files to fe/conf and be/conf respectively.

**Cause:**

Failure to obtain metadata in the partitions of configuration unit.

## Slow queries on large tables. Predicate pushdown is not performed

When multiple large tables are connected, the older version of planner sometimes does not perform predicate pushdown, for example:

```sql
A JION B ON A.col1=B.col1 JOIN C on B.col1=C.col1 where A.col1='newyork' ，
```

This could be revised as the following:

```sql
A JION B ON A.col1=B.col1 JOIN C on A.col1=C.col1 where A.col1='newyork'，
```

Or, you can update it to the latest version and restart CBO. After this, predicate pushdown will be performed to optimize query performance.

## Query reports an error: Doris planner use long time 3000 remaining task num 1

**Solution:**

View fe.gc log to see if full gc issue is caused by multiple concurrent tasks.

If the backend monitor and fe.gc log indicate that GC is indeed becoming very frequent, there are two solutions to resolve it:

1. SQLclient could be used to access multiple fe to achieve load balance.
2. Change jvm8g to 16g in fe.conf. (Maximum memory could alleviate full gc impact.)

## When the cardinal A is small, each query result of  select B from tbl order by A limit 10 is different

**Solution:**

`select B from tbl order by A,B limit 10`, and query results would become identical after also adding B in the ordering process.

**Cause：**

The SQL above can only make sure that A is in a clear sequence, with B being still disordered. This is because MySQL is a device database while StarRocks is a distributed database with data storage is sharded in the bottom tables. Data related to A is stored in multiple devices. Thus, each query may send back results in different sequence, which makes B disordered.

## Execution efficiency between select * and select are of too great difference

When there is an exceptional difference in the execution efficiency between these two, we need to verify their profiles and view detailed information about MERGE.

- Please verify if it is because the aggregation of storage tiers is too time-consuming.
- Please verify if it is because there are too many sorting columns waiting to aggregate hundreds of columns and millions of rows.

```plain text
MERGE:
    - aggr: 26s270ms
    - sort: 15s551ms
```

## The current delete does not support Nested Function

A nested function like this is not currently supported: DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;`to_days(now())

## When there are over a hundred tables in a database, use database becomes too slow

`mysql -uroot -h127.0.0.1 -P8867 -A`. When connecting the client, please add -A parameter. This will not preread the database information, thus making it fast to change among databases.

## What should be done when there are too many log files in be/fe?

You can modify the log level and the parameter size. For more detail, please refer to Parameter configuration( [参数配置](/administration/Configuration.md)) for explanations on default parameter value and effect related to log.
