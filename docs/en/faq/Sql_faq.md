# SQL

This topic provides answers to some frequently asked questions about SQL.

## This error "fail to allocate memory." when I build a materialized view

To solve this problem, increase the value of the `memory_limitation_per_thread_for_schema_change` parameter in the **be.conf** file. This parameter refers to the maximum storage that can be allocated for a single task to change the scheme. The default value of the maximum storage is 2 GB.

## Does StarRocks support caching query results?

StarRocks does not directly cache final query results. StarRocks uses Page Cache to cache original data from data sources on BE memory. This allows subsequent queries to reuse the cached data and accelerates queries. Page Cache is enabled by default from v2.4. You can specify the size of the page cache by setting the `storage_page_cache_limit` parameter in the `be.conf` file. The default size of the page cache is 20% of the system memory. After you set this parameter, restart BEs for the settings to take effect.

## When a `Null` is included in the calculation, the calculation results of functions are false except for the ISNULL() function

In standard SQL, every calculation that includes an operand with a `NULL` value returns a `NULL`.

## Why is the query result incorrect after I enclose quotation marks around a value of the BIGINT data type for an equivalence query?

### Problem description

See the following examples:

```Plain_Text
select cust_id,idno 

from llyt_dev.dwd_mbr_custinfo_dd 

where Pt= ‘2021-06-30’ 

and cust_id = ‘20210129005809043707’ 

limit 10 offset 0;
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
select cust_id,idno 

from llyt_dev.dwd_mbr_custinfo_dd 

where Pt= ‘2021-06-30’ 

and cust_id = 20210129005809043707 

limit 10 offset 0;
+---------------------+-----------------------------------------+

|   cust_id           |      idno                               |

+---------------------+-----------------------------------------+

|  20210189979989976  | xuywehuhfuhruehfurhghcfCNMX39dw=        |

+---------------------+-----------------------------------------+
```

### Solution

When you compare the STRING data type and the INTEGER data type, the fields of these two types are cast to the DOUBLE data type. Therefore, quotation marks cannot be added. Otherwise, the condition defined in the WHERE clause cannot be indexed.

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

## This error "get partition detail failed: org.apache.doris.common.DdlException: get hive partition meta data failed: java.net.UnknownHostException:hadooptest" occurs when I query the external tables of Apache Hive

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

```Plain_Text
MERGE:

    - aggr: 26s270ms

    - sort: 15s551ms
```

## Does DELETE support nested functions?

Nested functions are not supported, such as `to_days(now())` in `DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;`.

## How to improve the usage efficiency of a database when there are hundreds of tables in it?

To improve efficiency, add the `-A` parameter when you connect to MySQL's client server: `mysql -uroot -h127.0.0.1 -P8867 -A`. MySQL's client server does not pre-read database information.

## How to reduce the disk space occupied by the BE log and the FE log?

Adjust the log level and corresponding parameters. For more information, see [Parameter Configuration](../administration/Configuration.md).

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

You can use the [SHOW DATA](../sql-reference/sql-statements/data-manipulation/SHOW_DATA.md) command.

`SHOW DATA;` displays the data size and replicas of all tables in the current database.

`SHOW DATA FROM <db_name>.<table_name>;` displays the data size, number of replicas, and number of rows in a specified table of a specified database.
