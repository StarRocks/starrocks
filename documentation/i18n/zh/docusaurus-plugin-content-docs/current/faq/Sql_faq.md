---
displayed_sidebar: "Chinese"
---

# 查询相关问题

## 构建物化视图失败：fail to allocate memory

修改 `be.conf` 中的`memory_limitation_per_thread_for_schema_change`。

该参数表示单个 schema change 任务允许占用的最大内存，默认大小 2G。修改完成后，需重启 BE 使配置生效。

## StarRocks 会缓存查询结果吗？

StarRocks 不直接缓存最终查询结果。从 2.5 版本开始，StarRocks 会将多阶段聚合查询的第一阶段聚合的中间结果缓存在 Query Cache 里，后续查询可以复用之前缓存的结果，加速计算。Query Cache 占用所在 BE 的内存。更多信息，参见 [Query Cache](../using_starrocks/query_cache.md)。
<!-- StarRocks 不直接缓存查询结果。而是使用 Page Cache 将原始数据分成 page 缓存在 BE 内存上，后续查询同一个 page 时，可以直接使用 cache 中的数据。2.4 版本及以后默认开启 Page Cache。您可以通过设置 `be.conf` 中的 `storage_page_cache_limit` 来限制 page cache 的大小，默认为系统内存的 20%。修改完成后，需重启 BE 使配置生效。-->

## 当字段为NULL时，除了is null， 其他所有的计算结果都是false

标准 SQL 中 null 和其他表达式计算结果都是null。

## [BIGINT 等值查询中加引号] 出现多余数据

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

**问题描述**

WHERE 里使用 BIGINT 类型，查询加单引号，查出很多无关数据。

**解决方案**

字符串和 INT 比较，相当于 CAST 成 DOUBLE。INT 比较时，不要加引号。加了引号，还会导致无法命中索引。

## StarRocks有decode函数吗？

StarRocks 不支持 Oracle 中的 decode 函数，StarRocks 语法兼容 MySQL，可以使用case when。

## StarRocks的主键覆盖是立刻生效的吗？还是说要等后台慢慢合并数据?

StarRocks 的后台合并参考 Google 的 MESA 模型，有两层 compaction，会后台策略触发合并。如果没有合并完成，查询时会合并，但是读出来只会有一个最新的版本，不存在「导入后数据读不到最新版本」的情况。

## StarRocks 存储 utf8mb4 的字符，会不会被截断或者乱码？

MySQL的“utf8mb4”是标准的“UTF-8”，StarRocks 可以完全兼容。

## [Schema change] alter table 时显示：table's state is not normal

ALTER TABLE 是异步操作，如果之前有 ALTER TABLE 操作还没完成，可以通过如下语句查看 ALTER 状态。

```sql
show tablet from lineitem where State="ALTER"; 
```

执行时间与数据量大小有关系，一般是分钟级别，建议 ALTER 过程中停止数据导入，导入会降低 ALTER 速度。

## [Hive外部表查询问题] 查询 Hive 外部表时报错获取分区失败

**问题描述**

查询 Hive 外部表时具体报错信息：
`get partition detail failed: com.starrocks.common.DdlException: get hive partition meta data failed: java.net.UnknownHostException:hadooptest（具体hdfs-ha的名字）`

**解决方案**

将`core-site.xml`和`hdfs-site.xml`文件拷贝到 `fe/conf` 和 `be/conf`中即可，然后重启 FE 和 BE。

**问题原因**

获取配置单元分区元数据失败。

## 大表查询结果慢，没有谓词下推

多张大表关联时，旧 planner有时没有自动谓词下推，比如：

```sql
A JOIN B ON A.col1=B.col1 JOIN C on B.col1=C.col1 where A.col1='北京'，
```

可以更改为：

```sql
A JOIN B ON A.col1=B.col1 JOIN C on A.col1=C.col1 where A.col1='北京'，
```

或者升级到较新版本并开启 CBO 功能，会有此类谓词下推操作，优化查询性能。

## 查询报错 planner use long time 3000 remaining task num 1

**解决方案**

查看`fe.gc`日志确认该问题是否是多并发引起的full gc。

如果查看后台监控和日志初步判断有频繁gc，可参考两个方案：

  1. 让sqlclient同时访问多个FE去做负载均衡。
  2. 修改`fe.conf`中 `jvm8g` 为16g（更大内存，减少 full gc 影响）。修改后需重启FE。

## 当A基数很小时，select B from tbl order by A limit 10查询结果每次不一样

**解决方案：**

使用`select B from tbl order by A,B limit 10` ，将B也进行排序就能保证结果一致。

**问题原因**

上面的SQL只能保证A是有序的，并不能保证每次查询出来的B顺序是一致的，MySQL能保证这点因为它是单机数据库，而StarRocks是分布式数据库，底层表数据存储是sharding的。A的数据分布在多台机器上，每次查询多台机器返回的顺序可能不同，就会导致每次B顺序不一致。

## select * 和 select 的列效率差距过大

select * 和 select 时具体列效率差距会很大，这时应该去排查profile，看 MERGE 的具体信息。

* 确认是否是存储层聚合消耗的时间过长。
* 确认是否指标列有很多，需要对几百万行的几百列进行聚合。

```plain text
MERGE:
    - aggr: 26s270ms
    - sort: 15s551ms
```

## 目前 DELETE 中不支持嵌套函数

目前不支持类似如下的嵌套：`DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;`。这里'to_days(now())'属于嵌套。

## 如果一个数据库中有上百张表，use database 会特别慢

client连接的时候加上`-A`参数，比如 `mysql -uroot -h127.0.0.1 -P8867 -A`。`-A`不会预读数据库信息，切换database会很快。

## BE 和 FE 日志文件太多，怎么处理？

调整日志级别和参数大小，详情参考 log 相关的参数默认值和作用说明：[参数配置](/administration/Configuration.md)。

## 更改副本数失败：table lineorder is colocate table, cannot change replicationNum

colocate table 是有 分组 (group) 的。一个组包含多个表，不支持修改单个表的副本数。需要把group内的所有表的`group_with`属性设置成空，然后给所有表设置`replication_num`，再把所有表的`group_with`属性设置回去。

## varchar 设置成最大值对存储有没有影响

VARCHAR 是变长存储，存储跟数据实际长度有关，建表时指定不同的 VARCHAR 长度对同一数据的查询性能影响很小。

## truncate table 失败，报错create partititon timeout

目前 TRUNCATE 会先创建对应空分区再swap，如果存在大量创建分区任务，积压就会超时，compaction过程中会持锁很长时间，也导致建表拿不到锁。如果集群导入比较多，设置`be.conf`中参数`tablet_map_shard_size=512`，可以降低锁冲突。修改参数后需重启 FE。

## Hive 外表访问出错，Failed to specify server's Kerberos principal name

在`fe.conf` 和`be.conf`的`hdfs-site.xml`里添加如下信息：

```plain text
<property>
<name>dfs.namenode.kerberos.principal.pattern</name>
<value>*</value>
</property>
```

## 2021-10在StarRocks里是合法的日期格式吗？可以用作分区字段吗？

不是合法的日期格式，不可以用作分区字段，需要调整成 `2021-10-01` 再分区。

## StarRocks on ES，创建 Elasticsearch 外表时，如果相关字符串长度过长，超过 256，同时 Elasticsearch 使用动态mapping， 那么使用select语句将会导致无法查询到该列

动态mapping 时 Elasticsearch 的数据类型为

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

StarRocks 使用 keyword 数据类型对该查询语句进行转换。因为该列的数据 keyword 长度超过 256，所以无法查询该列。

解决方案：去除该字段映射中的

```json
            "fields": {
                   "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                   }
                }
```

让其使用 text 类型即可。

## 如何快速统计 StarRocks 库、表的大小，所占的磁盘资源？

查看库、表的存储大小可以用 [SHOW DATA](../sql-reference/sql-statements/data-manipulation/SHOW_DATA.md) 命令查看。

`SHOW DATA;` 可以展示当前数据库下所有表的数据量和副本数。

`SHOW DATA FROM <db_name>.<table_name>;` 可以展示指定数据库下某个表的数据量、副本数和统计行数。
