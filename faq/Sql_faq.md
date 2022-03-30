# 查询相关问题

## 构建物化视图失败：fail to allocate memory

修改 be.conf 中的`memory_limitation_per_thread_for_schema_change`。

该参数表示单个schema change任务允许占用的最大内存，默认大小2G。

## StarRocks对结果缓存这块有限制吗？

starrocks不会对结果缓存，第一次查询慢后面快的原因是因为后续的查询使用了操作系统的 pagecache。

pagecache大小可以通过设置 be.conf 中`storage_page_cache_limit`参数来限制pagecache，默认20G。

## 当字段为NULL时，除了is null， 其他所有的计算结果都是false

标准sql中null和其他表达式计算结果都是null。

## [bigint等值查询中加引号]出现多余数据

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

**问题描述：**

where里bigint类型，查询加单引号，查出很多无关数据。

**解决方案：**

字符串和int比较，相当于 cast 成double。int比较时，不要加引号。同时，加了引号，还会导致没法命中索引。

## StarRocks有decode函数吗？

不支持Oracle中的decode函数，StarRocks语法兼容MySQL，可以使用case when。

## StarRocks的主键覆盖是立刻生效的吗？还是说要等后台慢慢合并数据?

StarRocks的后台合并就是参考google的mesa的模型，有两层compaction，会后台策略触发合并。如果没有合并完成，查询的时候会合并，但是读出来只会有一个最新的版本，不存在「导入后数据读不到最新版本」的情况。

## StarRocks存储utf8mb4的字符 会不会被截断或者乱码？

MySQL的“utf8mb4”是真正的“UTF-8”，所以StarRocks是没问题的

## [Schema change] alter table 时显示：table's state is not normal

Alter table 是异步的，之前有过alter table 操作还没完成，可以通过

```sql
show tablet from lineitem where State="ALTER"; 
```

查看alter状态，执行时间与数据量大小有关系，一般是分钟级别，建议alter过程中停止数据导入。导入会降低alter 速度。

## [hive外部表查询问题] 查询hive外部表的时候报错获取分区失败

**问题描述：**

查询hive外部表是具体报错信息为：
`get partition detail failed: com.starrocks.common.DdlException: get hive partition meta data failed: java.net.UnknownHostException:hadooptest（具体hdfs-ha的名字）`

**解决方案:**

把core-site.xml和hdfs-site.xml文件拷贝到 fe/conf 和 be/conf中即可。

**问题原因：**

获取配置单元分区元数据失败。

## 大表查询结果慢，没有谓词下推

多张大表关联时，旧planner有时没有自动谓词下推，比如：

```sql
A JOIN B ON A.col1=B.col1 JOIN C on B.col1=C.col1 where A.col1='北京' ，
```

可以更改为：

```sql
A JOIN B ON A.col1=B.col1 JOIN C on A.col1=C.col1 where A.col1='北京'，
```

或者升级较新版本并开启CBO，然后会有此类谓词下推操作，优化查询性能。

## 查询报错 planner use long time 3000 remaining task num 1

**解决方案：**

查看fe.gc日志看是否是多并发引起的full gc问题。

如果查看后台监控和日志初步判断有频繁gc，两个方案：

  1. 可以让sqlclient去同时访问多个fe去做负载均衡；
  2. 修改fe.conf中jvm8g为16g（更大内存，减少 full gc 影响）；

## 当A基数很小时，select B from tbl order by A limit 10查询结果每次不一样

**解决方案：**

`select B from tbl order by A,B limit 10` ，将B也进行排序就能保证结果一致。

**问题原因：**

上面的SQL只能保证A是有序的，并不能保证每次查询出来的B顺序是一致的，MySQL能保证这点因为它是单机数据库，而StarRocks是分布式数据库，底层表数据存储是sharding的，A的数据分布在多台机器上，每次查询多台机器返回的顺序可能不同，就会导致每次B顺序不一致。

## select * 和select 具体列效率差距过大

select * 和select 时具体列效率差距会很大，这时我们应该去排查profile，看MERGE的具体信息。

* 确认是否是存储层聚合消耗的时间过长。
* 确认是否指标列有很多，需要对几百万行的几百列进行聚合。

```plain text
MERGE:
    - aggr: 26s270ms
    - sort: 15s551ms
```

## 目前delete中不支持嵌套函数

类似这种：`DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;`to_days(now())这个嵌套了，目前不支持。

## 在一个 database 中有上百张表后，use database 会特别慢

`mysql -uroot -h127.0.0.1 -P8867 -A`，client连接的时候加上-A参数，-A不会预读数据库信息，切换database会很快。

## be/fe日志文件太多，怎么处理？

调整日志级别和参数大小，详情参考文档中log相关的参数默认值和作用说明：[参数配置](/administration/Configuration.md)

## 更改副本数失败：table lineorder is colocate table, cannot change replicationNum

colocate table是有group的，一个组包含多个表，不支持单个表修改副本数，现在的话，需要把group内的所有表的group_with属性设置成空，然后给所有的表设置一下replication_num，然后再把所有表的group_with属性设置回去。

## varchar 设置成最大值对存储有没有影响

varchar是变长存储，存储跟数据实际长度有关，建表时指定不同的varchar长度对同一数据的查询性能影响很小

## truncate table 失败，报错create partititon timeout

目前truncate会先创建对应空分区再swap，如果存在大量创建分区任务，积压就会超时，compaction过程中会持锁很长时间，也导致建表拿不到锁，如果集群导入比较多，设置be.conf中参数tablet_map_shard_size=512，可以降低锁冲突

## hive外表访问出错，Failed to specify server's Kerberos principal name

在fe/be conf的hdfs-site.xml里加如下信息：

```plain text
<property>
<name>dfs.namenode.kerberos.principal.pattern</name>
<value>*</value>
</property>
```

## 2021-10这种在starrocks里算日期格式么？可以用作分区字段么

不可以，函数补足成2021-10-01这种再分区吧
