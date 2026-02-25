---
displayed_sidebar: docs
---

# 查询常见问题

## 构建物化视图失败：fail to allocate memory

修改 `be.conf` 中的`memory_limitation_per_thread_for_schema_change`。

该参数表示单个 schema change 任务允许占用的最大内存，默认大小 2G。修改完成后，需重启 BE 使配置生效。

## StarRocks 会缓存查询结果吗？

StarRocks 不直接缓存最终查询结果。从 2.5 版本开始，StarRocks 会将多阶段聚合查询的第一阶段聚合的中间结果缓存在 Query Cache 里，后续查询可以复用之前缓存的结果，加速计算。Query Cache 占用所在 BE 的内存。更多信息，参见 [Query Cache](../using_starrocks/caching/query_cache.md)。
<!-- StarRocks 不直接缓存查询结果。而是使用 Page Cache 将原始数据分成 page 缓存在 BE 内存上，后续查询同一个 page 时，可以直接使用 cache 中的数据。2.4 版本及以后默认开启 Page Cache。您可以通过设置 `be.conf` 中的 `storage_page_cache_limit` 来限制 page cache 的大小，默认为系统内存的 20%。修改完成后，需重启 BE 使配置生效。-->

## 当字段为NULL时，除了is null， 其他所有的计算结果都是false

标准 SQL 中 null 和其他表达式计算结果都是null。

## StarRocks有decode函数吗？

StarRocks 不支持 Oracle 中的 decode 函数，StarRocks 语法兼容 MySQL，可以使用case when。

## StarRocks的主键覆盖是立刻生效的吗？还是说要等后台慢慢合并数据?

StarRocks 的后台合并参考 Google 的 MESA 模型，有两层 compaction，会后台策略触发合并。如果没有合并完成，查询时会合并，但是读出来只会有一个最新的版本，不存在「导入后数据读不到最新版本」的情况。

## StarRocks 存储 utf8mb4 的字符，会不会被截断或者乱码？

MySQL的 utf8mb4 是标准的 UTF-8，StarRocks 可以完全兼容。

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

- 确认是否是存储层聚合消耗的时间过长。
- 确认是否指标列有很多，需要对几百万行的几百列进行聚合。

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

调整日志级别和参数大小，详情参考 log 相关的参数默认值和作用说明：[参数配置](../administration/management/FE_configuration.md)。

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

查看库、表的存储大小可以用 [SHOW DATA](../sql-reference/sql-statements/Database/SHOW_DATA.md) 命令查看。

`SHOW DATA;` 可以展示当前数据库下所有表的数据量和副本数。

`SHOW DATA FROM <db_name>.<table_name>;` 可以展示指定数据库下某个表的数据量、副本数和统计行数。

## 为什么在分区键上使用函数会导致查询变慢？

分区键使用函数会导致分区裁剪不准确，从而降低查询性能。

## 为什么 DELETE 语句不支持嵌套函数？

```SQL
mysql > DELETE FROM starrocks.ods_sale_branch WHERE create_time >= concat(substr(202201,1,4),'01') and create_time <= concat(substr(202301,1,4),'12');

SQL Error [1064][42000]: Right expr of binary predicate should be value
```

BINARY 谓词必须是 `column op literal` 类型，不能是表达式。目前没有支持表达式作为比较值的计划。

## 如何使用保留关键字命名列？

需要对保留关键字（例如 `rank`）进行转义，如使用 `` `rank` ``。

## 如何停止正在执行的 SQL？

可以使用 `show processlist;` 查看执行中的 SQL，并使用 `kill <id>;` 终止对应 SQL。也可以通过 `SHOW PROC '/current_queries';` 查看与管理。

## 如何清理 Idle 连接？

可通过会话变量 `wait_timeout` 控制空闲连接的超时时间（单位：秒）。MySQL 默认 Idle 连接约 8 小时后会自动清理。

## UNION ALL 中的多个 SQL 段是并行执行的吗？

是的，它们会并行执行。

## 遇到 SQL 导致 BE Crash 应该如何处理？

1. 基于 `be.out` 报错堆栈查看导致 Crash 的 `query_id`。
2. 根据 `query_id` 在 `fe.audit.log` 中找到对应的 SQL。

请将以下信息收集并发给支持团队：

- `be.out` 日志
- 运行 `pstack $be_pid > pstack.log`，执行 SQL。
- Core Dump 文件

按照以下步骤收集 Core 文件：

1. 获取对应的 BE 进程：

   ```Bash
   ps aux| grep be
   ```

2. 设置 Core 文件大小限制为 unlimited。

   ```Bash
   prlimit -p $bePID --core=unlimited:unlimited
   ```

   验证大小限制是否为 unlimited。

   ```Bash
   cat /proc/$bePID/limits
   ```

如果不为 `0`，则进程崩溃时，系统会在 BE 部署根目录下生成一个 Core 文件。

## 如何使用 Hints 控制表关联优化器行为？

支持 `broadcast` 与 `shuffle` 两种 Hint。例如：

- `select * from a join [broadcast] b on a.id = b.id;`
- `select * from a join [shuffle] b on a.id = b.id;`

## 如何提高 SQL 查询并发度？

通过调整会话变量 `pipeline_dop`。

## 如何查看 DDL 的执行进度？

- 查看默认数据库下所有列修改任务：

   ```SQL
   SHOW ALTER TABLE COLUMN;
   ```

- 查看指定表最近一次列修改任务：

   ```SQL
   SHOW ALTER TABLE COLUMN WHERE TableName="table1" ORDER BY CreateTime DESC LIMIT 1;
   ```

## 为什么浮点数比较会导致查询结果有时能查到、有时查不到？

直接使用浮点数 `=` 比较会因误差导致不稳定，建议使用范围判断。

## 为什么浮点数计算会出现误差？

FLOAT/DOUBLE 类型在 `avg`、`sum` 等计算中存在精度误差，不同查询结果可能不一致。如需高精度，请使用 DECIMAL 类型，但性能会下降 2~3 倍。

## 为什么子查询中的 ORDER BY 不生效？

在分布式执行中，若不在子查询外层指定 ORDER BY，则无法保证全局有序。这是预期行为。

## 为什么 row_number() 的结果在多次执行时不一致？

如果 ORDER BY 的字段存在重复（如多行 `createTime` 相同），SQL 标准不保证稳定排序。建议在 ORDER BY 中加入唯一字段（如 `employee_id`）以确保稳定性。

## SQL 优化或排查问题需要收集哪些信息？

- `EXPLAIN COSTS <SQL>`（包含统计信息）
- `EXPLAIN VERBOSE <SQL>`（包含数据类型、nullable、优化策略）
- Query Profile（通过 FE Web 界面 `http://<fe_ip>:<fe_http_port>` 并导航至 Queries Tab 查看）
- Query Dump（通过 HTTP API 获取）

  ```Bash
  wget --user=${username} --password=${password} --post-file ${query_file} http://${fe_host}:${fe_http_port}/api/query_dump?db=${database} -O ${dump_file}
  ```

Query Dumb 包含以下信息：

- 查询语句
- 查询中引用的表结构
- 会话变量
- BE 个数
- 统计信息（Min，Max 值）
- 异常信息（异常堆栈）

## 如何查看数据倾斜情况？

使用 `ADMIN SHOW REPLICA DISTRIBUTION FROM <table>` 查看 Tablet 分布情况。

## 出现内存相关报错时如何排查？

常见三类情况：

- **单查询内存超限：**
  - 报错：`Mem usage has exceed the limit of single query, You can change the limit by set session variable exec_mem_limit.`
  - 解决方案：调整 `exec_mem_limit`
- **查询池内存超限：**
  - 报错：`Mem usage has exceed the limit of query pool`
  - 解决方案：需优化 SQL。
- **BE 总内存超限：**
  - 报错：`Mem usage has exceed the limit of BE`
  - 解决方案：分析内存占用。

内存分析方法：

```Bash
curl -XGET -s http://BE_IP:BE_HTTP_PORT/metrics | grep "^starrocks_be_.*_mem_bytes\|^starrocks_be_tcmalloc_bytes_in_use"
curl -XGET -s http://BE_IP:BE_HTTP_PORT/mem_tracker
```

---

## 报错 `StarRocks planner use long time xxx ms in logical phase` 怎么办？

1. 提供分析 `fe.gc.log` 检查是否出现 Full GC。
2. 如果该 SQL 执行计划较复杂，可调大 `new_planner_optimize_timeout`（单位：ms）：

   ```SQL
   set global new_planner_optimize_timeout = 6000;
   ```

## 遇到 Unknown Error 时如何排查？

可依次尝试调整以下参数后重新执行 SQL：

```SQL
set disable_join_reorder = true;
set enable_global_runtime_filter = false;
set enable_query_cache = false;
set cbo_enable_low_cardinality_optimize = false;
```

然后收集 EXPLAIN COSTS、EXPLAIN VERBOSE、PROFILE 和 Query Dump，并反馈给支持团队。

## `select now()` 返回的时间是什么时区？

返回的是由 `time_zone` 系统变量指定的时区。FE/BE 日志则使用机器本地时区。

## 为什么高并发场景下资源正常但 SQL 仍然变慢？

原因为网络或 RPC 耗时高。您可以将 BE 参数 `brpc_connection_type` 调整为 `pooled`，然后重启 BE。

## 如何关闭统计信息采集？

- 关闭自动收集：

  ```SQL
  enable_statistic_collect = false;
  ```

- 关闭导入触发收集：

  ```SQL
  enable_statistic_collect_on_first_load = false;
  ```

- 低版本升级到 v3.3 及以上后需手动设置：

  ```SQL
  set global analyze_mv = "";
  ```
