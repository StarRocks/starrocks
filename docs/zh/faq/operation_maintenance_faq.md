---
displayed_sidebar: docs
---

# 运维

本主题提供了一些与运维相关问题的解答。

## 可以清理 `trash` 目录吗？

您可以配置 FE 参数 `catalog_trash_expire_second` 来指定文件在 FE `trash` 目录中保留的时间（默认：24 小时）。

BE 参数 `trash_file_expire_time_sec` 控制 BE 垃圾清理间隔（默认：24 小时）。

在执行 DROP TABLE 或 DROP DATABASE 后，数据首先进入 FE 垃圾箱并保留一天，在此期间可以通过 RECOVER 恢复。之后，它会移动到 BE 垃圾箱，也会保留 24 小时。

## tablet 是否有主从关系？如果一些副本丢失，会对查询产生什么影响？

如果表属性 `replicated_storage` 设置为 `true`，写入使用主从机制：数据首先写入主副本，然后同步到其他副本。多个副本通常对查询性能没有重大影响。

## 可以通过监控界面测量任务的 CPU 和内存使用情况吗？

您可以查看 `fe.audit.log` 中的 `cpucostns` 和 `memcostbytes` 字段。

## 在更新表上创建物化视图时返回错误 "The aggregation type of column[now_time] must be same as the aggregate type of base column in aggregate table"。更新表支持物化视图吗？

该错误表明物化视图的聚合类型必须与基表的聚合类型匹配。对于更新表，您只能通过物化视图来调整它们的排序键顺序。
例如，如果基表 `tableA` 有列 `k1`、`k2`、`k3`，其中 `k1` 和 `k2` 为排序键。当您的查询包含 `WHERE k3=x` 子句并需要加速前缀索引时，您可以创建一个以 `k3` 为第一列的物化视图：

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA;
```

## 如何获取具有精确时间戳的导入量指标？`query_latency` 是平均响应时间的指标吗？

表级导入指标可以从以下地址获取：`http://user:password@fe_host:http_port/metrics?type=json&with_table_metrics=all`。

集群级数据可以从以下地址获取：`http://fe_host:http_port/api/show_data`（增量必须手动计算）。

`query_latency` 提供百分位查询响应时间。

## SHOW PROC '/backends' 和 SHOW BACKENDS 有什么区别？

`SHOW PROC '/backends'` 从当前 FE 检索元数据，可能会有延迟。而 `SHOW BACKENDS` 从 Leader FE 检索元数据，是权威的。

## StarRocks 有超时机制吗？为什么一些客户端连接会持续很长时间？

有。您可以配置系统变量 `wait_timeout`（默认：8 小时）来调整连接超时。

示例：

```SQL
SET GLOBAL wait_timeout = 3600;
```

## 可以在一个语句中授权多个表吗？

不可以。不支持类似 `GRANT <priv> on db1.tb1, db1.tb2` 的语句。

## 当授予了 ALL TABLES 权限时，可以撤销特定表的权限吗？

不支持子集撤销。建议在数据库或表级别授予权限。

## StarRocks 支持表级和行级权限吗？

支持表级访问控制。开源版不支持行级和列级访问控制。

## 如果主键表没有分区，冷热数据分离仍然有效吗？

不。冷热分离基于分区。

## 如果数据错误地放置在根目录，可以更改数据存储路径吗？

可以。更新 `be.conf` 中的 `storage_root_path` 以添加新磁盘，并使用分号分隔路径。

## 如何检查 FE 的 StarRocks 版本？

运行 `SHOW FRONTENDS;` 并检查 `Version` 字段。

## StarRocks 支持带有嵌套子查询的 DELETE 吗？

从 v2.3 开始，主键表支持完整的 DELETE WHERE 语法。
详情请参见 [Reference - DELETE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/table_bucket_part_index/DELETE/)。

## 对于动态分区，如果不希望旧分区自动清理，可以简单省略 dynamic_partition.start 吗？

不可以。需要将其设置为一个非常大的值。

## 如果 BE 机器内存故障需要维护，该怎么办？

[Decommission](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md#be) 该 BE。修复后，将其重新添加到集群。

## 可以强制所有未来的分区默认使用 SSD 吗？

不可以。默认是 HDD。需要手动配置。

## 添加新 BEs 后，tablets 自动重新平衡。可以立即退役旧 BEs 吗？

可以。无需等待重新平衡完成。最多可以同时退役两个节点。

## 添加新 BEs 和移除旧 BEs 会影响性能吗？

重新平衡是自动进行的，不应影响正常操作。建议一次移除一个节点。

## 如何用六个新节点替换六个 BE 节点？

添加六个新的 BE 节点，然后逐个退役旧节点。

## 为什么在退役节点后不健康的 tablet 数量没有减少？

检查单副本表。如果它们不断重试，其他修复可能会被阻止。

## 这个 BE 日志是什么意思？"tcmalloc: large alloc xxxxxxxx bytes"

发生了大内存分配请求，通常由大查询引起。检查 `be.INFO` 中的相应 `query_id` 以定位 SQL。

## 添加节点后，tablet 迁移会导致磁盘 I/O 波动吗？

是的，在平衡期间会出现临时的 I/O 波动。

## 云部署的数据迁移推荐方法是什么？

- 要迁移单个表，您可以：
  - 创建一个 StarRocks external table，然后使用 INSERT INTO SELECT 从中导入数据。
  - 使用基于 Spark-connector 的程序从源 BE 读取数据，并使用封装的 STREAM LOAD 将数据导入到目标 BE。
- 要迁移多个表，您可以使用备份和恢复。首先，将数据从源集群备份到远端存储。然后，从远端存储恢复数据到目标集群。
- 如果您的集群使用 HDFS 作为远端存储，您可以先使用 `distcp` 迁移数据文件，然后使用 Broker Load 将数据导入到目标集群。

## 如何解决错误 "failed to create task: disk ... exceed limit usage"？

磁盘已满。扩展存储或清理垃圾。

## FE 日志显示 "tablet migrate failed"。如何解决？

可能是因为 `storage_root_path` 是 HDD，而表属性 `storage_medium` 设置为 `SSD`。您可以将表属性设置为 `HDD`：

```SQL
ALTER TABLE db.table MODIFY PARTITION (*) SET("storage_medium"="HDD");
```

## 可以通过仅复制元数据将 FE 迁移到另一台机器吗？

不可以。使用推荐的方法：添加新节点，然后移除旧节点。

## 为什么一个 BE 上的磁盘使用不均衡（500 GB 99%，2 TB 20%）？为什么没有进行平衡？

平衡假设磁盘大小相同。使用相同大小的磁盘以确保均匀分布。

## 如果 `max_backend_down_time_second` 设置为 `3600`，这是否意味着我必须在一小时内恢复故障 BE？

如果 BE 的停机时间超过此配置，FE 将在其他 BE 上补充副本。当故障 BE 被重新添加到集群时，可能会产生较大的重新平衡成本。

## 是否有 IDE 工具可以从生产环境中导出表结构和视图？

有。请参见 [olapdb-tool](https://github.com/Astralidea/olapdb-tool)。

## 可以在单个 SQL 中设置多个系统变量（例如，超时和并行度）吗？

可以。

示例：

```SQL
SELECT /*+ SET_VAR(query_timeout=1, is_report_success=true, parallel_fragment_exec_instance_num=2) */ COUNT(1)
FROM table;
```

## 排序键中的 VARCHAR 列是否遵循 36 字节前缀限制？

VARCHAR 列根据实际长度进行截断。只有第一列获得短键索引。如果可能，将 VARCHAR 排序键放在第三个位置。

## BE 启动失败并显示 "while lock file" 错误。如何处理？

BE 进程仍在运行。终止守护进程并重新启动。

## 客户端是否需要显式连接到 observer FEs 以进行只读查询？

不需要。写请求会自动路由到 leader FE；observer 提供只读查询服务。

## FE 退出并显示 `LOG_FILE_NOT_FOUND`，原因是打开的文件过多。如何解决？

通过运行 `cat /proc/$pid/limits` 检查操作系统文件描述符限制，并运行 `lsof -n -p $fe_pid>/tmp/fe_fd.txt` 检查正在使用的文件描述符。

## 分区和桶的数量有限制吗？

- 对于分区，默认限制为 `4096`（可通过 FE 配置 `max_partitions_in_one_batch` 进行配置）。
- 对于桶，没有限制。每个桶的推荐大小为 1 GB。

## 如何手动切换 FE leader？

停止当前 Leader，将自动选举出新的 leader。

## 如何用新机器替换三个 FE 节点（1 个 Leader 和 2 个 Follower）？

1. 向集群中添加 2 个新的 Follower。
2. 移除 1 个旧 Follower。
3. 添加最后一个新的 Follower。
4. 移除剩余的旧 Follower。
5. 移除旧的 Leader。

## 动态分区创建未按预期工作。为什么？

动态分区检查每 10 分钟运行一次。此行为由 FE 配置 `dynamic_partition_check_interval_seconds` 控制。

## FE 日志频繁显示 "connect processor exception because，java.io.IOException: Connection reset by peer"。为什么？

可能是客户端断开连接、连接池丢弃或网络问题。检查操作系统的 backlog 指标和网络稳定性。

检查以下指标是否发生变化：

```Bash
netstat -s | grep -i LISTEN
netstat -s | grep TCPBacklogDrop
cat /proc/sys/net/core/somaxconn
```

## 执行 TRUNCATE 语句后，存储空间何时释放？

立即释放。

## 如何检查桶分布是否均衡？

运行以下命令：

```SQL
SHOW TABLET FROM db.table PARTITION (<partition_name>);
```

## 如何解决错误 "Fail to get master client from cache"？

这是 FE–BE 通信失败。检查 IP 和端口连接性。

## 如何在 IP 变化时迁移 StarRocks？

推荐使用 FQDN 模式部署。

## 可以将非分区表转换为分区表吗？

不可以。您可以创建一个新的分区表，并使用 INSERT INTO SELECT 迁移数据。

## 可以查询历史 SQL 执行记录吗？是否有审计日志？

可以。请参见 `fe.audit.log`。

## 如何将非分区表转换为 SSD 存储？

运行以下 SQL：

```SQL
ALTER TABLE db.tbl MODIFY PARTITION (*) SET ("storage_medium"="SSD");
```

## 执行 ALTER TABLE ADD COLUMN 后，查询 `information_schema.COLUMNS` 显示延迟。这正常吗？

是的。ALTER 操作是异步的。通过 `SHOW ALTER COLUMN` 检查进度。

## 如果动态分区表的保留期从 366 天更改为 732 天，可以自动创建历史分区吗？

请按照以下步骤操作：

1. 禁用动态分区。

   ```SQL
   ALTER TABLE db.tbl SET  ("dynamic_partition.enable" = "false");
   ```

2. 手动添加分区。

   ```SQL
   ALTER TABLE db.tbl ADD PARTITIONS START ("2019-01-01") END ("2019-12-31") EVERY (interval 1 day);
   ```

3. 重新启用动态分区。

   ```SQL
   ALTER TABLE db.tbl SET  ("dynamic_partition.enable" = "true"); 
   ```

## 可以监控和警报 Routine Load 任务从 Running 切换到 Paused 吗？

可以。StarRocks 支持 Routine Load 任务的监控指标，并可以连接到警报系统。

## 如何诊断不健康的副本？

运行以下语句以识别 UnhealthyTablets：

```SQL
SHOW PROC '/statistic'
SHOW PROC '/statistic/<db_id>'
```

然后，使用 `SHOW TABLET tablet_id` 分析 UnhealthyTablets。

如果结果显示两个副本的数据一致，但一个副本的数据不一致——这意味着三个副本中有两个成功完成了写入——这被视为成功写入。然后您可以检查 UnhealthyTablets 中的 tablets 是否已修复。如果它们已修复，则表示存在问题。如果状态正在变化，您可以调整相应表的导入频率。

## 错误："SyntaxErrorException: Reach limit of connections"。如何排查？

通过运行以下命令增加每个用户的限制：

```SQL
ALTER USER 'jack' SET PROPERTIES ('max_user_connections'='1000');
```

还要检查负载均衡器和空闲连接积累（`wait_timeout`）。

## XFS 和 ext4 如何影响 QPS？

StarRocks 通常在 XFS 上表现更好。

## BE 被认为宕机并开始 tablet 迁移需要多长时间？

1. 当心跳（默认：每 5 秒）失败 3 次时，BE 被标记为不活跃。
2. 之后，在执行 Clone 之前有一个预期的延迟，为 60 秒。
3. 然后，副本克隆开始。

如果 BE 后来恢复，其副本将被删除。

## 新节点上的 tablet 调度速度慢（一次只有 100 个）。如何调整？

调整以下 FE 配置：

```SQL
ADMIN SET FRONTEND CONFIG ("schedule_slot_num_per_path"="8");
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets"="1000");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets"="1000");
```

## BACKUP 操作是串行的吗？似乎只有一个 HDFS 目录在变化

BACKUP 操作是并行的，但上传到 HDFS 使用单个工作线程，由 BE 配置 `upload_worker_count` 控制。调整时要谨慎，因为它可能会影响磁盘 I/O 和网络 I/O。

## 如何解决 FE OOM 错误 "OutOfMemoryError: GC overhead limit exceeded"？

增加 FE JVM 内存。

## 如何解决错误 "Cannot truncate a file by broker"？

1. 检查 broker 日志中的错误信息。
2. 检查 BE 警告日志中的错误 "remote file checksum is invalid. remote:**** local: *****"。
3. 在 broker `apache_hdfs_broker.log` 中搜索错误 "receive a check path request, request detail"，并识别重复文件。
4. 删除或重命名有问题的远程文件并重试。

## 如何验证磁盘移除已完成？

运行 `SHOW PROC '/statistic'` 并确保 `UnhealthyTablet` 计数为零。

## 如何修改历史分区的副本数量？

运行以下命令：

```SQL
ALTER TABLE db.tbl MODIFY PARTITION (*) SET("replication_num"="3");
```

## 对于三副本表，如果 BE 节点的磁盘损坏，副本会自动恢复以保持三份吗？

是的，如果有足够的 BE 节点可用。

## 如果在增加用户限制后错误 "reach limit connections" 仍然持续，如何排查？

检查负载均衡器（ProxySQL、F5）、空闲连接积累，并将 `wait_timeout` 减少到 2–4 小时。

## 如果 FE 元数据丢失，整个集群的元数据是否丢失？

元数据驻留在 FE 中。只有一个 FE 时，是不可恢复的。多个 FE 时，可以重新添加故障节点，元数据将被复制。

## 如何解决导入错误 "INTERNAL_ERROR, FE leader shows NullPointerException"？

添加 JVM 选项 `-XX:-OmitStackTraceInFastThrow` 并重启 FE 以获取完整的堆栈跟踪。

## 如何解决错误 "The partition column could not be aggregated column" 在为主键表设置分区列时？

分区列必须是键列。
