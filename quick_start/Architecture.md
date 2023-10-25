# StarRocks的系统架构

## 系统架构图

![architecture](../assets/2.1-1.png)

## 组件介绍

StarRocks 集群由 FE 和 BE 构成, 可以使用 MySQL 客户端访问 StarRocks 集群。

### FE

FE接收MySQL客户端的连接, 解析并执行SQL语句。

* 管理元数据, 执行SQL DDL命令, 用Catalog记录库, 表, 分区, tablet副本等信息。
* FE高可用部署, 使用复制协议选主和主从同步元数据, 所有的元数据修改操作, 由FE leader节点完成, FE follower节点可执行读操作。 元数据的读写满足顺序一致性。  FE的节点数目采用2n+1, 可容忍n个节点故障。  当FE leader故障时, 从现有的follower节点重新选主, 完成故障切换。
* FE的SQL layer对用户提交的SQL进行解析, 分析, 改写, 语义分析和关系代数优化, 生产逻辑执行计划。
* FE的Planner负责把逻辑计划转化为可分布式执行的物理计划, 分发给一组BE。
* FE监督BE, 管理BE的上下线, 根据BE的存活和健康状态, 维持tablet副本的数量。
* FE协调数据导入, 保证数据导入的一致性。

### BE

* BE管理tablet副本, tablet是table经过分区分桶形成的子表, 采用列式存储。
* BE受FE指导, 创建或删除子表。
* BE接收FE分发的物理执行计划并指定BE coordinator节点, 在BE coordinator的调度下, 与其他BE worker共同协作完成执行。
* BE读本地的列存储引擎获取数据,并通过索引和谓词下沉快速过滤数据。
* BE后台执行compact任务, 减少查询时的读放大。
* 数据导入时, 由FE指定BE coordinator, 将数据以fanout的形式写入到tablet多副本所在的BE上。

### 其他组件

* 管理平台, 在后面会专门的章节介绍。
* Hdfs Broker:  用于从Hdfs中导入数据到StarRocks集群，见[数据导入](../loading/Loading_intro.md)章节。
