---
displayed_sidebar: "Chinese"
---

# SHOW PROC

## 功能

查看当前集群中的特定指标。

## 语法

```SQL
SHOW PROC { '/auth' | '/backends' | '/compute_nodes' | '/dbs' 
          | '/jobs' | '/statistic' | '/tasks' | '/frontends' 
          | '/brokers' | '/resources' | '/load_error_hub' 
          | '/transactions' | '/monitor' | '/current_queries' 
          | '/current_backend_instances' | '/cluster_balance' 
          | '/routine_loads' | '/colocation_group' | '/catalog' }
```

## 参数说明

| **参数**                     | **说明**                                                     |
| ---------------------------- | ------------------------------------------------------------ |
| '/auth'                      | 查看当前集群的用户权限及认证信息。                           |
| '/backends'                  | 查看当前集群的 BE 节点信息。                                 |
| '/compute_nodes'             | 查看当前集群的 CN 节点信息。                                 |
| '/dbs'                       | 查看当前集群的数据库信息。                                   |
| '/jobs'                      | 查看当前集群的作业信息。                                     |
| '/statistic'                 | 查看当前集群各数据库的统计信息。                             |
| '/tasks'                     | 查看当前集群各种任务类型的总数和失败总数。                   |
| '/frontends'                 | 查看当前集群的 FE 节点信息。                                 |
| '/brokers'                   | 查看当前集群的 Broker 节点信息。                             |
| '/resources'                 | 查看当前集群的资源信息。                                     |
| '/load_error_hub'            | 查看当前集群的 Error Hub 的配置信息。Error Hub 用于管理导入作业产生的错误信息。 |
| '/transactions'              | 查看当前集群的事务信息。                                     |
| '/monitor'                   | 查看当前集群的监控信息。                                     |
| '/current_queries'           | 查看当前连接的FE节点正在执行的查询信息。                       |
| '/current_backend_instances' | 查看当前集群正在执行作业的 BE 节点。                         |
| '/cluster_balance'           | 查看当前集群的负载信息。                                     |
| '/routine_loads'             | 查看当前集群的 Routine Load 导入信息。                       |
| '/colocation_group'          | 查看当前集群的 Colocate Join Group 信息。                    |
| '/catalog'                   | 查看当前集群的 Catalog 信息。                                |

## 示例

示例一：查看当前集群的用户权限及认证信息。

```Plain
mysql> SHOW PROC '/auth';
+-------------------------------------+----------+------------+-------------------+----------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+---------------+
| UserIdentity                        | Password | AuthPlugin | UserForAuthPlugin | GlobalPrivs                                                                | DatabasePrivs                                                                                                                                                                         | TablePrivs | ResourcePrivs |
+-------------------------------------+----------+------------+-------------------+----------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+---------------+
| 'root'@'%'                          | Yes      | NULL       | NULL              | Node_priv Admin_priv  (false)                                              | NULL                                                                                                                                                                                  | NULL       | NULL          |
| 'default_cluster:johndoe'@'%'       | Yes      | NULL       | NULL              | Admin_priv Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false) | information_schema: Select_priv  (false)                                                                                                                                              | NULL       | NULL          |
| 'default_cluster:katherine'@'%'     | Yes      | NULL       | NULL              |  (false)                                                                   | information_schema: Select_priv  (false)                                                                                                                                              | NULL       | NULL          |
+-------------------------------------+----------+------------+-------------------+----------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+---------------+
```

| **返回**          | **说明**                                                     |
| ----------------- | ------------------------------------------------------------ |
| UserIdentity      | 用户标识，形式为 `username@'userhost'` 或 `username@['domain']`。 |
| Password          | 用户是否通过密码连接集群。                                   |
| AuthPlugin        | 用户的认证方式，仅包括 `mysql_native_password` 和 `authentication_ldap_simple`。 |
| UserForAuthPlugin | 假如用户以 LDAP 认证，则提供 LDAP 中的 Distinguished Name（DN）。 |
| GlobalPrivs       | 用户拥有的全局权限。                                         |
| DatabasePrivs     | 用户拥有的数据库级别权限。                                   |
| TablePrivs        | 用户拥有的表级别权限。                                       |
| ResourcePrivs     | 用户拥有的资源级别权限。                                     |

示例二：查看当前集群的 BE 节点信息。

```Plain
mysql> SHOW PROC '/backends';
+-----------+---------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+-------------------+--------------------------------------------------------+-------------------+-------------+----------+
| BackendId | IP            | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | TabletNum | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | MaxDiskUsedPct | ErrMsg | Version           | Status                                                 | DataTotalCapacity | DataUsedPct | CpuCores |
+-----------+---------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+-------------------+--------------------------------------------------------+-------------------+-------------+----------+
| 56038515  | xxx.xx.xx.xxx | 9052          | 9060   | 8097     | 8059     | 2022-10-09 19:57:57 | 2022-10-10 16:23:24 | true  | false                | false                 | 47015     | 67.952 GB        | 1.089 TB      | 1.968 TB      | 44.69 % | 44.69 %        |        | UNKNOWN-de75d4fbb | {"lastSuccessReportTabletsTime":"2022-10-10 16:22:44"} | 1.155 TB          | 5.74 %      | 104      |
+-----------+---------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+-------------------+--------------------------------------------------------+-------------------+-------------+----------+
```

| **返回**              | **说明**                                                     |
| --------------------- | ------------------------------------------------------------ |
| BackendId             | BE 节点 ID。                                                 |
| IP                    | BE 节点 IP 地址。                                            |
| HeartbeatPort         | BE 节点心跳服务端口。                                        |
| BePort                | BE 节点 Thrift Server 端口。                                 |
| HttpPort              | BE 节点 HTTP Server 端口。                                   |
| BrpcPort              | BE 节点 bRPC 端口。                                          |
| LastStartTime         | BE 节点上一次启动时间。                                      |
| LastHeartbeat         | BE 节点上一次接受 FE 心跳时间。                              |
| Alive                 | BE 节点存活状态。                                            |
| SystemDecommissioned  | BE 节点是否正在下线。                                        |
| ClusterDecommissioned | BE 节点是否正在集群中下线。                                  |
| TabletNum             | BE 节点 Tablet 数量。                                        |
| DataUsedCapacity      | BE 节点被占用的存储空间。                                    |
| AvailCapacity         | BE 节点剩余存储空间。                                        |
| TotalCapacity         | BE 节点总存储空间。                                          |
| UsedPct               | BE 节点当前存储空间占用比例。                                |
| MaxDiskUsedPct        | BE 节点最大存储空间占用比例。                                |
| ErrMsg                | BE 节点错误信息。                                            |
| Version               | BE 节点 StarRocks 版本。                                     |
| Status                | BE节点状态信息，包含最近一次 BE 汇报 Tablet 的时间信息。     |
| DataTotalCapacity     | 数据文件所占用的磁盘空间 + 可用磁盘空间，即 DataUsedCapacity + AvailCapacity。 |
| DataUsedPct           | 数据文件占用磁盘的比例，即 DataUsedCapacity/DataTotalCapacity。 |
| CpuCores              | BE 节点 CPU 核数。                                           |

示例三：查看当前集群的数据库信息。

```Plain
mysql> SHOW PROC '/dbs';
+---------+------------------------+----------+----------------+--------------------------+---------------------+
| DbId    | DbName                 | TableNum | Quota          | LastConsistencyCheckTime | ReplicaQuota        |
+---------+------------------------+----------+----------------+--------------------------+---------------------+
| 1       | information_schema     | 22       | 8388608.000 TB | NULL                     | 9223372036854775807 |
| 840997  | tpcds_100g             | 25       | 1024.000 GB    | NULL                     | 1073741824          |
| 1275196 | _statistics_           | 3        | 8388608.000 TB | 2022-09-06 23:00:58      | 9223372036854775807 |
| 1286207 | tpcds_n                | 24       | 8388608.000 TB | NULL                     | 9223372036854775807 |
| 1381289 | test                   | 6        | 8388608.000 TB | 2022-01-14 23:10:18      | 9223372036854775807 |
| 6186781 | test_stddev            | 1        | 8388608.000 TB | 2022-09-06 23:00:58      | 9223372036854775807 |
+---------+------------------------+----------+----------------+--------------------------+---------------------+
```

| **返回**                 | **说明**                     |
| ------------------------ | ---------------------------- |
| DbId                     | 数据库 ID。                  |
| DbName                   | 数据库名称。                 |
| TableNum                 | 数据库包含表数量。           |
| Quota                    | 数据库设置的存储配额。       |
| LastConsistencyCheckTime | 数据库上一次一致性检查时间。 |
| ReplicaQuota             | 数据库的副本配额。           |

示例四：查看当前集群的作业信息。您可以通过对应的 `DbId` 进一步查询该数据库中的详细作业信息。

```Plain
mysql> SHOW PROC '/jobs';
+-------+--------------------------------------+
| DbId  | DbName                               |
+-------+--------------------------------------+
| 10005 | default_cluster:_statistics_         |
| 0     | default_cluster:information_schema   |
| 12711 | default_cluster:starrocks_audit_db__ |
+-------+--------------------------------------+
3 rows in set (0.00 sec)

mysql> SHOW PROC '/jobs/10005';
+---------------+---------+---------+----------+-----------+-------+
| JobType       | Pending | Running | Finished | Cancelled | Total |
+---------------+---------+---------+----------+-----------+-------+
| load          | 0       | 0       | 3        | 0         | 3     |
| rollup        | 0       | 0       | 0        | 0         | 0     |
| schema_change | 0       | 0       | 0        | 0         | 0     |
| export        | 0       | 0       | 0        | 0         | 0     |
+---------------+---------+---------+----------+-----------+-------+
4 rows in set (0.00 sec)
```

| **返回**  | **说明**         |
| --------- | ---------------- |
| DbId      | 数据库 ID。      |
| DbName    | 数据库名称。     |
| JobType   | 作业类型。       |
| Pending   | 待执行作业数。   |
| Running   | 正在执行作业数。 |
| Finished  | 已完成作业数。   |
| Cancelled | 已取消作业数。   |
| Total     | 总作业数。       |

示例五：查看当前集群各数据库的统计信息。

```Plain
mysql> SHOW PROC '/statistic';
+--------+----------------------------------------------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+------------------+---------------------+
| DbId   | DbName                                                   | TableNum | PartitionNum | IndexNum | TabletNum | ReplicaNum | UnhealthyTabletNum | InconsistentTabletNum | CloningTabletNum | ErrorStateTabletNum |
+--------+----------------------------------------------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+------------------+---------------------+
| 10004  | _statistics_                                             | 3        | 3            | 3        | 30        | 60         | 0                  | 0                     | 0                | 0                   |
| 1      | information_schema                                       | 0        | 0            | 0        | 0         | 0          | 0                  | 0                     | 0                | 0                   |
| 92498  | stream_load_test_db_03afc714_b1cb_11ed_a82c_00163e237e98 | 0        | 0            | 0        | 0         | 0          | 0                  | 0                     | 0                | 0                   |
| 92542  | stream_load_test_db_79876e92_b1da_11ed_b50e_00163e237e98 | 1        | 1            | 1        | 3         | 3          | 0                  | 0                     | 0                | 0                   |
| 115476 | testdb                                                   | 0        | 0            | 0        | 0         | 0          | 0                  | 0                     | 0                | 0                   |
| 10002  | zq_test                                                  | 8        | 8            | 8        | 5043      | 7063       | 0                  | 0                     | 0                | 2                   |
| Total  | 6                                                        | 12       | 12           | 12       | 5076      | 7126       | 0                  | 0                     | 0                | 2                   |
+--------+----------------------------------------------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+------------------+---------------------+
7 rows in set (0.01 sec)

mysql> show proc '/statistic/10002';
+------------------+---------------------+----------------+-------------------+
| UnhealthyTablets | InconsistentTablets | CloningTablets | ErrorStateTablets |
+------------------+---------------------+----------------+-------------------+
| []               | []                  | []             | [116703, 116706]  |
+------------------+---------------------+----------------+-------------------+
```

| **返回**              | **说明**                                    |
| --------------------- | ------------------------------------------- |
| DbId                  | 数据库 ID。                                 |
| DbName                | 数据库名称。                                |
| TableNum              | 数据库中表的数量。                          |
| PartitionNum          | 数据库中分区的数量。                        |
| IndexNum              | 数据库中索引的数量。                        |
| TabletNum             | 数据库中 Tablet 的数量。                    |
| ReplicaNum            | 数据库中副本的数量。                        |
| UnhealthyTabletNum    | 数据重分布过程中还未完成的 Tablet 数量。      |
| InconsistentTabletNum | 数据库中不一致的 Tablet 数量。              |
| CloningTabletNum      | 数据库中正在进行 Clone 操作的 Tablet 数量。   |
| ErrorStateTabletNum   | 主键表中错误状态的 Tablet 数量。          |
| ErrorStateTablets     | 主键表中错误状态的 Tablet 的 ID。         |

示例六：查看当前集群各种任务类型的总数和失败总数。

```Plain
mysql> SHOW PROC '/tasks';
+-------------------------+-----------+----------+
| TaskType                | FailedNum | TotalNum |
+-------------------------+-----------+----------+
| CREATE                  | 0         | 0        |
| DROP                    | 0         | 0        |
| PUSH                    | 0         | 0        |
| CLONE                   | 0         | 0        |
| STORAGE_MEDIUM_MIGRATE  | 0         | 0        |
| ROLLUP                  | 0         | 0        |
| SCHEMA_CHANGE           | 0         | 0        |
| CANCEL_DELETE           | 0         | 0        |
| MAKE_SNAPSHOT           | 0         | 0        |
| RELEASE_SNAPSHOT        | 0         | 0        |
| CHECK_CONSISTENCY       | 0         | 0        |
| UPLOAD                  | 0         | 0        |
| DOWNLOAD                | 0         | 0        |
| CLEAR_REMOTE_FILE       | 0         | 0        |
| MOVE                    | 0         | 0        |
| REALTIME_PUSH           | 0         | 0        |
| PUBLISH_VERSION         | 0         | 0        |
| CLEAR_ALTER_TASK        | 0         | 0        |
| CLEAR_TRANSACTION_TASK  | 0         | 0        |
| RECOVER_TABLET          | 0         | 0        |
| STREAM_LOAD             | 0         | 0        |
| UPDATE_TABLET_META_INFO | 0         | 0        |
| ALTER                   | 0         | 0        |
| INSTALL_PLUGIN          | 0         | 0        |
| UNINSTALL_PLUGIN        | 0         | 0        |
| NUM_TASK_TYPE           | 0         | 0        |
| Total                   | 0         | 0        |
+-------------------------+-----------+----------+
```

| **返回**  | **说明**     |
| --------- | ------------ |
| TaskType  | 任务类型。   |
| FailedNum | 失败任务数。 |
| TotalNum  | 总任务数。   |

示例七：查看当前集群的 FE 节点信息。

```Plain
mysql> SHOW PROC '/frontends';
+----------------------------------+---------------+-------------+----------+-----------+---------+----------+------------+-------+-------+-------------------+---------------+----------+---------------+-----------+---------+
| Name                             | IP            | EditLogPort | HttpPort | QueryPort | RpcPort | Role     | ClusterId  | Join  | Alive | ReplayedJournalId | LastHeartbeat | IsHelper | ErrMsg        | StartTime | Version |
+----------------------------------+---------------+-------------+----------+-----------+---------+----------+------------+-------+-------+-------------------+---------------+----------+---------------+-----------+---------+
| xxx.xx.xx.xxx_9009_1600088918395 | xxx.xx.xx.xxx | 9009        | 7390     | 0         | 0       | FOLLOWER | 1747363037 | false | false | 0                 | NULL          | true     | got exception | NULL      | NULL    |
+----------------------------------+---------------+-------------+----------+-----------+---------+----------+------------+-------+-------+-------------------+---------------+----------+---------------+-----------+---------+
```

| **返回**          | **说明**                                      |
| ----------------- | --------------------------------------------- |
| Name              | FE 节点名称。                                 |
| IP                | FE 节点 IP 地址。                             |
| EditLogPort       | FE 节点之间的通信端口。                       |
| HttpPort          | FE 节点 HTTP Server 端口。                    |
| QueryPort         | FE 节点 MySQL Server 端口。                   |
| RpcPort           | FE 节点 RPC 端口。                            |
| Role              | FE 节点角色（Leader、Follower 或 Observer）。 |
| ClusterId         | 集群 ID。                                     |
| Join              | FE 节点是否曾加入过集群。                     |
| Alive             | FE 节点存活状态。                             |
| ReplayedJournalId | FE 节点已回放过的最大元数据日志 ID。          |
| LastHeartbeat     | FE 节点上一次发送心跳时间。                   |
| IsHelper          | FE 节点是否为 BDBJE 的 Helper 节点。          |
| ErrMsg            | FE 节点错误信息。                             |
| StartTime         | FE 节点启动时间。                             |
| Version           | FE 节点 StarRocks 版本。                      |

示例八：查看当前集群的 Broker 节点信息。

```Plain
mysql> SHOW PROC '/brokers';
+-------------+---------------+------+-------+---------------+---------------------+--------+
| Name        | IP            | Port | Alive | LastStartTime | LastUpdateTime      | ErrMsg |
+-------------+---------------+------+-------+---------------+---------------------+--------+
| hdfs_broker | xxx.xx.xx.xxx | 8500 | true  | NULL          | 2022-10-10 16:37:59 |        |
| hdfs_broker | xxx.xx.xx.xxx | 8500 | true  | NULL          | 2022-10-10 16:37:59 |        |
| hdfs_broker | xxx.xx.xx.xxx | 8500 | true  | NULL          | 2022-10-10 16:37:59 |        |
+-------------+---------------+------+-------+---------------+---------------------+--------+
```

| **返回**       | **说明**                                         |
| -------------- | ------------------------------------------------ |
| Name           | Broker 节点名称。                                |
| IP             | Broker 节点 IP 地址。                            |
| Port           | Broker 节点的 Thrift Server 端口，用于接收请求。 |
| Alive          | Broker 节点存活状态。                            |
| LastStartTime  | Broker 节点上一次启动时间。                      |
| LastUpdateTime | Broker 节点上一次更新时间。                      |
| ErrMsg         | Broker 节点错误信息。                            |

示例九：查看当前集群的资源信息。

```Plain
mysql> SHOW PROC '/resources';
+-------------------------+--------------+---------------------+------------------------------+
| Name                    | ResourceType | Key                 | Value                        |
+-------------------------+--------------+---------------------+------------------------------+
| hive_resource_stability | hive         | hive.metastore.uris | thrift://xxx.xx.xxx.xxx:9083 |
| hive2                   | hive         | hive.metastore.uris | thrift://xxx.xx.xx.xxx:9083  |
+-------------------------+--------------+---------------------+------------------------------+
```

| **返回**     | **说明**     |
| ------------ | ------------ |
| Name         | 资源名称。   |
| ResourceType | 资源类型。   |
| Key          | 资源关键字。 |
| Value        | 资源值。     |

示例十：查看当前集群的事务信息。您可以通过对应的 `DbId` 进一步查询该数据库中的详细事务信息。

```Plain
mysql> SHOW PROC '/transactions';
+-------+--------------------------------------+
| DbId  | DbName                               |
+-------+--------------------------------------+
| 10005 | default_cluster:_statistics_         |
| 12711 | default_cluster:starrocks_audit_db__ |
+-------+--------------------------------------+
2 rows in set (0.00 sec)

mysql> SHOW PROC '/transactions/10005';
+----------+--------+
| State    | Number |
+----------+--------+
| running  | 0      |
| finished | 4      |
+----------+--------+
2 rows in set (0.00 sec)
```

| **返回** | **说明**     |
| -------- | ------------ |
| DbId     | 数据库 ID。  |
| DbName   | 数据库名称。 |
| State    | 事务状态。   |
| Number   | 事务数量。   |

示例十一：查看当前集群的监控信息。

```Plain
mysql> SHOW PROC '/monitor';
+------+------+
| Name | Info |
+------+------+
| jvm  |      |
+------+------+
```

| **返回** | **说明**   |
| -------- | ---------- |
| Name     | JVM 名称。 |
| Info     | JVM 信息。 |

示例十二：查看当前集群的负载信息。

```Plain
mysql> SHOW PROC '/cluster_balance';
+-------------------+--------+
| Item              | Number |
+-------------------+--------+
| cluster_load_stat | 1      |
| working_slots     | 3      |
| sched_stat        | 1      |
| priority_repair   | 0      |
| pending_tablets   | 2001   |
| running_tablets   | 0      |
| history_tablets   | 1000   |
+-------------------+--------+
```

| **返回** | **说明**                                     |
| -------- | -------------------------------------------- |
| Item     | cluster_balance 中的子命令。                 |
| Number   | cluster_balance 中每个子命令正在执行的个数。 |

示例十三：查看当前集群的 Colocate Join Group 信息。

```Plain
mysql> SHOW PROC '/colocation_group';
+-----------------+----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+------------+----------------+-------------+----------+
| GroupId         | GroupName                  | TableIds                                                                                                                                          | BucketsNum | ReplicationNum | DistCols    | IsStable |
+-----------------+----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+------------+----------------+-------------+----------+
| 24010.177354    | 24010_lineitem_str_g1      | 177672                                                                                                                                            | 12         | 1              | varchar(-1) | true     |
| 24010.182146    | 24010_lineitem_str_g2      | 182144                                                                                                                                            | 192        | 1              | varchar(-1) | true     |
| 1439318.1735496 | 1439318_group_agent_uid    | 1735677, 1738390                                                                                                                                  | 12         | 2              | bigint(20)  | true     |
| 24010.37804     | 24010_gsdaf2449s9e         | 37802                                                                                                                                             | 192        | 1              | int(11)     | true     |
| 174844.175370   | 174844_groupa4             | 175368, 591307, 591362, 591389, 591416                                                                                                            | 12         | 1              | int(11)     | true     |
| 24010.30587     | 24010_group2               | 30585, 30669                                                                                                                                      | 12         | 1              | int(11)     | true     |
| 10005.181366    | 10005_lineorder_str_normal | 181364                                                                                                                                            | 192        | 1              | varchar(-1) | true     |
| 1904968.5973175 | 1904968_groupa2            | 5973173                                                                                                                                           | 12         | 1              | int(11)     | true     |
| 24010.182535    | 24010_lineitem_str_g3      | 182533                                                                                                                                            | 192        | 1              | varchar(-1) | true     |
+-----------------+----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+------------+----------------+-------------+----------+
```

| **返回**       | **说明**                              |
| -------------- | ------------------------------------- |
| GroupId        | Colocate Join Group 的 ID。           |
| GroupName      | Colocate Join Group 的名称。          |
| TableIds       | Colocate Join Group 所包含的表的 ID。 |
| BucketsNum     | Colocate Join Group 的分桶数。        |
| ReplicationNum | Colocate Join Group 的副本数。        |
| DistCols       | Colocate Join Group 的分桶列类型。    |
| IsStable       | Colocate Join Group 是否稳定。        |

示例十四：查看当前集群的 Catalog 信息。

```Plain
mysql> SHOW PROC '/catalog';
+--------------------------------------------------------------+----------+----------------------+
| Catalog                                                      | Type     | Comment              |
+--------------------------------------------------------------+----------+----------------------+
| resource_mapping_inside_catalog_hive_hive2                   | hive     | mapping hive catalog |
| resource_mapping_inside_catalog_hive_hive_resource_stability | hive     | mapping hive catalog |
| default_catalog                                              | Internal | Internal Catalog     |
+--------------------------------------------------------------+----------+----------------------+
```

| **返回** | **说明**       |
| -------- | -------------- |
| Catalog  | Catalog 名称。 |
| Type     | Catalog 类型。 |
| Comment  | Catalog 描述。 |
