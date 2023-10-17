# SHOW PROC

## Description

Shows certain indicators of the StarRocks cluster.

## Syntax

```SQL
SHOW PROC { '/auth' | '/backends' | '/compute_nodes' | '/dbs' 
          | '/jobs' | '/statistic' | '/tasks' | '/frontends' 
          | '/brokers' | '/resources' | '/load_error_hub' 
          | '/transactions' | '/monitor' | '/current_queries' 
          | '/current_backend_instances' | '/cluster_balance' 
          | '/routine_loads' | '/colocation_group' | '/catalog' }
```

## Parameters

| **Parameter**                | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| '/auth'                      | Shows the user privilege and authentication information in the cluster. |
| '/backends'                  | Shows the information of BE nodes in the cluster.            |
| '/compute_nodes'             | Shows the information of CN nodes in the cluster.            |
| '/dbs'                       | Shows the information of databases in the cluster.           |
| '/jobs'                      | Shows the information of jobs in the cluster.                |
| '/statistic'                 | Shows the statistics of each database in the cluster.        |
| '/tasks'                     | Shows the total number of all generic tasks and the failed tasks in the cluster. |
| '/frontends'                 | Shows the information of FE nodes in the cluster.            |
| '/brokers'                   | Shows the information of Broker nodes in the cluster.        |
| '/resources'                 | Shows the information of resources in the cluster.           |
| '/load_error_hub'            | Shows the configuration of the cluster's Load Error Hub, which is used to manage error messages of loading jobs. |
| '/transactions'              | Shows the information of transactions in the cluster.        |
| '/monitor'                   | Shows the monitoring information in the cluster.             |
| '/current_queries'           | Shows the information of currently running queries in the cluster. |
| '/current_backend_instances' | Shows the BE nodes that are processing requests in the cluster. |
| '/cluster_balance'           | Shows the load balance information in the cluster.           |
| '/routine_loads'             | Shows the information of Routine Load in the cluster.        |
| '/colocation_group'          | Shows the information of Colocate Join groups in the cluster. |
| '/catalog'                   | Shows the information of catalogs in the cluster.            |

## Examples

Example 1: Shows the user privilege and authentication information in the cluster.

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

| **Return**        | **Description**                                              |
| ----------------- | ------------------------------------------------------------ |
| UserIdentity      | User identity, shown as `username@'userhost'` or `username@['domain']`. |
| Password          | If the user connects to the cluster using password.          |
| AuthPlugin        | Authentication plugin the user adopted, including `mysql_native_password` and `authentication_ldap_simple`. |
| UserForAuthPlugin | Distinguished Name (DN) of the user if they connect to the cluster via LDAP. |
| GlobalPrivs       | Global privileges owned by the user.                         |
| DatabasePrivs     | Database-level privileges owned by the user.                 |
| TablePrivs        | Table-level privileges owned by the user.                    |
| ResourcePrivs     | Resource-level privileges owned by the user.                 |

Example 2: Shows the information of BE nodes in the cluster.

```Plain
mysql> SHOW PROC '/backends';
+-----------+---------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+-------------------+--------------------------------------------------------+-------------------+-------------+----------+
| BackendId | IP            | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | TabletNum | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | MaxDiskUsedPct | ErrMsg | Version           | Status                                                 | DataTotalCapacity | DataUsedPct | CpuCores |
+-----------+---------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+-------------------+--------------------------------------------------------+-------------------+-------------+----------+
| 56038515  | xxx.xx.xx.xxx | 9052          | 9060   | 8097     | 8059     | 2022-10-09 19:57:57 | 2022-10-10 16:23:24 | true  | false                | false                 | 47015     | 67.952 GB        | 1.089 TB      | 1.968 TB      | 44.69 % | 44.69 %        |        | UNKNOWN-de75d4fbb | {"lastSuccessReportTabletsTime":"2022-10-10 16:22:44"} | 1.155 TB          | 5.74 %      | 104      |
+-----------+---------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+-------------------+--------------------------------------------------------+-------------------+-------------+----------+
```

| **Return**            | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| BackendId             | ID of the BE node.                                           |
| IP                    | IP address of the BE node.                                   |
| HeartbeatPort         | Heartbeat service port of the BE node.                       |
| BePort                | Thrift Server port of the BE node.                           |
| HttpPort              | HTTP Server port of the BE node.                             |
| BrpcPort              | BRPC port of the BE node.                                    |
| LastStartTime         | The last time when the BE node was started.                  |
| LastHeartbeat         | The last time when the BE node received a heartbeat.         |
| Alive                 | If the BE node is alive.                                     |
| SystemDecommissioned  | If the BE node is decommissioned.                            |
| ClusterDecommissioned | If the BE node is decommissioned within the cluster.         |
| TabletNum             | Number of tablets in the BE node.                            |
| DataUsedCapacity      | Storage capacity that is used for data in the BE node.       |
| AvailCapacity         | Available storage capacity in the BE node.                   |
| TotalCapacity         | Total storage capacity in the BE node.                       |
| UsedPct               | Percentage at which the storage capacity is used in the BE node. |
| MaxDiskUsedPct        | The maximum percentage at which the storage capacity is used in the BE node. |
| ErrMsg                | Error messages in the BE node.                               |
| Version               | StarRocks version of the BE node.                            |
| Status                | Status information of the BE node, including the last time when the BE node reported tablets. |
| DataTotalCapacity     | Total of used and available data storage capacity. The sum of `DataUsedCapacity` and `AvailCapacity`. |
| DataUsedPct           | Percentage at which the data storage takes up the total data capacity (DataUsedCapacity/DataTotalCapacity). |
| CpuCores              | Number of CPU cores in the BE node.                          |

Example 3: Shows the information of databases in the cluster.

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

| **Return**               | **Description**                                   |
| ------------------------ | ------------------------------------------------- |
| DbId                     | Database ID.                                      |
| DbName                   | Database name.                                    |
| TableNum                 | Number of tables in the database.                 |
| Quota                    | Storage quota of the database.                    |
| LastConsistencyCheckTime | The last time when consistency check is executed. |
| ReplicaQuota             | Data replica quota of the database.               |

Example 4: Shows the information of jobs in the cluster.

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

| **Return** | **Description**                    |
| ---------- | ---------------------------------- |
| DbId       | Database ID.                       |
| DbName     | Database name.                     |
| JobType    | Job type.                          |
| Pending    | Number of jobs that are pending.   |
| Running    | Number of jobs that are running.   |
| Finished   | Number of jobs that are finished.  |
| Cancelled  | Number of jobs that are cancelled. |
| Total      | Total number of jobs.              |

Example 5: Shows the statistics of each database in the cluster.

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

| **Return**            | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| DbId                  | Database ID.                                                 |
| DbName                | Database name.                                               |
| TableNum              | Number of tables in the database.                            |
| PartitionNum          | Number of partitions in the database.                        |
| IndexNum              | Number of indexes in the database.                           |
| TabletNum             | Number of tablets in the database.                           |
| ReplicaNum            | Number of replicas in the database.                          |
| UnhealthyTabletNum    | Number of unfinished (unhealthy) tablets in the database during data redistribution. |
| InconsistentTabletNum | Number of inconsistent tablets in the database.              |
| CloningTabletNum      | Number of tablets that are being cloned in the database.     |
| ErrorStateTabletNum   | In a Primary Key type table, the number of tablets in Error state. |
| ErrorStateTablets     | In a Primary Key type table, the IDs of the tablets in Error state. |

Example 6: Shows the total number of all generic tasks and the failed tasks in the cluster.

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

| **Return** | **Description**         |
| ---------- | ----------------------- |
| TaskType   | Task type.              |
| FailedNum  | Number of failed tasks. |
| TotalNum   | Total number of tasks.  |

Example 7: Shows the information of FE nodes in the cluster.

```Plain
mysql> SHOW PROC '/frontends';
+----------------------------------+---------------+-------------+----------+-----------+---------+----------+------------+-------+-------+-------------------+---------------+----------+---------------+-----------+---------+
| Name                             | IP            | EditLogPort | HttpPort | QueryPort | RpcPort | Role     | ClusterId  | Join  | Alive | ReplayedJournalId | LastHeartbeat | IsHelper | ErrMsg        | StartTime | Version |
+----------------------------------+---------------+-------------+----------+-----------+---------+----------+------------+-------+-------+-------------------+---------------+----------+---------------+-----------+---------+
| xxx.xx.xx.xxx_9009_1600088918395 | xxx.xx.xx.xxx | 9009        | 7390     | 0         | 0       | FOLLOWER | 1747363037 | false | false | 0                 | NULL          | true     | got exception | NULL      | NULL    |
+----------------------------------+---------------+-------------+----------+-----------+---------+----------+------------+-------+-------+-------------------+---------------+----------+---------------+-----------+---------+
```

| **Return**        | **Description**                                        |
| ----------------- | ------------------------------------------------------ |
| Name              | FE node name.                                          |
| IP                | IP address of the FE node.                             |
| EditLogPort       | Port for communication between FE nodes.               |
| HttpPort          | HTTP Server port of the FE node.                       |
| QueryPort         | MySQL Server port of the FE node.                      |
| RpcPort           | RPC port of the FE node.                               |
| Role              | Role of the FE node (Leader, Follower, or Observer).   |
| ClusterId         | Cluster ID.                                            |
| Join              | If the FE node has joined a cluster.                   |
| Alive             | If the FE node is alive.                               |
| ReplayedJournalId | The largest metadata ID that the FE node has replayed. |
| LastHeartbeat     | The last time when the FE node sent a heartbeat.       |
| IsHelper          | If the FE node is the BDBJE helper node.               |
| ErrMsg            | Error messages in the FE node.                         |
| StartTime         | Time when the FE node is started.                      |
| Version           | StarRocks version of the FE node.                      |

Example 8: Shows the information of Broker nodes in the cluster.

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

| **Return**     | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| Name           | Broker node name.                                            |
| IP             | IP address of the broker node.                               |
| Port           | Thrift Server port of the broker node. The port is used to receive requests. |
| Alive          | If the broker node is alive.                                 |
| LastStartTime  | The last time when the broker node was started.              |
| LastUpdateTime | The last time when the broker node was updated.              |
| ErrMsg         | Error message in the broker node.                            |

Example 9: Shows the information of resources in the cluster.

```Plain
mysql> SHOW PROC '/resources';
+-------------------------+--------------+---------------------+------------------------------+
| Name                    | ResourceType | Key                 | Value                        |
+-------------------------+--------------+---------------------+------------------------------+
| hive_resource_stability | hive         | hive.metastore.uris | thrift://xxx.xx.xxx.xxx:9083 |
| hive2                   | hive         | hive.metastore.uris | thrift://xxx.xx.xx.xxx:9083  |
+-------------------------+--------------+---------------------+------------------------------+
```

| **Return**   | **Description** |
| ------------ | --------------- |
| Name         | Resource name.  |
| ResourceType | Resource type.  |
| Key          | Resource key.   |
| Value        | Resource value. |

Example 10: Shows the information of transactions in the cluster.

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

| **Return** | **Description**               |
| ---------- | ----------------------------- |
| DbId       | Database ID.                  |
| DbName     | Database name.                |
| State      | The state of the transaction. |
| Number     | Number of transactions.       |

Example 11: Shows the monitoring information in the cluster.

```Plain
mysql> SHOW PROC '/monitor';
+------+------+
| Name | Info |
+------+------+
| jvm  |      |
+------+------+
```

| **Return** | **Description**  |
| ---------- | ---------------- |
| Name       | JVM name.        |
| Info       | JVM information. |

Example 12: Shows the load balance information in the cluster.

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

| **Return** | **Description**                                  |
| ---------- | ------------------------------------------------ |
| Item       | Sub-command item in `cluster_balance `.          |
| Number     | Number of each sub-command in `cluster_balance`. |

Example 13: Shows the information of Colocate Join groups in the cluster.

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

| **Return**     | **Description**                                 |
| -------------- | ----------------------------------------------- |
| GroupId        | Colocate Join Group ID.                         |
| GroupName      | Colocate Join Group name.                       |
| TableIds       | IDs of tables in the Colocate Join Group.       |
| BucketsNum     | Buckets in the Colocate Join Group.             |
| ReplicationNum | Replications in the Colocate Join Group.        |
| DistCols       | Distribution column of the Colocate Join Group. |
| IsStable       | If the Colocate Join Group is stable.           |

Example 14: Shows the information of catalogs in the cluster.

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

| **Return** | **Description**           |
| ---------- | ------------------------- |
| Catalog    | Catalog name.             |
| Type       | Catalog type.             |
| Comment    | Comments for the catalog. |
