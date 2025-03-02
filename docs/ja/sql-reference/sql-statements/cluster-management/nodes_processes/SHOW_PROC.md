---
displayed_sidebar: docs
---

# SHOW PROC

## Description

StarRocks クラスターの特定の指標を表示します。

:::tip

この操作には SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## Syntax

```SQL
SHOW PROC { '/backends' | '/compute_nodes' | '/dbs' | '/jobs' 
          | '/statistic' | '/tasks' | '/frontends' | '/brokers' 
          | '/resources' | '/load_error_hub' | '/transactions' 
          | '/monitor' | '/current_queries' | '/current_backend_instances' 
          | '/cluster_balance' | '/routine_loads' | '/colocation_group' 
          | '/catalog' | '/replications' }
```

## Parameters

| **Parameter**                | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| '/backends'                  | クラスター内の BE ノードの情報を表示します。                 |
| '/compute_nodes'             | クラスター内の CN ノードの情報を表示します。                 |
| '/dbs'                       | クラスター内のデータベースの情報を表示します。               |
| '/jobs'                      | クラスター内のジョブの情報を表示します。                     |
| '/statistic'                 | クラスター内の各データベースの統計情報を表示します。         |
| '/tasks'                     | クラスター内のすべての一般的なタスクと失敗したタスクの総数を表示します。 |
| '/frontends'                 | クラスター内の FE ノードの情報を表示します。                 |
| '/brokers'                   | クラスター内の Broker ノードの情報を表示します。             |
| '/resources'                 | クラスター内のリソースの情報を表示します。                   |
| '/load_error_hub'            | ロードジョブのエラーメッセージを管理するために使用されるクラスターの Load Error Hub の設定を表示します。 |
| '/transactions'              | クラスター内のトランザクションの情報を表示します。           |
| '/monitor'                   | クラスター内の監視情報を表示します。                         |
| '/current_queries'           | 現在の FE ノードで実行中のクエリの情報を表示します。         |
| '/current_backend_instances' | クラスター内でリクエストを処理している BE ノードを表示します。 |
| '/cluster_balance'           | クラスター内のロードバランス情報を表示します。               |
| '/routine_loads'             | クラスター内の Routine Load の情報を表示します。             |
| '/colocation_group'          | クラスター内の Colocate Join グループの情報を表示します。    |
| '/catalog'                   | クラスター内のカタログの情報を表示します。                   |
| '/replications'              | クラスター内のデータレプリケーションタスクの情報を表示します。|

## Examples

Example 1: クラスター内の BE ノードの情報を表示します。

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: xxx.xx.92.200
        HeartbeatPort: 9354
               BePort: 9360
             HttpPort: 8338
             BrpcPort: 8360
        LastStartTime: 2023-04-21 09:56:10
        LastHeartbeat: 2023-04-21 09:56:10
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 2199
     DataUsedCapacity: 0.000 
        AvailCapacity: 584.578 GB
        TotalCapacity: 1.968 TB
              UsedPct: 71.00 %
       MaxDiskUsedPct: 71.00 %
               ErrMsg: 
              Version: BRANCH-3.0-RELEASE-8eb8705
               Status: {"lastSuccessReportTabletsTime":"N/A"}
    DataTotalCapacity: 584.578 GB
          DataUsedPct: 0.00 %
             CpuCores: 16
    NumRunningQueries: 0
           MemUsedPct: 0.52 %
           CpuUsedPct: 0.0 %
```

| **Return**            | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| BackendId             | BE ノードの ID。                                             |
| IP                    | BE ノードの IP アドレス。                                    |
| HeartbeatPort         | BE ノードのハートビートサービスのポート。                    |
| BePort                | BE ノードの Thrift サーバーポート。                          |
| HttpPort              | BE ノードの HTTP サーバーポート。                            |
| BrpcPort              | BE ノードの bRPC ポート。                                    |
| LastStartTime         | BE ノードが最後に開始された時間。                            |
| LastHeartbeat         | BE ノードが最後にハートビートを受信した時間。                |
| Alive                 | BE ノードが生存しているかどうか。                            |
| SystemDecommissioned  | BE ノードがシステムから除籍されているかどうか。              |
| ClusterDecommissioned | BE ノードがクラスター内で除籍されているかどうか。            |
| TabletNum             | BE ノード内のタブレットの数。                                |
| DataUsedCapacity      | BE ノード内でデータに使用されているストレージ容量。          |
| AvailCapacity         | BE ノード内の利用可能なストレージ容量。                      |
| TotalCapacity         | BE ノード内の総ストレージ容量。                              |
| UsedPct               | BE ノード内でストレージ容量が使用されている割合。            |
| MaxDiskUsedPct        | BE ノード内でストレージ容量が使用されている最大割合。        |
| ErrMsg                | BE ノード内のエラーメッセージ。                              |
| Version               | BE ノードの StarRocks バージョン。                           |
| Status                | BE ノードのステータス情報。タブレットが最後に報告された時間を含む。 |
| DataTotalCapacity     | 使用済みおよび利用可能なデータストレージ容量の合計。`DataUsedCapacity` と `AvailCapacity` の合計。 |
| DataUsedPct           | データストレージが総データ容量を占める割合 (DataUsedCapacity/DataTotalCapacity)。 |
| CpuCores              | BE ノード内の CPU コア数。                                   |
| NumRunningQueries     | クラスター内で現在実行中のクエリの数。                       |
| MemUsedPct            | 現在のメモリ使用率。                                         |
| CpuUsedPct            | 現在の CPU 使用率。                                          |

Example 2: クラスター内のデータベースの情報を表示します。

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
| DbId                     | データベース ID。                                 |
| DbName                   | データベース名。                                  |
| TableNum                 | データベース内のテーブル数。                      |
| Quota                    | データベースのストレージクォータ。                |
| LastConsistencyCheckTime | 一貫性チェックが最後に実行された時間。            |
| ReplicaQuota             | データベースのデータレプリカクォータ。            |

Example 3: クラスター内のジョブの情報を表示します。

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
| DbId       | データベース ID。                   |
| DbName     | データベース名。                    |
| JobType    | ジョブタイプ。                      |
| Pending    | 保留中のジョブの数。                |
| Running    | 実行中のジョブの数。                |
| Finished   | 完了したジョブの数。                |
| Cancelled  | キャンセルされたジョブの数。        |
| Total      | ジョブの総数。                      |

Example 4: クラスター内の各データベースの統計情報を表示します。

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
| DbId                  | データベース ID。                                            |
| DbName                | データベース名。                                             |
| TableNum              | データベース内のテーブル数。                                 |
| PartitionNum          | データベース内のパーティション数。                           |
| IndexNum              | データベース内のインデックス数。                             |
| TabletNum             | データベース内のタブレット数。                               |
| ReplicaNum            | データベース内のレプリカ数。                                 |
| UnhealthyTabletNum    | データ再配布中に未完了（不健全）なタブレットの数。           |
| InconsistentTabletNum | データベース内の不整合なタブレットの数。                     |
| CloningTabletNum      | データベース内でクローンされているタブレットの数。           |
| ErrorStateTabletNum   | Primary Key タイプのテーブルで、エラーステートのタブレットの数。 |
| ErrorStateTablets     | Primary Key タイプのテーブルで、エラーステートのタブレットの ID。 |

Example 5: クラスター内のすべての一般的なタスクと失敗したタスクの総数を表示します。

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
| TaskType   | タスクタイプ。           |
| FailedNum  | 失敗したタスクの数。     |
| TotalNum   | タスクの総数。           |

Example 6: クラスター内の FE ノードの情報を表示します。

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
| Name              | FE ノード名。                                          |
| IP                | FE ノードの IP アドレス。                              |
| EditLogPort       | FE ノード間の通信ポート。                              |
| HttpPort          | FE ノードの HTTP サーバーポート。                      |
| QueryPort         | FE ノードの MySQL サーバーポート。                     |
| RpcPort           | FE ノードの RPC ポート。                               |
| Role              | FE ノードの役割 (Leader, Follower, または Observer)。 |
| ClusterId         | クラスター ID。                                        |
| Join              | FE ノードがクラスターに参加しているかどうか。          |
| Alive             | FE ノードが生存しているかどうか。                      |
| ReplayedJournalId | FE ノードが再生した最大のメタデータ ID。               |
| LastHeartbeat     | FE ノードが最後にハートビートを送信した時間。          |
| IsHelper          | FE ノードが BDBJE ヘルパーノードかどうか。             |
| ErrMsg            | FE ノード内のエラーメッセージ。                        |
| StartTime         | FE ノードが開始された時間。                            |
| Version           | FE ノードの StarRocks バージョン。                     |

Example 7: クラスター内の Broker ノードの情報を表示します。

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
| Name           | Broker ノード名。                                            |
| IP             | Broker ノードの IP アドレス。                                |
| Port           | Broker ノードの Thrift サーバーポート。このポートはリクエストを受信するために使用されます。 |
| Alive          | Broker ノードが生存しているかどうか。                        |
| LastStartTime  | Broker ノードが最後に開始された時間。                        |
| LastUpdateTime | Broker ノードが最後に更新された時間。                        |
| ErrMsg         | Broker ノード内のエラーメッセージ。                          |

Example 8: クラスター内のリソースの情報を表示します。

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
| Name         | リソース名。    |
| ResourceType | リソースタイプ。|
| Key          | リソースキー。  |
| Value        | リソース値。    |

Example 9: クラスター内のトランザクションの情報を表示します。

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
| DbId       | データベース ID。              |
| DbName     | データベース名。               |
| State      | トランザクションの状態。       |
| Number     | トランザクションの数。         |

Example 10: クラスター内の監視情報を表示します。

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
| Name       | JVM 名。         |
| Info       | JVM 情報。       |

Example 11: クラスター内のロードバランス情報を表示します。

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
| Item       | `cluster_balance` のサブコマンド項目。 <ul><li>cluster_load_stat: クラスターの現在のロードステータス。</li><li>working_slots: 現在利用可能な作業スロットの数。</li><li>sched_stat: スケジューラの現在のステータス。</li><li>priority_repair: 優先されるタブレット修復タスクの数。</li><li>pending_tablets: 処理待ちのタブレットの数。</li><li>running_tablets: 現在修復中のタブレットの数。</li><li>history_tablets: 過去に修復されたタブレットの総数。</li></ul>         |
| Number     | `cluster_balance` の各サブコマンドの数。         |

Example 12: クラスター内の Colocate Join グループの情報を表示します。

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
| GroupId        | Colocate Join グループ ID。                     |
| GroupName      | Colocate Join グループ名。                      |
| TableIds       | Colocate Join グループ内のテーブル ID。         |
| BucketsNum     | Colocate Join グループ内のバケット数。          |
| ReplicationNum | Colocate Join グループ内のレプリケーション数。  |
| DistCols       | Colocate Join グループの分散カラム。            |
| IsStable       | Colocate Join グループが安定しているかどうか。  |

Example 13: クラスター内のカタログの情報を表示します。

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
| Catalog    | カタログ名。               |
| Type       | カタログタイプ。           |
| Comment    | カタログに関するコメント。 |

Example 14: クラスター内のレプリケーションタスクの情報を表示します。

```Plain
mysql> SHOW PROC '/replications';
+-------------------------------------------------+------------+---------+-------+---------------------+---------------------+-----------+----------+-------+
| JobID                                           | DatabaseID | TableID | TxnID | CreatedTime         | FinishedTime        | State     | Progress | Error |
+-------------------------------------------------+------------+---------+-------+---------------------+---------------------+-----------+----------+-------+
| FAILOVER_GROUP_group1-11006-11010-1725593360156 | 11006      | 11010   | 99    | 2024-09-06 11:29:20 | 2024-09-06 11:29:21 | COMMITTED |          |       |
| FAILOVER_GROUP_group1-11006-11009-1725593360161 | 11006      | 11009   | 98    | 2024-09-06 11:29:20 | 2024-09-06 11:29:21 | COMMITTED |          |       |
| FAILOVER_GROUP_group1-11006-11074-1725593360161 | 11006      | 11074   | 100   | 2024-09-06 11:29:20 | 2024-09-06 11:29:21 | COMMITTED |          |       |
| FAILOVER_GROUP_group1-11006-12474-1725593360250 | 11006      | 12474   | 102   | 2024-09-06 11:29:20 | 2024-09-06 11:29:24 | COMMITTED |          |       |
| FAILOVER_GROUP_group1-11006-11024-1725593360293 | 11006      | 11024   | 101   | 2024-09-06 11:29:20 | 2024-09-06 11:29:24 | COMMITTED |          |       |
| FAILOVER_GROUP_group1-11006-13861-1725607270963 | 11006      | 13861   | 627   | 2024-09-06 15:21:10 | 2024-09-06 15:21:14 | COMMITTED |          |       |
+-------------------------------------------------+------------+---------+-------+---------------------+---------------------+-----------+----------+-------+
```

| **Return**   | **Description**                 |
| ------------ | ------------------------------- |
| JobID        | ジョブ ID。                      |
| DatabaseID   | データベース ID。               |
| TableID      | テーブル ID。                   |
| TxnID        | トランザクション ID。           |
| CreatedTime  | タスクが作成された時間。        |
| FinishedTime | タスクが終了した時間。          |
| State        | タスクのステータス。 有効な値: INITIALIZING, SNAPSHOTING, REPLICATING, COMMITTED, ABORTED. |
| Progress     | タスクの進捗。                  |
| Error        | エラーメッセージ（ある場合）。  |
```