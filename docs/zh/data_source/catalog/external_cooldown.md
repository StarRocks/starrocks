---
displayed_sidebar: docs
---

# 分区降冷

分区降冷是将本地OLAP表分区导出到外部存储(比如iceberg表)的过程，降冷之后的数据可以通过连外部catalog查询(后续PR支持使用一张表查询降冷后的数据)。
本文介绍如何配置降冷相关属性及触发分区下沉。

## 分区降冷表属性

- `external_cooldown_target` 降冷目标表，格式: `{catalog}.{db}.{tbl}`
- `external_cooldown_schedule` 降冷调度配置，比如配置 `START 23:00 END 08:00 EVERY INTERVAL 1 MINUTE` 表示晚上23:00到第二天早上8:00（不包含08:00）这个时间范围可以执行自动降冷任务, 默认空，不会自动生成降冷任务
- `external_cooldown_wait_second` 降冷等待时间，分区最后一次修改(以`VisibleVersionTime`为准)之后过`external_cooldown_wait_second`时间之后才可以降冷，默认值0, external_cooldown_wait_second不大于0时不会自动降冷

```SQL
ALTER TABLE olap_tbl SET (
    'external_cooldown_target'='iceberg.db1.tbl1',
    'external_cooldown_schedule'='START 01:00 END 08:00 EVERY INTERVAL 1 MINUTE',
    'external_cooldown_wait_second'='3600'
);
```

   或

```SQL
alter table olap_tbl set('external_cooldown_target'='iceberg.db1.tbl1');
alter table olap_tbl set('external_cooldown_schedule'='START 01:00 END 08:00 EVERY INTERVAL 1 MINUTE');
alter table olap_tbl set('external_cooldown_wait_second'='3600');
```

2. 可以通过查看建表语句查看分区降冷相关属性

```SQL
mysql> show create table olap_tbl \G
*************************** 1. row ***************************
       Table: olap_tbl
Create Table: CREATE TABLE `olap_tbl` (
  `event` varchar(65533) NULL COMMENT "",
  `dteventtime` datetime NULL COMMENT "",
  `f_date` date NULL COMMENT "",
  `f_int` int(11) NULL COMMENT "",
  `f_bigint` bigint(20) NULL COMMENT "",
  `f_double` double NULL COMMENT "",
  `f_boolean` boolean NULL COMMENT "",
  `f_decimal` decimal(12, 4) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`event`, `dteventtime`)
COMMENT "OLAP"
PARTITION BY RANGE(`dteventtime`)
(PARTITION p20220417 VALUES [("2022-04-17 00:00:00"), ("2022-04-18 00:00:00")),
PARTITION p20220418 VALUES [("2022-04-18 00:00:00"), ("2022-04-19 00:00:00")),
PARTITION p20220419 VALUES [("2022-04-19 00:00:00"), ("2022-04-20 00:00:00")),
PARTITION p20220420 VALUES [("2022-04-20 00:00:00"), ("2022-04-21 00:00:00")),
PARTITION p20220421 VALUES [("2022-04-21 00:00:00"), ("2022-04-22 00:00:00")))
DISTRIBUTED BY HASH(`event`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"dynamic_partition.buckets" = "1",
"dynamic_partition.enable" = "false",
"dynamic_partition.end" = "2",
"dynamic_partition.history_partition_num" = "0",
"dynamic_partition.prefix" = "p",
"dynamic_partition.start" = "-2147483648",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"external_cooldown_schedule" = "START 14:00 END 19:59 EVERY INTERVAL 180 SECOND",
"external_cooldown_target" = "iceberg.db1.tbl1",
"external_cooldown_wait_second" = "0",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
1 row in set (0.01 sec)
```

## 手动触发下沉

- 降冷整个表

```SQL
cooldown table olap_tbl;
```

- 降冷分区时间范围在2022-04-18 00:00:00到2022-04-19 00:00:00之间的分区, 如果已降冷不会重复降冷，空分区不会降冷
 
```SQL
cooldown table olap_tbl partition start ('2022-04-18 00:00:00') end ('2022-04-19 00:00:00');
```

- 强制降冷分区时间范围在2022-04-18 00:00:00到2022-04-19 00:00:00之间的分区, 如果已下沉不会重复降冷，空分区不会降冷

```SQL
cooldown table olap_tbl partition start ('2022-04-18 00:00:00') end ('2022-04-19 00:00:00') force;
```

## 查看降冷后分区状态

```SQL
mysql> show partitions from olap_tbl where partitionname='p20220419' \G
*************************** 1. row ***************************
                               PartitionId: 10743
                             PartitionName: p20220419
                            VisibleVersion: 2
                        VisibleVersionTime: 2024-02-28 16:52:54
                        VisibleVersionHash: 0
                                     State: NORMAL
                              PartitionKey: dteventtime
                                     Range: [types: [DATETIME]; keys: [2022-04-19 00:00:00]; ..types: [DATETIME]; keys: [2022-04-20 00:00:00]; )
                           DistributionKey: event
                                   Buckets: 1
                            ReplicationNum: 1
                             StorageMedium: HDD
                              CooldownTime: 9999-12-31 23:59:59
                  LastConsistencyCheckTime: NULL
                                  DataSize: 2.1KB
                                IsInMemory: false
                                  RowCount: 0
                               DataVersion: 2
                              VersionEpoch: 313464788426424322
                            VersionTxnType: TXN_NORMAL
                ExternalCoolDownSyncedTime: 2024-02-28 16:52:54
      ExternalCoolDownConsistencyCheckTime: NULL
ExternalCoolDownConsistencyCheckDifference: NULL
1 row in set (0.00 sec)
```

## 查看降冷task

通过查询tasks和task_runs表可查看降冷任务执行情况

```SQL
select * from information_schema.tasks;
select * from information_schema.task_runs;
```
