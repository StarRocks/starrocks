---
displayed_sidebar: "Chinese"
---

# SHOW TABLET

## 功能

查看 Tablet 相关信息。

> **注意**
>
> 从 3.0 版本开始，该操作需要 SYSTEM 级 OPERATE 权限，以及对应表的 SELECT 权限。2.5 及之前版本，该操作需要 ADMIN_PRIV 权限。

## 语法

### 查看某张表或某个分区内所有 Tablet 的信息

您也可以指定 WHERE 子句来过滤符合条件的 Tablet。

```sql
SHOW TABLET
FROM [<db_name>.]<table_name>
[PARTITION(<partition_name>, ...]
[
WHERE
    [version = <version_number>] 
    [[AND] backendid = <backend_id>] 
    [[AND] STATE = "NORMAL"|"ALTER"|"CLONE"|"DECOMMISSION"]
]
[ORDER BY <field_name> [ASC | DESC]]
[LIMIT [<offset>,]<limit>]
```

| **参数**       | **必选** | **说明**                                                     |
| -------------- | -------- | ------------------------------------------------------------ |
| db_name        | 否       | 数据库名称。如不指定该参数，默认使用当前数据库。                     |
| table_name     | 是       | 表名。必须指定，否则报错。                                                     |
| partition_name | 否       | 分区名。                                                     |
| version_number | 否       | 数据版本号。                                                   |
| backend_id     | 否       | Tablet 副本所在的 BE 的 ID。                                 |
| STATE          | 否       | Tablet 副本的状态。<ul><li>`NORMAL`：副本处于正常状态。</li><li>`ALTER`：副本正在做 Rollup 或 schema change。</li><li>`CLONE`：副本处于 clone 状态（未完成 clone 的副本暂不可用）。</li><li>`DECOMMISSION`：副本处于 DECOMMISSION 状态（下线）。</li></ul> |
| field_name     | 否       | 将返回结果按照指定字段升序或降序排列。`SHOW TABLET FROM <table_name>` 返回的字段都可以作为排序字段。<ul><li>如要升序排列，指定 `ORDER BY field_name ASC`。</li><li>如要降序排列，指定 `ORDER BY field_name DESC`。</li></ul> |
| offset         | 否       | 返回结果中跳过的 Tablet 的数量，默认值为 0。例如 `OFFSET 5` 表示跳过前 5 个 Tablet，返回剩下的结果。 |
| limit          | 否       | 查看指定数量的 Tablet。例如 `LIMIT 10` 会显示 10 个 Tablet 的信息。如果不指定该参数，则默认显示所有符合筛选条件的 Tablet。 |

### 查看单个 Tablet 的信息

通过 `SHOW TABLET FROM <table_name>` 获取了所有 Tablet ID 后，您可以只查询某个 Tablet 的详细信息。

```sql
SHOW TABLET <tablet_id>
```

| **参数**  | **必选** | **说明**       |
| --------- | -------- | -------------- |
| tablet_id | 是       | Tablet 的 ID |

## 返回结果说明

### 查看某张表或分区内的所有 tablet

```plain
+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+------------------+--------------+----------+----------+-------------------+
| TabletId | ReplicaId | BackendId | SchemaHash | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | DataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion | CheckVersionHash | VersionCount | PathHash | MetaUrl  | CompactionStatus  |
+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+------------------+--------------+----------+----------+-------------------+
```

| **字段**                | **说明**                        |
| ----------------------- | ------------------------------- |
| TabletId                | Table 的 ID。                   |
| ReplicaId               | 副本 ID。                      |
| BackendId               | 副本所在的 BE 的 ID。           |
| SchemaHash              | Schema hash（随机生成）。        |
| Version                 | 数据版本号。                      |
| VersionHash             | 数据版本号的 hash。               |
| LstSuccessVersion       | 最后一次 load 成功的版本。        |
| LstSuccessVersionHash   | 最后一次 load 成功的版本的 hash。 |
| LstFailedVersion        | 最后一次 load 失败的版本。`-1` 表示不存在失败的版本。 |
| LstFailedVersionHash    | 最后一次 load 失败的版本的 hash。 |
| LstFailedTime           | 最后一次 load 失败的时间。`NULL` 表示没有失败。        |
| DataSize                | 该 Tablet 上的数据大小。             |
| RowCount                | 该 Tablet 上的数据行数。             |
| State                   | Tablet 的副本状态。              |
| LstConsistencyCheckTime | 最后一次一致性检查的时间。`NULL` 表示没有进行一致性检查.  |
| CheckVersion            | 一致性检查的版本。`-1` 表示不存在检查版本。    |
| CheckVersionHash        | 一致性检查的版本的 hash。         |
| VersionCount            | 数据版本数。                      |
| PathHash                | Tablet 存储目录的 hash。        |
| MetaUrl                 | 通过 URL 查询更多的 meta 信息。     |
| CompactionStatus        | 通过 URL 查询 Compaction 状态。    |

### 查看单个 tablet

```Plain
+--------+-----------+---------------+-----------+------+---------+-------------+---------+--------+-----------+
| DbName | TableName | PartitionName | IndexName | DbId | TableId | PartitionId | IndexId | IsSync | DetailCmd |
+--------+-----------+---------------+-----------+------+---------+-------------+---------+--------+-----------+
```

| **字段**      | **说明**                                                     |
| ------------- | ------------------------------------------------------------ |
| DbName        | Tablet 所属数据库的名称。                                    |
| TableName     | Tablet 所属表的名称。                                        |
| PartitionName | Tablet 所属分区的名称。                                      |
| IndexName     | 索引名称。                                                     |
| DbId          | 数据库 ID。                                                  |
| TableId       | 表 ID。                                                      |
| PartitionId   | 分区 ID。                                                    |
| IndexId       | 索引 ID。                                                      |
| IsSync        | 检查 Tablet 上的数据是否与表 meta 里的数据一致。`false` 表示检查项数据有缺失。`true` 表示检查项数据正常，即代表 Tablet 正常。 |
| DetailCmd     | 通过 URL 查询更多信息。                                      |

## 示例

在数据库 `example_db` 下创建表 `test_show_tablet`。

```sql
CREATE TABLE `test_show_tablet` (
  `k1` date NULL COMMENT "",
  `k2` datetime NULL COMMENT "",
  `k3` char(20) NULL COMMENT "",
  `k4` varchar(20) NULL COMMENT "",
  `k5` boolean NULL COMMENT "",
  `k6` tinyint(4) NULL COMMENT "",
  `k7` smallint(6) NULL COMMENT "",
  `k8` int(11) NULL COMMENT "",
  `k9` bigint(20) NULL COMMENT "",
  `k10` largeint(40) NULL COMMENT "",
  `k11` float NULL COMMENT "",
  `k12` double NULL COMMENT "",
  `k13` decimal128(27, 9) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`)
(PARTITION p20210101 VALUES [("2021-01-01"), ("2021-01-02")),
PARTITION p20210102 VALUES [("2021-01-02"), ("2021-01-03")),
PARTITION p20210103 VALUES [("2021-01-03"), ("2021-01-04")),
PARTITION p20210104 VALUES [("2021-01-04"), ("2021-01-05")),
PARTITION p20210105 VALUES [("2021-01-05"), ("2021-01-06")),
PARTITION p20210106 VALUES [("2021-01-06"), ("2021-01-07")),
PARTITION p20210107 VALUES [("2021-01-07"), ("2021-01-08")),
PARTITION p20210108 VALUES [("2021-01-08"), ("2021-01-09")),
PARTITION p20210109 VALUES [("2021-01-09"), ("2021-01-10")))
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`);
```

- 查看指定数据库下指定表的所有 Tablet 信息。以下示例仅截取其中一行 tablet 信息来说明。

    ```plain
    mysql> show tablet from example_db.test_show_tablet\G
    *************************** 1. row ***************************
               TabletId: 9588955
              ReplicaId: 9588956
              BackendId: 10004
             SchemaHash: 0
                Version: 1
            VersionHash: 0
      LstSuccessVersion: 1
  LstSuccessVersionHash: 0
       LstFailedVersion: -1
   LstFailedVersionHash: 0
          LstFailedTime: NULL
               DataSize: 0B
               RowCount: 0
                  State: NORMAL
  LstConsistencyCheckTime: NULL
           CheckVersion: -1
       CheckVersionHash: 0
           VersionCount: 1
               PathHash: 0
                MetaUrl: http://172.26.92.141:8038/api/meta/header/9588955
       CompactionStatus: http://172.26.92.141:8038/api/compaction/show?tablet_id=9588955
    ```

- 查看 id 为 9588955 的 Tablet 的信息。

    ```plain
    mysql> show tablet 9588955\G
    *************************** 1. row ***************************
       DbName: example_db
    TableName: test_show_tablet
    PartitionName: p20210103
    IndexName: test_show_tablet
         DbId: 11145
      TableId: 9588953
  PartitionId: 9588946
      IndexId: 9588954
       IsSync: true
    DetailCmd: SHOW PROC '/dbs/11145/9588953/partitions/9588946/9588954/9588955';
    ```

- 查看表中分区 `p20210103` 的 Tablet 信息。

    ```sql
        SHOW TABLET FROM test_show_tablet partition(p20210103);
    ```

- 返回表中 10 个 Tablet 的信息。

    ```sql
        SHOW TABLET FROM test_show_tablet limit 10;
    ```

- 从偏移位置 5 开始获取 10 个 Tablet 的信息。

    ```sql
        SHOW TABLET FROM test_show_tablet limit 5,10;
    ```

- 按照 `backendid`，`version`，`state` 字段进行过滤。

    ```sql
        SHOW TABLET FROM test_show_tablet
        WHERE backendid = 10004 and version = 1 and state = "NORMAL";
    ```

- 按照 `version` 字段进行排序。

    ```sql
        SHOW TABLET FROM table_name where backendid = 10004 order by version;
    ```

- 查看 index 名为 `test_show_tablet` 的 Tablet 的信息。

    ```sql
        SHOW TABLET FROM test_show_tablet where indexname = "test_show_tablet";
    ```
