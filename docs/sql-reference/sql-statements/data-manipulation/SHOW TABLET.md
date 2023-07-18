# SHOW TABLET

## Description

Displays tablet related information.

> **NOTE**
>
<<<<<<< HEAD
> This operation requires the SELECT_PRIV privilege.
=======
> For v3.0 and later, this operation requires the SYSTEM-level OPERATE privilege and TABLE-level SELECT privilege. For v2.5 and earlier, this operation requires the ADMIN_PRIV privilege.
>>>>>>> 73797fe93e ([Doc] add func/datatype overview (#27428))

## Syntax

### Query information of tablets in a table or a partition

```sql
SHOW TABLET
FROM [<db_name>.]<table_name>
[PARTITION(<partition_name>, ...]
[
WHERE [version = <version_number>] 
    [[AND] backendid = <backend_id>] 
    [[AND] STATE = "NORMAL"|"ALTER"|"CLONE"|"DECOMMISSION"]
]
[ORDER BY <field_name> [ASC | DESC]]
[LIMIT [<offset>,]<limit>]
```

| **Parameter**       | **Required** | **Description**                                                     |
| -------------- | -------- | ------------------------------------------------------------ |
| db_name        | No       | The database name. If you do not specify this parameter, the current database is used by default.                     |
| table_name     | Yes       | The name of the table from which you want to query tablet information. You must specify this parameter. Otherwise, an error is returned.                                                     |
| partition_name | No       | The name of the partition from which you want to query tablet information.                                                     |
| version_number | No       | The data version number.                                                   |
| backend_id     | No       | The ID of the BE where the replica of the tablet is located.                                 |
| STATE          | No       | The status of tablet replicas. <ul><li>`NORMAL`: The replica is normal.</li><li>`ALTER`: A Rollup or schema change is being performed on the replica.</li><li>`CLONE`: The replica is being cloned. (Replicas in this state are not available for use). </li><li>`DECOMMISSION`: The replica is being decommissioned. </li></ul> |
| field_name     | No       | The field by which the results are sorted. All the fields returned by `SHOW TABLET FROM <table_name>` are sortable.<ul><li>If you want to display results in ascending order, use `ORDER BY field_name ASC`. </li><li>If you want to display results in descending order, use `ORDER BY field_name DESC`.</li></ul> |
| offset         | No       | The number of tablets to skip from the results. For example, `OFFSET 5` means to skip the first five tablets. Default value: 0. |
| limit          | No       | The number of tablets to return. For example, `LIMIT 10` means to return only 10 tablets. If this parameter is not specified, all the tablets that meet the filter conditions are returned. |

### Query information of a single tablet

After obtaining all tablet IDs using `SHOW TABLET FROM <table_name>`, you can query the information of a single tablet.

```sql
SHOW TABLET <tablet_id>
```

| **Parameter**  | **Required** | **Description**       |
| --------- | -------- | -------------- |
| tablet_id | Yes       | Tablet ID |

## Description of return fields

### Query information of tablets in a table or a partition

```plain
+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+------------------+--------------+----------+----------+-------------------+
| TabletId | ReplicaId | BackendId | SchemaHash | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | DataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion | CheckVersionHash | VersionCount | PathHash | MetaUrl  | CompactionStatus  |
+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+------------------+--------------+----------+----------+-------------------+
```

| **Field**                | **Description**                        |
| ----------------------- | ------------------------------- |
| TabletId                | Table ID.                   |
| ReplicaId               | Replica ID.                      |
| BackendId               | The ID of the BE where the replica is located.  |
| SchemaHash              | Schema hash (randomly generated).        |
| Version                 | Data version number.                     |
| VersionHash             | Hash of data version number.              |
| LstSuccessVersion       | The last successfully loaded version.        |
| LstSuccessVersionHash   | The hash of the last successfully loaded version. |
| LstFailedVersion        | The version of the last failed loading. `-1` indicates no version failed to be loaded. |
| LstFailedVersionHash    | The hash of the last failed version. |
| LstFailedTime           | The time of the last failed loading. `NULL` indicates there is no load failure.       |
| DataSize                | Data size of the tablet.          |
| RowCount                | The number of data rows of the tablet.            |
| State                   | Replica status of the tablet.           |
| LstConsistencyCheckTime | The time of the last consistency check. `NULL` indicates no consistency check was performed. |
| CheckVersion            | The data version on which consistency check was performed. `-1` indicates no version was checked.    |
| CheckVersionHash        | The hash of the version on which consistency check was performed.         |
| VersionCount            | The total number of data versions.                      |
| PathHash                | The hash of the directory in which the tablet is stored.        |
| MetaUrl                 | The URL used to query more meta information.     |
| CompactionStatus        | The URL used to query data version compaction status.    |

### Query information of a specific tablet

```Plain
+--------+-----------+---------------+-----------+------+---------+-------------+---------+--------+-----------+
| DbName | TableName | PartitionName | IndexName | DbId | TableId | PartitionId | IndexId | IsSync | DetailCmd |
+--------+-----------+---------------+-----------+------+---------+-------------+---------+--------+-----------+
```

| **Field**      | **Description**                                                     |
| ------------- | ------------------------------------------------------------ |
| DbName        | The name of the database to which the tablet belongs.      |
| TableName     | The name of the table to which the tablet belongs.         |
| PartitionName | The name of the partition to which the tablet belongs.     |
| IndexName     | The index name.                                            |
| DbId          | The database ID.                                           |
| TableId       | The table ID.                                              |
| PartitionId   | The partition ID.                                          |
| IndexId       | The index ID.                                              |
| IsSync        | Whether data on the tablet is consistent with table meta. `true` indicates data is consistent and the tablet is normal. `false` indicates data is missing on the tablet. |
| DetailCmd     | The URL used to query more information.                    |

## Examples

Create table `test_show_tablet` in the database `example_db`.

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

- Example 1: Query information of all the tablets in the specified table. The following example excerpts information of only one tablet from the return information.

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

- Example 2: Query information of tablet 9588955.

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

- Example 3: Query information of tablets in partition `p20210103`.

    ```sql
    SHOW TABLET FROM test_show_tablet partition(p20210103);
    ```

- Example 4: Return information of 10 tablets.

    ```sql
        SHOW TABLET FROM test_show_tablet limit 10;
    ```

- Example 5: Return information of 10 tablets with an offset 5.

    ```sql
    SHOW TABLET FROM test_show_tablet limit 5,10;
    ```

- Example 6: Filter tablets by `backendid`, `version`, and `state`.

    ```sql
        SHOW TABLET FROM test_show_tablet
        WHERE backendid = 10004 and version = 1 and state = "NORMAL";
    ```

- Example 7: Sort tablets by `version`.

    ```sql
        SHOW TABLET FROM table_name where backendid = 10004 order by version;
    ```

- Example 8: Return information of tablets whose index name is `test_show_tablet`.

    ```sql
    SHOW TABLET FROM test_show_tablet where indexname = "test_show_tablet";
    ```
