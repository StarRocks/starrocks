---
displayed_sidebar: docs
---

# SHOW TABLET

## 説明

tablet に関連する情報を表示します。

> **注意**
>
> バージョン 3.0 以降、この操作には SYSTEM レベルの OPERATE 権限と TABLE レベルの SELECT 権限が必要です。バージョン 2.5 以前では、この操作には ADMIN_PRIV 権限が必要です。

## 構文

### テーブルまたはパーティション内の tablets の情報をクエリする

```sql
SHOW TABLET
FROM [<db_name>.]<table_name>
[PARTITION(<partition_name>, ...)]
[
WHERE [version = <version_number>] 
    [[AND] backendid = <backend_id>] 
    [[AND] STATE = "NORMAL"|"ALTER"|"CLONE"|"DECOMMISSION"]
]
[ORDER BY <field_name> [ASC | DESC]]
[LIMIT [<offset>,]<limit>]
```

| **パラメータ**       | **必須** | **説明**                                                     |
| -------------- | -------- | ------------------------------------------------------------ |
| db_name        | いいえ       | データベース名。このパラメータを指定しない場合、デフォルトで現在のデータベースが使用されます。                     |
| table_name     | はい       | tablet 情報をクエリするテーブルの名前。このパラメータを指定しないとエラーが返されます。                                                     |
| partition_name | いいえ       | tablet 情報をクエリするパーティションの名前。                                                     |
| version_number | いいえ       | データのバージョン番号。                                                   |
| backend_id     | いいえ       | tablet のレプリカが配置されている BE の ID。                                 |
| STATE          | いいえ       | tablet レプリカのステータス。 <ul><li>`NORMAL`: レプリカは正常です。</li><li>`ALTER`: レプリカに Rollup または schema change が実行されています。</li><li>`CLONE`: レプリカがクローンされています。（この状態のレプリカは使用できません）。</li><li>`DECOMMISSION`: レプリカが廃止されています。</li></ul> |
| field_name     | いいえ       | 結果をソートするフィールド。`SHOW TABLET FROM <table_name>` で返されるすべてのフィールドはソート可能です。<ul><li>結果を昇順で表示する場合は、`ORDER BY field_name ASC` を使用します。</li><li>結果を降順で表示する場合は、`ORDER BY field_name DESC` を使用します。</li></ul> |
| offset         | いいえ       | 結果からスキップする tablets の数。例えば、`OFFSET 5` は最初の 5 つの tablets をスキップすることを意味します。デフォルト値: 0。 |
| limit          | いいえ       | 返す tablets の数。例えば、`LIMIT 10` は 10 個の tablets のみを返すことを意味します。このパラメータが指定されていない場合、フィルター条件を満たすすべての tablets が返されます。 |

### 単一の tablet の情報をクエリする

`SHOW TABLET FROM <table_name>` を使用してすべての tablet ID を取得した後、単一の tablet の情報をクエリできます。

```sql
SHOW TABLET <tablet_id>
```

| **パラメータ**  | **必須** | **説明**       |
| --------- | -------- | -------------- |
| tablet_id | はい       | Tablet ID |

## 返されるフィールドの説明

### テーブルまたはパーティション内の tablets の情報をクエリする

```plain
+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+------------------+--------------+----------+----------+-------------------+---------------+
| TabletId | ReplicaId | BackendId | SchemaHash | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | DataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion | CheckVersionHash | VersionCount | PathHash | MetaUrl  | CompactionStatus  | DiskRootPath  |
+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+------------------+--------------+----------+----------+-------------------+---------------+
```

| **フィールド**                | **説明**                        |
| ----------------------- | ------------------------------- |
| TabletId                | テーブル ID。                   |
| ReplicaId               | レプリカ ID。                      |
| BackendId               | レプリカが配置されている BE の ID。  |
| SchemaHash              | スキーマハッシュ（ランダムに生成される）。        |
| Version                 | データのバージョン番号。                     |
| VersionHash             | データバージョン番号のハッシュ。              |
| LstSuccessVersion       | 最後に正常にロードされたバージョン。        |
| LstSuccessVersionHash   | 最後に正常にロードされたバージョンのハッシュ。 |
| LstFailedVersion        | 最後にロードに失敗したバージョン。`-1` は失敗したバージョンがないことを示します。 |
| LstFailedVersionHash    | 最後に失敗したバージョンのハッシュ。 |
| LstFailedTime           | 最後にロードに失敗した時間。`NULL` はロード失敗がないことを示します。       |
| DataSize                | tablet のデータサイズ。          |
| RowCount                | tablet のデータ行数。            |
| State                   | tablet のレプリカステータス。           |
| LstConsistencyCheckTime | 最後の整合性チェックの時間。`NULL` は整合性チェックが行われていないことを示します。 |
| CheckVersion            | 整合性チェックが行われたデータバージョン。`-1` はチェックされたバージョンがないことを示します。    |
| CheckVersionHash        | 整合性チェックが行われたバージョンのハッシュ。         |
| VersionCount            | データバージョンの総数。                      |
| PathHash                | tablet が保存されているディレクトリのハッシュ。        |
| MetaUrl                 | より多くのメタ情報をクエリするために使用される URL。     |
| CompactionStatus        | データバージョンの Compaction ステータスをクエリするために使用される URL。    |
| DiskRootPath            | レプリカが配置されているディスク。    |

### 特定の tablet の情報をクエリする

```Plain
+--------+-----------+---------------+-----------+------+---------+-------------+---------+--------+-----------+
| DbName | TableName | PartitionName | IndexName | DbId | TableId | PartitionId | IndexId | IsSync | DetailCmd |
+--------+-----------+---------------+-----------+------+---------+-------------+---------+--------+-----------+
```

| **フィールド**      | **説明**                                                     |
| ------------- | ------------------------------------------------------------ |
| DbName        | tablet が属するデータベースの名前。      |
| TableName     | tablet が属するテーブルの名前。         |
| PartitionName | tablet が属するパーティションの名前。     |
| IndexName     | インデックス名。                                            |
| DbId          | データベース ID。                                           |
| TableId       | テーブル ID。                                              |
| PartitionId   | パーティション ID。                                          |
| IndexId       | インデックス ID。                                              |
| IsSync        | tablet 上のデータがテーブルメタと一致しているかどうか。`true` はデータが一致しており、tablet が正常であることを示します。`false` は tablet 上のデータが欠落していることを示します。 |
| DetailCmd     | より多くの情報をクエリするために使用される URL。                    |

## 例

データベース `example_db` にテーブル `test_show_tablet` を作成します。

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

- 例 1: 指定されたテーブル内のすべての tablets の情報をクエリします。以下の例は、返された情報から 1 つの tablet の情報のみを抜粋しています。

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
            DiskRootPath: /storage/disk
    ```

- 例 2: tablet 9588955 の情報をクエリします。

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

- 例 3: パーティション `p20210103` 内の tablets の情報をクエリします。

    ```sql
    SHOW TABLET FROM test_show_tablet partition(p20210103);
    ```

- 例 4: 10 個の tablets の情報を返します。

    ```sql
        SHOW TABLET FROM test_show_tablet limit 10;
    ```

- 例 5: オフセット 5 で 10 個の tablets の情報を返します。

    ```sql
    SHOW TABLET FROM test_show_tablet limit 5,10;
    ```

- 例 6: `backendid`、`version`、および `state` で tablets をフィルタリングします。

    ```sql
        SHOW TABLET FROM test_show_tablet
        WHERE backendid = 10004 and version = 1 and state = "NORMAL";
    ```

- 例 7: `version` で tablets をソートします。

    ```sql
        SHOW TABLET FROM table_name where backendid = 10004 order by version;
    ```

- 例 8: インデックス名が `test_show_tablet` の tablets の情報を返します。

    ```sql
    SHOW TABLET FROM test_show_tablet where indexname = "test_show_tablet";
    ```