---
displayed_sidebar: docs
---

# SHOW PARTITIONS

## 説明

共通パーティションおよび[一時パーティション](../../../table_design/data_distribution/Temporary_partition.md)を含むパーティション情報を表示します。

## 構文

```sql
SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

> NOTE
>
> この構文は StarRocks テーブル (`"ENGINE" = "OLAP"`) のみをサポートします。
> バージョン 3.0 以降、この操作には指定されたテーブルに対する SELECT 権限が必要です。バージョン 2.5 以前では、この操作には指定されたテーブルに対する SELECT__PRIV 権限が必要です。

## 戻りフィールドの説明

```plaintext
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
```

| **フィールド**            | **説明**                                                      |
| ------------------------ | ------------------------------------------------------------ |
| PartitionId              | パーティションのIDです。                                      |
| PartitionName            | パーティションの名前です。                                    |
| VisibleVersion           | 最後に成功したロードトランザクションのバージョン番号です。成功するたびにバージョン番号は1ずつ増加します。 |
| VisibleVersionTime       | 最後に成功したロードトランザクションのタイムスタンプです。   |
| VisibleVersionHash       | 最後に成功したロードトランザクションのバージョン番号のハッシュ値です。 |
| State                    | パーティションの状態です。固定値: `Normal`。                  |
| PartitionKey             | 1つ以上のパーティション列からなるパーティションキーです。     |
| Range                    | パーティションの範囲で、右半開区間です。                      |
| DistributionKey          | ハッシュバケッティングのバケットキーです。                    |
| Buckets                  | パーティションのバケット数です。                              |
| ReplicationNum           | パーティション内の各タブレットのレプリカ数です。              |
| StorageMedium            | パーティション内のデータを保存する記憶媒体です。値 `HHD` はハードディスクドライブを示し、値 `SSD` はソリッドステートドライブを示します。 |
| CooldownTime             | パーティション内のデータのクールダウン時間です。初期記憶媒体が SSD の場合、このパラメータで指定された時間後に記憶媒体は SSD から HDD に切り替わります。フォーマット: "yyyy-MM-dd HH:mm:ss"。 |
| LastConsistencyCheckTime | 最後の整合性チェックの時間です。`NULL` は整合性チェックが行われなかったことを示します。 |
| DataSize                 | パーティション内のデータサイズです。                          |
| IsInMemory               | パーティション内のすべてのデータがメモリに保存されているかどうかです。 |
| RowCount                 | パーティションのデータ行数です。                              |
| MaxCS                    | パーティションの最大 Compaction スコアです。共有データクラスタのみ。 |

## 例

1. 指定されたデータベース `test` の指定されたテーブル `site_access` からすべての通常のパーティション情報を表示します。

    ```SQL
    MySQL > show partitions from test.site_access\G
    *************************** 1. row ***************************
                PartitionId: 20990
            PartitionName: p2019 
            VisibleVersion: 1
        VisibleVersionTime: 2023-08-08 15:45:13
        VisibleVersionHash: 0
                    State: NORMAL
                PartitionKey: datekey
                    Range: [types: [DATE]; keys: [2019-01-01]; ..types: [DATE]; keys: [2020-01-01]; )
            DistributionKey: site_id
                    Buckets: 6
            ReplicationNum: 3
            StorageMedium: HDD
                CooldownTime: 9999-12-31 23:59:59
    LastConsistencyCheckTime: NULL
                    DataSize:  4KB   
                IsInMemory: false
                    RowCount: 3 
    1 row in set (0.00 sec)
    ```

2. 指定されたデータベース `test` の指定されたテーブル `site_access` からすべての一時パーティション情報を表示します。

    ```sql
    SHOW TEMPORARY PARTITIONS FROM test.site_access;
    ```

3. 指定されたデータベース `test` の指定されたテーブル `site_access` の指定されたパーティション `p1` の情報を表示します。

    ```sql
    -- 通常のパーティション
    SHOW PARTITIONS FROM test.site_access WHERE PartitionName = "p1";
    -- 一時パーティション
    SHOW TEMPORARY PARTITIONS FROM test.site_access WHERE PartitionName = "p1";
    ```

4. 指定されたデータベース `test` の指定されたテーブル `site_access` の最新のパーティション情報を表示します。

    ```sql
    -- 通常のパーティション
    SHOW PARTITIONS FROM test.site_access ORDER BY PartitionId DESC LIMIT 1;
    -- 一時パーティション
    SHOW TEMPORARY PARTITIONS FROM test.site_access ORDER BY PartitionId DESC LIMIT 1;
    ```