---
displayed_sidebar: docs
---

# SHOW MATERIALIZED VIEWS

## 説明

すべてまたは特定の非同期マテリアライズドビューを表示します。

バージョン 3.0 以降、このステートメントの名前は SHOW MATERIALIZED VIEW から SHOW MATERIALIZED VIEWS に変更されました。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```SQL
SHOW MATERIALIZED VIEWS
[FROM db_name]
[
WHERE NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

:::note

バージョン 3.3 以降、`SHOW MATERIALIZED VIEWS` コマンドは、リフレッシュタスクが複数のパーティション/タスクランで構成されている場合、すべてのタスクランの状態を追跡します。すべてのタスクランが成功した場合にのみ、`last_refresh_state` は `SUCCESS` を返します。

:::

## パラメータ

| **パラメータ**   | **必須** | **説明**                                              |
| --------------- | ------------ | ------------------------------------------------------------ |
| db_name         | いいえ           | マテリアライズドビューが存在するデータベースの名前。このパラメータが指定されていない場合、デフォルトで現在のデータベースが使用されます。 |
| mv_name         | いいえ           | 表示するマテリアライズドビューの名前。                   |
| mv_name_matcher | いいえ           | マテリアライズドビューをフィルタリングするために使用されるマッチャー。               |

## 戻り値

| **戻り値**                 | **説明**                                              |
| -------------------------- | ------------------------------------------------------------ |
| id                         | マテリアライズドビューのID。                             |
| database_name              | マテリアライズドビューが存在するデータベースの名前。 |
| name                       | マテリアライズドビューの名前。                           |
| refresh_type               | マテリアライズドビューのリフレッシュタイプ。ROLLUP、MANUAL、ASYNC、INCREMENTAL などがあります。 |
| is_active                  | マテリアライズドビューの状態がアクティブかどうか。 有効な値: `true` と `false`。 |
| partition_type             | マテリアライズドビューのパーティションタイプ。RANGE と UNPARTITIONED があります。                |
| task_id                    | マテリアライズドビューのリフレッシュタスクのID。                  |
| task_name                  | マテリアライズドビューのリフレッシュタスクの名前。                |
| last_refresh_start_time    | マテリアライズドビューの最後のリフレッシュの開始時間。 |
| last_refresh_finished_time | マテリアライズドビューの最後のリフレッシュの終了時間。   |
| last_refresh_duration      | 最後のリフレッシュにかかった時間。単位: 秒。           |
| last_refresh_state         | 最後のリフレッシュのステータス。PENDING、RUNNING、FAILED、SUCCESS があります。 |
| last_refresh_force_refresh | 最後のリフレッシュがFORCEリフレッシュかどうか。                 |
| last_refresh_start_partition | マテリアライズドビューの最後のリフレッシュの開始パーティション。 |
| last_refresh_end_partition | マテリアライズドビューの最後のリフレッシュの終了パーティション。 |
| last_refresh_base_refresh_partitions | 最後のリフレッシュで更新されたベーステーブルのパーティション。 |
| last_refresh_mv_refresh_partitions | 最後のリフレッシュで更新されたマテリアライズドビューパーティション。 |
| last_refresh_error_code    | マテリアライズドビューの最後の失敗したリフレッシュのエラーコード（マテリアライズドビューの状態がアクティブでない場合）。 |
| last_refresh_error_message | マテリアライズドビューの最後のリフレッシュが失敗した理由（マテリアライズドビューの状態がアクティブでない場合）。 |
| rows                       | マテリアライズドビューのデータ行数。            |
| text                       | マテリアライズドビューを作成するために使用されたステートメント。          |

## 例

以下の例は、このビジネスシナリオに基づいています:

```Plain
-- テーブルを作成: customer
CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        VARCHAR(25) NOT NULL,
                        C_ADDRESS     VARCHAR(40) NOT NULL,
                        C_NATIONKEY   INTEGER NOT NULL,
                        C_PHONE       CHAR(15) NOT NULL,
                        C_ACCTBAL     double   NOT NULL,
                        C_MKTSEGMENT  CHAR(10) NOT NULL,
                        C_COMMENT     VARCHAR(117) NOT NULL,
                        PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT"
);

-- マテリアライズドビューを作成: customer_mv
CREATE MATERIALIZED VIEW customer_mv
DISTRIBUTED BY HASH(c_custkey)
REFRESH MANUAL
PROPERTIES (
    "replication_num" = "3"
)
AS SELECT
              c_custkey, c_phone, c_acctbal, count(1) as c_count, sum(c_acctbal) as c_sum
   FROM
              customer
   GROUP BY c_custkey, c_phone, c_acctbal;

-- マテリアライズドビューをリフレッシュ
REFRESH MATERIALIZED VIEW customer_mv;
```

例 1: 特定のマテリアライズドビューを表示します。

```Plain
mysql> SHOW MATERIALIZED VIEWS WHERE NAME='customer_mv'\G
*************************** 1. row ***************************
                        id: 10142
                      name: customer_mv
             database_name: test
              refresh_type: MANUAL
                 is_active: true
   last_refresh_start_time: 2023-02-17 10:27:33
last_refresh_finished_time: 2023-02-17 10:27:33
     last_refresh_duration: 0
        last_refresh_state: SUCCESS
             inactive_code: 0
           inactive_reason:
                      text: CREATE MATERIALIZED VIEW `customer_mv`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`c_custkey`)
REFRESH MANUAL
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`, count(1) AS `c_count`, sum(`customer`.`c_acctbal`) AS `c_sum`
FROM `test`.`customer`
GROUP BY `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`;
                      rows: 0
1 row in set (0.11 sec)
```

例 2: 名前をマッチングしてマテリアライズドビューを表示します。

```Plain
mysql> SHOW MATERIALIZED VIEWS WHERE NAME LIKE 'customer_mv'\G
*************************** 1. row ***************************
                        id: 10142
                      name: customer_mv
             database_name: test
              refresh_type: MANUAL
                 is_active: true
   last_refresh_start_time: 2023-02-17 10:27:33
last_refresh_finished_time: 2023-02-17 10:27:33
     last_refresh_duration: 0
        last_refresh_state: SUCCESS
             inactive_code: 0
           inactive_reason:
                      text: CREATE MATERIALIZED VIEW `customer_mv`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`c_custkey`)
REFRESH MANUAL
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`, count(1) AS `c_count`, sum(`customer`.`c_acctbal`) AS `c_sum`
FROM `test`.`customer`
GROUP BY `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`;
                      rows: 0
1 row in set (0.12 sec)
```