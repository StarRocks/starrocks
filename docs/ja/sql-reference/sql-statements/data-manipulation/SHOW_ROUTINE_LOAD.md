---
displayed_sidebar: docs
---

# SHOW ROUTINE LOAD

## 説明

Routine Load ジョブの実行情報を表示します。

## 構文

```SQL
SHOW [ALL] ROUTINE LOAD [ FOR [<db_name>.]<job_name> | FROM <db_name> ]
[ WHERE [ STATE = { "NEED_SCHEDULE" | "RUNNING" | "PAUSED" | "UNSTABLE" | "STOPPED" | "CANCELLED"  } ] ]
[ ORDER BY field_name [ ASC | DESC ] ]
[ LIMIT { [offset, ] limit | limit OFFSET offset } ]
```

:::tip

ステートメントに `\G` オプションを追加することで、通常の横方向のテーブル形式ではなく、縦方向に返される結果を表示できます（例: `SHOW ROUTINE LOAD FOR <job_name>\G`）。

:::

## パラメータ

| **パラメータ**                     | **必須** | **説明**                                              |
| --------------------------------- | ------------ | ------------------------------------------------------------ |
| db_name                           | いいえ           | ロードジョブが属するデータベースの名前。このパラメータは `FROM` 句が使用される場合に必須です。 |
| job_name                          | いいえ           | ロードジョブの名前。このパラメータは `FOR` 句が使用される場合に必須です。         |
| ALL                               | いいえ           | `STOPPED` または `CANCELLED` 状態のものを含むすべてのロードジョブを表示します。 |
| STATE                             | いいえ           |  ロードジョブのステータス。                                       |
| ORDER BY field_name [ASC \| DESC] | いいえ           | 指定されたフィールドに基づいて、返される結果を昇順または降順にソートします。サポートされるフィールドは次のとおりです: `Id`, `Name`, `CreateTime`, `PauseTime`, `EndTime`, `TableName`, `State`, および `CurrentTaskNum`。<ul><li>返される結果を昇順にソートするには、`ORDER BY field_name ASC` を指定します。</li><li>返される結果を降順にソートするには、`ORDER BY field_name DESC` を指定します。</li></ul>フィールドまたはソート順を指定しない場合、デフォルトで `Id` の昇順にソートされます。 |
| LIMIT limit                       | いいえ           | 返されるロードジョブの数。例えば、`LIMIT 10` を指定すると、フィルター条件に一致する10件のロードジョブの情報のみが返されます。このパラメータが指定されていない場合、フィルター条件に一致するすべてのロードジョブの情報が表示されます。  |
| OFFSET offset                     | いいえ           | `offset` パラメータはスキップされるロードジョブの数を定義します。例えば、`OFFSET 5` は最初の5件のロードジョブをスキップし、残りを返します。`offset` パラメータのデフォルト値は `0` です。 |

## 出力

| **パラメータ**        | **説明**                                              |
| -------------------- | ------------------------------------------------------------ |
| Id                   | StarRocks によって自動生成されるロードジョブのグローバルに一意のID。 |
| Name                 | ロードジョブの名前。                                        |
| CreateTime           | ロードジョブが作成された日時。             |
| PauseTime            | ロードジョブが `PAUSED` 状態に入った日時。  |
| EndTime              | ロードジョブが `STOPPED` 状態に入った日時。 |
| DbName               | ロードジョブのターゲットテーブルが属するデータベース。  |
| TableName            | ロードジョブのターゲットテーブル。                                |
| State                | ロードジョブのステータス。以下を含む:<ul><li>`NEED_SCHEDULE`: ロードジョブはスケジュール待ちです。CREATE ROUTINE LOAD または RESUME ROUTINE LOAD を使用して Routine Load ジョブを作成または再開すると、ロードジョブは最初に `NEED_SCHEDULE` 状態に入ります。</li><li>`RUNNING`: ロードジョブは実行中です。`Statistic` と `Progress` を通じて Routine Load ジョブの消費進捗を確認できます。</li><li>`PAUSED`: ロードジョブは一時停止されています。`ReasonOfStateChanged` と `ErrorLogUrls` を参照してトラブルシューティングを行います。エラーを修正した後、RESUME ROUTINE LOAD を使用して Routine Load ジョブを再開できます。</li><li>`CANCELLED`: ロードジョブはキャンセルされています。`ReasonOfStateChanged` と `ErrorLogUrls` を参照してトラブルシューティングを行います。ただし、エラーを修正した後、この状態ではロードジョブを復元できません。</li><li>`STOPPED`: ロードジョブは停止されています。この状態ではロードジョブを復元できません。</li><li>`UNSTABLE`: ロードジョブは不安定です。Routine Load ジョブ内のタスクが遅延している場合（つまり、消費されているメッセージのタイムスタンプと現在の時間の差がこの FE パラメータ [`routine_load_unstable_threshold_second`](../../../administration/Configuration.md#routine_load_unstable_threshold_second) を超え、データソースに未消費のメッセージが存在する場合）、Routine Load ジョブは `UNSTABLE` 状態に設定されます。</li></ul> |
| DataSourceType       | データソースのタイプ。固定値: `KAFKA`。           |
| CurrentTaskNum       | ロードジョブ内の現在のタスク数。                     |
| JobProperties        | ロードジョブのプロパティ（消費されるパーティションやカラムマッピングなど）。 |
| DataSourceProperties | データソースのプロパティ（トピックや Kafka クラスター内のブローカーのアドレスとポートのリストなど）。 |
| CustomProperties     | ロードジョブで定義された追加のデータソース関連のプロパティ。 |
| Statistic            | データのロード統計（成功した行数、総行数、受信データ量など）。 |
| Progress             | トピックのパーティション内のメッセージ消費の進捗（オフセットで測定）。 |
| TimestampProgress    | トピックのパーティション内のメッセージ消費の進捗（タイムスタンプで測定）。 |
| ReasonOfStateChanged | ロードジョブが `CANCELLED` または `PAUSED` 状態にある理由。 |
| ErrorLogUrls         | エラーログのURL。`curl` または `wget` コマンドを使用してURLにアクセスできます。 |
| TrackingSQL          | `information_schema` データベースに記録されたエラーログ情報を直接クエリするためのSQLコマンド。 |
| OtherMsg             | Routine Load ジョブのすべての失敗したロードタスクに関する情報。 |
| LatestSourcePosition | トピックのパーティション内のメッセージの最新の消費位置。 |

## 例

ロードジョブが正常に開始され、RUNNING 状態にある場合、返される結果は次のようになります:

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD FOR example_tbl_ordertest1\G
*************************** 1. row ***************************
                  Id: 10204
                Name: example_tbl_ordertest1
          CreateTime: 2023-12-21 21:01:31
           PauseTime: NULL
             EndTime: NULL
              DbName: example_db
           TableName: example_tbl
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","rowDelimiter":"\t","partial_update":"false","columnToColumnExpr":"order_id,pay_dt,customer_name,nationality,temp_gender,price","maxBatchIntervalS":"10","partial_update_mode":"null","whereExpr":"*","timezone":"Asia/Shanghai","format":"csv","columnSeparator":"','","log_rejected_record_num":"0","taskTimeoutSecond":"60","json_root":"","maxFilterRatio":"1.0","strict_mode":"false","jsonpaths":"","taskConsumeSecond":"15","desireTaskConcurrentNum":"5","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"lilyliuyitest4csv","currentKafkaPartitions":"0","brokerList":"xxx.xx.xx.xxx:9092"}
    CustomProperties: {"kafka_default_offsets":"OFFSET_BEGINNING","group.id":"example_tbl_ordertest1_b05da08f-9b9d-4fe1-b1f2-25d7116d617c"}
           Statistic: {"receivedBytes":313,"errorRows":0,"committedTaskNum":1,"loadedRows":6,"loadRowsRate":0,"abortedTaskNum":0,"totalRows":6,"unselectedRows":0,"receivedBytesRate":0,"taskExecuteTimeMs":699}
            Progress: {"0":"5"}
   TimestampProgress: {"0":"1686143856061"}
ReasonOfStateChanged: 
        ErrorLogUrls: 
         TrackingSQL: 
            OtherMsg: 
LatestSourcePosition: {"0":"6"}
1 row in set (0.01 sec)
```

ロードジョブが例外により `PAUSED` または `CANCELLED` 状態にある場合、返される結果の `ReasonOfStateChanged`、`ErrorLogUrls`、`TrackingSQL`、および `OtherMsg` フィールドに基づいてトラブルシューティングを行うことができます。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD FOR example_tbl_ordertest2\G
*************************** 1. row ***************************
                  Id: 10204
                Name: example_tbl_ordertest2
          CreateTime: 2023-12-22 12:13:18
           PauseTime: 2023-12-22 12:13:38
             EndTime: NULL
              DbName: example_db
           TableName: example_tbl
               State: PAUSED
      DataSourceType: KAFKA
      CurrentTaskNum: 0
       JobProperties: {"partitions":"*","rowDelimiter":"\t","partial_update":"false","columnToColumnExpr":"order_id,pay_dt,customer_name,nationality,temp_gender,price","maxBatchIntervalS":"10","partial_update_mode":"null","whereExpr":"*","timezone":"Asia/Shanghai","format":"csv","columnSeparator":"','","log_rejected_record_num":"0","taskTimeoutSecond":"60","json_root":"","maxFilterRatio":"1.0","strict_mode":"false","jsonpaths":"","taskConsumeSecond":"15","desireTaskConcurrentNum":"5","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"mytest","currentKafkaPartitions":"0","brokerList":"xxx.xx.xx.xxx:9092"}
    CustomProperties: {"kafka_default_offsets":"OFFSET_BEGINNING","group.id":"example_tbl_ordertest2_b3fada0f-6721-4ad1-920d-e4bf6d6ea7f7"}
           Statistic: {"receivedBytes":541,"errorRows":10,"committedTaskNum":1,"loadedRows":6,"loadRowsRate":0,"abortedTaskNum":0,"totalRows":16,"unselectedRows":0,"receivedBytesRate":0,"taskExecuteTimeMs":646}
            Progress: {"0":"19"}
   TimestampProgress: {"0":"1702623900871"}
ReasonOfStateChanged: ErrorReason{errCode = 102, msg='current error rows is more than max error num'}
        ErrorLogUrls: http://xxx.xx.xx.xxx:8040/api/_load_error_log?file=error_log_b25dcc7e642344b2_b0b342b9de0567db
         TrackingSQL: select tracking_log from information_schema.load_tracking_logs where job_id=10204
            OtherMsg: 
LatestSourcePosition: {"0":"20"}
1 row in set (0.00 sec)
```