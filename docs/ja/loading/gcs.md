---
displayed_sidebar: docs
toc_max_heading_level: 4
keywords: ['Broker Load']
---

# GCS からデータをロードする

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

StarRocks は、[Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を使用して Google Cloud Storage (GCS) からデータを一括でロードすることができます。

Broker Load は非同期モードで動作します。非同期の Broker Load プロセスは、GCS への接続を確立し、データを取得し、StarRocks にデータを保存する処理を行います。

Broker Load は、Parquet、ORC、および CSV ファイル形式をサポートしています。

## Broker Load の利点

- Broker Load はバックグラウンドで実行され、クライアントはジョブが続行するために接続を維持する必要がありません。
- Broker Load は長時間実行されるジョブに適しており、デフォルトのタイムアウトは 4 時間です。
- Parquet および ORC ファイル形式に加えて、Broker Load は CSV ファイルもサポートしています。

## データフロー

![Broker Load のワークフロー](../_assets/broker_load_how-to-work_en.png)

1. ユーザーがロードジョブを作成します。
2. フロントエンド (FE) がクエリプランを作成し、そのプランをバックエンドノード (BEs) またはコンピュートノード (CNs) に配布します。
3. BEs または CNs がソースからデータを取得し、StarRocks にデータをロードします。

## 始める前に

### ソースデータを準備する

StarRocks にロードしたいソースデータが GCS バケットに適切に保存されていることを確認してください。また、データとデータベースの位置を考慮することもお勧めします。バケットと StarRocks クラスターが同じリージョンにある場合、データ転送コストは大幅に低くなります。

このトピックでは、GCS バケット内のサンプルデータセット `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` を提供します。このオブジェクトは、GCP ユーザーであれば誰でも読み取れるため、有効な資格情報でアクセスできます。

### 権限を確認する

<InsertPrivNote />

### 認証情報を収集する

このトピックの例では、サービスアカウントベースの認証を使用します。サービスアカウントベースの認証を実践するには、次の GCS リソースに関する情報を収集する必要があります。

- データを保存する GCS バケット。
- バケット内の特定のオブジェクトにアクセスする場合の GCS オブジェクトキー（オブジェクト名）。GCS オブジェクトがサブフォルダに保存されている場合、オブジェクトキーにはプレフィックスを含めることができます。
- GCS バケットが属する GCS リージョン。
- Google Cloud サービスアカウントの `private_key_id`、`private_key`、および `client_email`

利用可能なすべての認証方法については、[Authenticate to Google Cloud Storage](../integrations/authenticate_to_gcs.md) を参照してください。

## 典型的な例

テーブルを作成し、GCS からサンプルデータセット `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` を取得するロードプロセスを開始し、データロードの進捗と成功を確認します。

### データベースとテーブルを作成する

データベースを作成し、切り替えます:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

手動でテーブルを作成します（GCS からロードしたい Parquet ファイルと同じスキーマを持つことをお勧めします）:

```SQL
CREATE TABLE user_behavior
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp varbinary
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

### Broker Load を開始する

次のコマンドを実行して、サンプルデータセット `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` から `user_behavior` テーブルにデータをロードする Broker Load ジョブを開始します:

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("gs://starrocks-samples/user_behavior_ten_million_rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
 (
 
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
 )
PROPERTIES
(
    "timeout" = "72000"
);
```

> **NOTE**
>
> 上記のコマンドの資格情報を自分の資格情報に置き換えてください。オブジェクトは GCP 認証済みユーザーであれば誰でも読み取れるため、有効なサービスアカウントのメール、キー、およびシークレットを使用できます。

このジョブには 4 つの主要なセクションがあります:

- `LABEL`: ロードジョブの状態をクエリする際に使用される文字列。
- `LOAD` 宣言: ソース URI、ソースデータ形式、および宛先テーブル名。
- `BROKER`: ソースの接続詳細。
- `PROPERTIES`: タイムアウト値およびロードジョブに適用するその他のプロパティ。

詳細な構文とパラメータの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

### ロードの進捗を確認する

StarRocks Information Schema の [`loads`](../sql-reference/information_schema.md#loads) ビューから Broker Load ジョブの進捗をクエリできます。この機能は v3.1 以降でサポートされています。

```SQL
SELECT * FROM information_schema.loads;
```

`loads` ビューで提供されるフィールドに関する情報については、[Information Schema](../sql-reference/information_schema.md#loads) を参照してください。

複数のロードジョブを送信した場合は、ジョブに関連付けられた `LABEL` でフィルタリングできます。例:

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior';
```

以下の出力には、ロードジョブ `user_behavior` の 2 つのエントリがあります:

- 最初のレコードは `CANCELLED` の状態を示しています。`ERROR_MSG` までスクロールすると、`listPath failed` によりジョブが失敗したことがわかります。
- 2 番目のレコードは `FINISHED` の状態を示しており、ジョブが成功したことを意味します。

```Plain
JOB_ID|LABEL                                      |DATABASE_NAME|STATE    |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                           |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG                             |TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
------+-------------------------------------------+-------------+---------+-------------------+------+--------+---------+-------------+---------------+---------+--------+----------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+------------+------------+--------------------+
 10121|user_behavior                              |mydatabase   |CANCELLED|ETL:N/A; LOAD:N/A  |BROKER|NORMAL  |        0|            0|              0|        0|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:59:30|                   |                   |                   |2023-08-10 14:59:34|{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}}                                                                                        |type:ETL_RUN_FAIL; msg:listPath failed|            |            |                    |
 10106|user_behavior                              |mydatabase   |FINISHED |ETL:100%; LOAD:100%|BROKER|NORMAL  | 86953525|            0|              0| 86953525|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:50:15|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:55:10|{"All backends":{"a5fe5e1d-d7d0-4826-ba99-c7348f9a5f2f":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":2710603082,"InternalTableLoadRows":86953525,"ScanBytes":1225637388,"ScanRows":86953525,"TaskNumber":1,"Unfinished backends":{"a5|                                      |            |            |                    |
```

ロードジョブが完了したことを確認した後、宛先テーブルのサブセットをチェックしてデータが正常にロードされたかどうかを確認できます。例:

```SQL
SELECT * from user_behavior LIMIT 3;
```

システムは次のようなクエリ結果を返し、データが正常にロードされたことを示します:

```Plain
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    142 | 2869980 |    2939262 | pv           | 2017-11-25 03:43:22 |
|    142 | 2522236 |    1669167 | pv           | 2017-11-25 15:14:12 |
|    142 | 3031639 |    3607361 | pv           | 2017-11-25 15:19:25 |
+--------+---------+------------+--------------+---------------------+
```