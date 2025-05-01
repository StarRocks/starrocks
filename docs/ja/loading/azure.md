---
displayed_sidebar: docs
toc_max_heading_level: 4
keywords: ['Broker Load']
---

# Microsoft Azure Storage からデータをロードする

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

StarRocks は、[Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を使用して Microsoft Azure Storage からデータを一括でロードすることができます。

Broker Load は非同期モードで実行されます。非同期の Broker Load プロセスは、Azure への接続を確立し、データを取得し、StarRocks にデータを保存する処理を行います。

Broker Load は、Parquet、ORC、CSV ファイル形式をサポートしています。

## Broker Load の利点

- Broker Load はバックグラウンドで実行され、クライアントはジョブが続行するために接続し続ける必要がありません。
- Broker Load は長時間実行されるジョブに適しており、デフォルトのタイムアウトは 4 時間です。
- Parquet と ORC ファイル形式に加えて、Broker Load は CSV ファイルもサポートしています。

## データフロー

![Workflow of Broker Load](../_assets/broker_load_how-to-work_en.png)

1. ユーザーがロードジョブを作成します。
2. フロントエンド (FE) がクエリプランを作成し、そのプランをバックエンドノード (BEs) またはコンピュートノード (CNs) に配布します。
3. BEs または CNs がソースからデータを取得し、StarRocks にデータをロードします。

## 始める前に

### ソースデータを準備する

StarRocks にロードしたいソースデータが、Azure ストレージアカウント内のコンテナに適切に保存されていることを確認してください。

このトピックでは、Azure Data Lake Storage Gen2 (ADLS Gen2) ストレージアカウント (`starrocks`) 内のコンテナ (`starrocks-container`) のルートディレクトリに保存されている Parquet 形式のサンプルデータセット (`user_behavior_ten_million_rows.parquet`) のデータをロードすることを想定しています。

### 権限を確認する

<InsertPrivNote />

### 認証情報を収集する

このトピックの例では、Shared Key 認証方法を使用します。ADLS Gen2 からデータを読み取る権限があることを確認するために、[Azure Data Lake Storage Gen2 > Shared Key (ストレージアカウントのアクセスキー)](../integrations/authenticate_to_azure_storage.md#shared-key-1) を読み、設定する必要がある認証パラメータを理解することをお勧めします。

要するに、Shared Key 認証を実践する場合、以下の情報を収集する必要があります：

- ADLS Gen2 ストレージアカウントのユーザー名
- ADLS Gen2 ストレージアカウントの共有キー

利用可能なすべての認証方法については、[Authenticate to Azure cloud storage](../integrations/authenticate_to_azure_storage.md) を参照してください。

## 典型的な例

テーブルを作成し、Azure からサンプルデータセット `user_behavior_ten_million_rows.parquet` を取得するロードプロセスを開始し、データロードの進捗と成功を確認します。

### データベースとテーブルを作成する

StarRocks クラスターに接続します。次に、データベースを作成し、それに切り替えます：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

手動でテーブルを作成します（Azure からロードしたい Parquet ファイルと同じスキーマを持つことをお勧めします）：

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

次のコマンドを実行して、サンプルデータセット `user_behavior_ten_million_rows.parquet` から `user_behavior` テーブルにデータをロードする Broker Load ジョブを開始します：

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("abfss://starrocks-container@starrocks.dfs.core.windows.net/user_behavior_ten_million_rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.storage_account" = "starrocks",
    "azure.adls2.shared_key" = "xxxxxxxxxxxxxxxxxx"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

このジョブには 4 つの主要なセクションがあります：

- `LABEL`: ロードジョブの状態をクエリする際に使用される文字列。
- `LOAD` 宣言: ソース URI、ソースデータ形式、宛先テーブル名。
- `BROKER`: ソースの接続詳細。
- `PROPERTIES`: タイムアウト値およびロードジョブに適用するその他のプロパティ。

詳細な構文とパラメータの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

### ロードの進捗を確認する

StarRocks Information Schema の [`loads`](../sql-reference/information_schema.md#loads) ビューから Broker Load ジョブの進捗をクエリできます。この機能は v3.1 以降でサポートされています。

```SQL
SELECT * FROM information_schema.loads \G
```

複数のロードジョブを送信した場合は、ジョブに関連付けられた `LABEL` でフィルタリングできます：

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior' \G
*************************** 1. row ***************************
              JOB_ID: 10250
               LABEL: user_behavior
       DATABASE_NAME: mydatabase
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: BROKER
            PRIORITY: NORMAL
           SCAN_ROWS: 10000000
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 10000000
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):3600; max_filter_ratio:0.0
         CREATE_TIME: 2023-12-28 16:15:19
      ETL_START_TIME: 2023-12-28 16:15:25
     ETL_FINISH_TIME: 2023-12-28 16:15:25
     LOAD_START_TIME: 2023-12-28 16:15:25
    LOAD_FINISH_TIME: 2023-12-28 16:16:31
         JOB_DETAILS: {"All backends":{"6a8ef4c0-1009-48c9-8d18-c4061d2255bf":[10121]},"FileNumber":1,"FileSize":132251298,"InternalTableLoadBytes":311710786,"InternalTableLoadRows":10000000,"ScanBytes":132251298,"ScanRows":10000000,"TaskNumber":1,"Unfinished backends":{"6a8ef4c0-1009-48c9-8d18-c4061d2255bf":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
```

`loads` ビューで提供されるフィールドについての情報は、[Information Schema](../sql-reference/information_schema.md#loads) を参照してください。

ロードジョブが完了したことを確認した後、宛先テーブルのサブセットをチェックして、データが正常にロードされたかどうかを確認できます。例：

```SQL
SELECT * from user_behavior LIMIT 3;
```

データが正常に返される場合、データロードは成功しています。

システムは次のようなクエリ結果を返し、データが正常にロードされたことを示します：

```Plain
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    142 | 2869980 |    2939262 | pv           | 2017-11-25 03:43:22 |
|    142 | 2522236 |    1669167 | pv           | 2017-11-25 15:14:12 |
|    142 | 3031639 |    3607361 | pv           | 2017-11-25 15:19:25 |
+--------+---------+------------+--------------+---------------------+
```