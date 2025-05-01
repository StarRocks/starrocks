---
displayed_sidebar: docs
keywords: ['Broker Load']
---

# HDFS またはクラウドストレージからデータをロードする

StarRocks は、MySQL ベースの Broker Load というロード方法を提供しており、HDFS またはクラウドストレージから大量のデータを StarRocks にロードするのに役立ちます。

Broker Load は非同期ロードモードで動作します。ロードジョブを送信すると、StarRocks はそのジョブを非同期で実行します。ジョブの結果を確認するには、 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) ステートメントまたは `curl` コマンドを使用する必要があります。

Broker Load は単一テーブルロードとマルチテーブルロードをサポートしています。1 つの Broker Load ジョブを実行することで、1 つまたは複数のデータファイルを 1 つまたは複数の宛先テーブルにロードできます。Broker Load は、複数のデータファイルをロードする各ロードジョブのトランザクションの原子性を保証します。原子性とは、1 つのロードジョブで複数のデータファイルをロードする際に、すべてが成功するか失敗するかのいずれかであることを意味します。一部のデータファイルのロードが成功し、他のファイルのロードが失敗することはありません。

Broker Load は、データロード時のデータ変換をサポートし、データロード中の UPSERT および DELETE 操作によるデータ変更をサポートします。詳細については、 [Transform data at loading](../loading/Etl_in_loading.md) および [Change data through loading](../loading/Load_to_Primary_Key_tables.md) を参照してください。

## 背景情報

v2.4 以前では、StarRocks は Broker Load ジョブを実行する際に、StarRocks クラスターと外部ストレージシステム間の接続を確立するためにブローカーに依存していました。そのため、ロードステートメントで使用するブローカーを指定するために `WITH BROKER "<broker_name>"` を入力する必要があります。これを「ブローカー ベースのロード」と呼びます。ブローカーは、ファイルシステムインターフェースと統合された独立したステートレスサービスです。ブローカーを使用すると、StarRocks は外部ストレージシステムに保存されているデータファイルにアクセスして読み取ることができ、独自のコンピューティングリソースを使用してこれらのデータファイルのデータを事前処理してロードできます。

v2.5 以降、StarRocks は Broker Load ジョブを実行する際に、StarRocks クラスターと外部ストレージシステム間の接続を確立するためにブローカーに依存しなくなりました。そのため、ロードステートメントでブローカーを指定する必要はありませんが、`WITH BROKER` キーワードは保持する必要があります。これを「ブローカーフリーロード」と呼びます。

データが HDFS に保存されている場合、ブローカーフリーロードが機能しない状況に遭遇することがあります。これは、データが複数の HDFS クラスターにまたがって保存されている場合や、複数の Kerberos ユーザーを構成している場合に発生する可能性があります。このような状況では、代わりにブローカー ベースのロードを使用することができます。これを成功させるには、少なくとも 1 つの独立したブローカーグループがデプロイされていることを確認してください。これらの状況で認証構成と HA 構成を指定する方法については、 [HDFS](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#hdfs) を参照してください。

> **NOTE**
>
> StarRocks クラスターにデプロイされているブローカーを確認するには、 [SHOW BROKER](../sql-reference/sql-statements/Administration/SHOW_BROKER.md) ステートメントを使用できます。ブローカーがデプロイされていない場合は、 [Deploy a broker](../deployment/deploy_broker.md) に記載された手順に従ってブローカーをデプロイできます。

## サポートされているデータファイル形式

Broker Load は次のデータファイル形式をサポートしています：

- CSV

- Parquet

- ORC

> **NOTE**
>
> CSV データについては、次の点に注意してください：
>
> - テキスト区切り文字として、長さが 50 バイトを超えない UTF-8 文字列（カンマ（,）、タブ、パイプ（|）など）を使用できます。
> - Null 値は `\N` を使用して示されます。たとえば、データファイルが 3 列で構成されており、そのデータファイルのレコードが第 1 列と第 3 列にデータを保持し、第 2 列にデータがない場合、この状況では第 2 列に `\N` を使用して Null 値を示す必要があります。つまり、レコードは `a,\N,b` としてコンパイルする必要があり、`a,,b` ではありません。`a,,b` は、レコードの第 2 列が空の文字列を保持していることを示します。

## サポートされているストレージシステム

Broker Load は次のストレージシステムをサポートしています：

- HDFS

- AWS S3

- Google GCS

- MinIO などの他の S3 互換ストレージシステム

## 動作の仕組み

ロードジョブを FE に送信すると、FE はクエリプランを生成し、利用可能な BE の数とロードするデータファイルのサイズに基づいてクエリプランを分割し、クエリプランの各部分を利用可能な BE に割り当てます。ロード中、関与する各 BE は HDFS またはクラウドストレージシステムからデータファイルのデータを取得し、データを事前処理してから StarRocks クラスターにデータをロードします。すべての BE がクエリプランの部分を終了すると、FE はロードジョブが成功したかどうかを判断します。

次の図は、Broker Load ジョブのワークフローを示しています。

![Workflow of Broker Load](../_assets/broker_load_how-to-work_en.png)

## 基本操作

### マルチテーブルロードジョブを作成する

このトピックでは、CSV を例にとり、複数のデータファイルを複数のテーブルにロードする方法を説明します。他のファイル形式でデータをロードする方法や Broker Load の構文とパラメーターの説明については、 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) を参照してください。

StarRocks では、いくつかのリテラルが SQL 言語によって予約キーワードとして使用されていることに注意してください。これらのキーワードを SQL ステートメントで直接使用しないでください。このようなキーワードを SQL ステートメントで使用する場合は、バッククォート (`) で囲んでください。 [Keywords](../sql-reference/sql-statements/keywords.md) を参照してください。

#### データ例

1. ローカルファイルシステムに CSV ファイルを作成します。

   a. `file1.csv` という名前の CSV ファイルを作成します。このファイルは、ユーザー ID、ユーザー名、ユーザースコアを順に表す 3 つの列で構成されています。

      ```Plain
      1,Lily,23
      2,Rose,23
      3,Alice,24
      4,Julia,25
      ```

   b. `file2.csv` という名前の CSV ファイルを作成します。このファイルは、都市 ID と都市名を順に表す 2 つの列で構成されています。

      ```Plain
      200,'Beijing'
      ```

2. `file1.csv` と `file2.csv` を HDFS クラスターの `/user/starrocks/` パス、AWS S3 バケット `bucket_s3` の `input` フォルダー、Google GCS バケット `bucket_gcs` の `input` フォルダー、MinIO バケット `bucket_minio` の `input` フォルダーにアップロードします。

3. StarRocks データベース `test_db` に StarRocks テーブルを作成します。

   > **NOTE**
   >
   > v2.5.7 以降、StarRocks はテーブルを作成したりパーティションを追加したりする際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、 [determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets) を参照してください。

   a. `table1` という名前の主キーテーブルを作成します。このテーブルは、`id`、`name`、`score` の 3 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table1`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NULL DEFAULT "" COMMENT "user name",
          `score` int(11) NOT NULL DEFAULT "0" COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

   b. `table2` という名前の主キーテーブルを作成します。このテーブルは、`id` と `city` の 2 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table2`
      (
          `id` int(11) NOT NULL COMMENT "city ID",
          `city` varchar(65533) NULL DEFAULT "" COMMENT "city name"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

#### HDFS からデータをロードする

次のステートメントを実行して、HDFS クラスターの `/user/starrocks` パスから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードします。

```SQL
LOAD LABEL test_db.label1
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
)
PROPERTIES
(
    "timeout" = "3600"
);
```

上記の例では、`StorageCredentialParams` は選択した認証方法に応じて異なる認証パラメーターのグループを表します。詳細については、 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#hdfs) を参照してください。

#### AWS S3 からデータをロードする

次のステートメントを実行して、AWS S3 バケット `bucket_s3` の `input` フォルダーから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードします。

```SQL
LOAD LABEL test_db.label2
(
    DATA INFILE("s3a://bucket_s3/input/file1.csv")
    INTO TABLE table1
    (id, name, score)
    ,
    DATA INFILE("s3a://bucket_s3/input/file2.csv")
    INTO TABLE table2
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
);
```

> **NOTE**
>
> Broker Load は S3A プロトコルに従ってのみ AWS S3 にアクセスをサポートしています。そのため、AWS S3 からデータをロードする際には、`DATA INFILE` にファイルパスとして渡す S3 URI の `s3://` を `s3a://` に置き換える必要があります。

上記の例では、`StorageCredentialParams` は選択した認証方法に応じて異なる認証パラメーターのグループを表します。詳細については、 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#aws-s3) を参照してください。

#### Google GCS からデータをロードする

次のステートメントを実行して、Google GCS バケット `bucket_gcs` の `/input/` フォルダーから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードします。

```SQL
LOAD LABEL test_db.label3
(
    DATA INFILE("s3a://bucket_gcs/input/file1.csv")
    INTO TABLE table1
    (id, name, score)
    ,
    DATA INFILE("s3a://bucket_gcs/input/file2.csv")
    INTO TABLE table2
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
);
```

> **NOTE**
>
> Broker Load は S3A プロトコルに従ってのみ Google GCS にアクセスをサポートしています。そのため、Google GCS からデータをロードする際には、`DATA INFILE` にファイルパスとして渡す GCS URI のプレフィックスを `s3a://` に置き換える必要があります。

上記の例では、`StorageCredentialParams` は選択した認証方法に応じて異なる認証パラメーターのグループを表します。詳細については、 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#google-gcs) を参照してください。

#### 他の S3 互換ストレージシステムからデータをロードする

MinIO を例にとります。次のステートメントを実行して、MinIO バケット `bucket_gcs` の `input` フォルダーから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードします。

```SQL
LOAD LABEL test_db.label7
(
    DATA INFILE("s3://bucket_minio/input/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("s3://bucket_minio/input/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
);
```

上記の例では、`StorageCredentialParams` は選択した認証方法に応じて異なる認証パラメーターのグループを表します。詳細については、 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#other-s3-compatible-storage-system) を参照してください。

#### データをクエリする

HDFS クラスター、AWS S3 バケット、または Google GCS バケットからのデータロードが完了した後、SELECT ステートメントを使用して StarRocks テーブルのデータをクエリし、ロードが成功したことを確認できます。

1. 次のステートメントを実行して `table1` のデータをクエリします。

   ```SQL
   MySQL [test_db]> SELECT * FROM table1;
   +------+-------+-------+
   | id   | name  | score |
   +------+-------+-------+
   |    1 | Lily  |    23 |
   |    2 | Rose  |    23 |
   |    3 | Alice |    24 |
   |    4 | Julia |    25 |
   +------+-------+-------+
   4 rows in set (0.00 sec)
   ```

1. 次のステートメントを実行して `table2` のデータをクエリします。

   ```SQL
   MySQL [test_db]> SELECT * FROM table2;
   +------+--------+
   | id   | city   |
   +------+--------+
   | 200  | Beijing|
   +------+--------+
   4 rows in set (0.01 sec)
   ```

### 単一テーブルロードジョブを作成する

指定されたパスから単一のデータファイルまたはすべてのデータファイルを単一の宛先テーブルにロードすることもできます。たとえば、AWS S3 バケット `bucket_s3` に `input` という名前のフォルダーが含まれているとします。この `input` フォルダーには複数のデータファイルが含まれており、そのうちの 1 つは `file1.csv` という名前です。これらのデータファイルは、`table1` と同じ数の列で構成されており、これらのデータファイルの各列は `table1` の列と順番に 1 対 1 でマッピングできます。

`file1.csv` を `table1` にロードするには、次のステートメントを実行します。

```SQL
LOAD LABEL test_db.label_7
(
    DATA INFILE("s3a://bucket_s3/input/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    FORMAT AS "CSV"
)
WITH BROKER 
(
    StorageCredentialParams
);
```

`input` フォルダーからすべてのデータファイルを `table1` にロードするには、次のステートメントを実行します。

```SQL
LOAD LABEL test_db.label_8
(
    DATA INFILE("s3a://bucket_s3/input/*")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    FORMAT AS "CSV"
)
WITH BROKER 
(
    StorageCredentialParams
);
```

上記の例では、`StorageCredentialParams` は選択した認証方法に応じて異なる認証パラメーターのグループを表します。詳細については、 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#aws-s3) を参照してください。

### ロードジョブを表示する

Broker Load を使用すると、SHOW LOAD ステートメントまたは `curl` コマンドを使用してジョブを表示できます。

#### SHOW LOAD を使用する

詳細については、 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) を参照してください。

#### curl を使用する

構文は次のとおりです。

```Bash
curl --location-trusted -u <username>:<password> \
    'http://<fe_host>:<fe_http_port>/api/<database_name>/_load_info?label=<label_name>'
```

> **NOTE**
>
> パスワードが設定されていないアカウントを使用する場合、`<username>:` のみを入力する必要があります。

たとえば、次のコマンドを実行して、`test_db` データベース内のラベルが `label1` のロードジョブに関する情報を表示できます。

```Bash
curl --location-trusted -u <username>:<password> \
    'http://<fe_host>:<fe_http_port>/api/test_db/_load_info?label=label1'
```

`curl` コマンドは、指定されたラベルを持つ最新のロードジョブに関する情報を JSON オブジェクト `jobInfo` として返します。

```JSON
{"jobInfo":{"dbName":"default_cluster:test_db","tblNames":["table1_simple"],"label":"label1","state":"FINISHED","failMsg":"","trackingUrl":""},"status":"OK","msg":"Success"}%
```

次の表は、`jobInfo` のパラメーターを説明しています。

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| dbName        | データがロードされるデータベースの名前                       |
| tblNames      | データがロードされるテーブルの名前                           |
| label         | ロードジョブのラベル                                         |
| state         | ロードジョブのステータス。 有効な値:<ul><li>`PENDING`: ロードジョブはスケジュール待ちのキューにあります。</li><li>`QUEUEING`: ロードジョブはスケジュール待ちのキューにあります。</li><li>`LOADING`: ロードジョブが実行中です。</li><li>`PREPARED`: トランザクションがコミットされました。</li><li>`FINISHED`: ロードジョブが成功しました。</li><li>`CANCELLED`: ロードジョブが失敗しました。</li></ul>詳細については、 [Overview of data loading](../loading/Loading_intro.md) の「非同期ロード」セクションを参照してください。 |
| failMsg       | ロードジョブが失敗した理由。ロードジョブの `state` 値が `PENDING`、`LOADING`、または `FINISHED` の場合、`failMsg` パラメーターには `NULL` が返されます。ロードジョブの `state` 値が `CANCELLED` の場合、`failMsg` パラメーターに返される値は `type` と `msg` の 2 つの部分で構成されます。<ul><li>`type` 部分は次のいずれかの値になります：</li><ul><li>`USER_CANCEL`: ロードジョブが手動でキャンセルされました。</li><li>`ETL_SUBMIT_FAIL`: ロードジョブの送信に失敗しました。</li><li>`ETL-QUALITY-UNSATISFIED`: 不合格データの割合が `max-filter-ratio` パラメーターの値を超えたため、ロードジョブが失敗しました。</li><li>`LOAD-RUN-FAIL`: ロードジョブが `LOADING` ステージで失敗しました。</li><li>`TIMEOUT`: 指定されたタイムアウト期間内にロードジョブが完了しませんでした。</li><li>`UNKNOWN`: 不明なエラーによりロードジョブが失敗しました。</li></ul><li>`msg` 部分はロード失敗の詳細な原因を提供します。</li></ul> |
| trackingUrl   | ロードジョブで検出された不合格データにアクセスするために使用される URL。`curl` または `wget` コマンドを使用して URL にアクセスし、不合格データを取得できます。不合格データが検出されない場合、`trackingUrl` パラメーターには `NULL` が返されます。 |
| status        | ロードジョブの HTTP リクエストのステータス。 有効な値：`OK` と `Fail`。 |
| msg           | ロードジョブの HTTP リクエストのエラー情報。                 |

### ロードジョブをキャンセルする

ロードジョブが **CANCELLED** または **FINISHED** ステージにない場合、 [CANCEL LOAD](../sql-reference/sql-statements/data-manipulation/CANCEL_LOAD.md) ステートメントを使用してジョブをキャンセルできます。

たとえば、次のステートメントを実行して、データベース `test_db` 内のラベルが `label1` のロードジョブをキャンセルできます。

```SQL
CANCEL LOAD
FROM test_db
WHERE LABEL = "label";
```

## ジョブの分割と同時実行

Broker Load ジョブは、同時に実行される 1 つ以上のタスクに分割できます。ロードジョブ内のタスクは単一のトランザクション内で実行されます。すべてが成功するか失敗する必要があります。StarRocks は、`LOAD` ステートメントで `data_desc` を宣言する方法に基づいて各ロードジョブを分割します：

- 複数の `data_desc` パラメーターを宣言し、それぞれが異なるテーブルを指定する場合、各テーブルのデータをロードするタスクが生成されます。

- 複数の `data_desc` パラメーターを宣言し、それぞれが同じテーブルの異なるパーティションを指定する場合、各パーティションのデータをロードするタスクが生成されます。

さらに、各タスクは 1 つ以上のインスタンスにさらに分割され、StarRocks クラスターの BE に均等に分散され、同時に実行されます。StarRocks は、次の [FE configurations](../administration/Configuration.md#fe-configuration-items) に基づいて各タスクを分割します：

- `min_bytes_per_broker_scanner`: 各インスタンスが処理する最小データ量。デフォルトの量は 64 MB です。

- `max_broker_concurrency`: 各タスクで許可される最大同時インスタンス数。デフォルトの最大数は 100 です。

- `load_parallel_instance_num`: 個々の BE で各ロードジョブに許可される同時インスタンス数。デフォルトの数は 1 です。

  個々のタスクのインスタンス数を計算するには、次の式を使用できます：

  **個々のタスクのインスタンス数 = min(個々のタスクがロードするデータ量/`min_bytes_per_broker_scanner`,`max_broker_concurrency`,`load_parallel_instance_num` x BE の数)**

ほとんどの場合、各ロードジョブに対して 1 つの `data_desc` のみが宣言され、各ロードジョブは 1 つのタスクにのみ分割され、そのタスクは BE の数と同じ数のインスタンスに分割されます。

## 使用上の注意

[FE configuration item](../administration/Configuration.md#fe-configuration-items) `max_broker_load_job_concurrency` は、StarRocks クラスター内で同時に実行できる Broker Load ジョブの最大数を指定します。

StarRocks v2.4 以前では、特定の期間内に送信された Broker Load ジョブの総数が最大数を超える場合、過剰なジョブはキューに入れられ、送信時間に基づいてスケジュールされます。

StarRocks v2.5 以降、特定の期間内に送信された Broker Load ジョブの総数が最大数を超える場合、過剰なジョブは優先順位に基づいてキューに入れられ、スケジュールされます。ジョブ作成時に `priority` パラメーターを使用してジョブの優先順位を指定できます。 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md#opt_properties) を参照してください。また、 [ALTER LOAD](../sql-reference/sql-statements/data-manipulation/ALTER_LOAD.md) を使用して、**QUEUEING** または **LOADING** 状態にある既存のジョブの優先順位を変更することもできます。