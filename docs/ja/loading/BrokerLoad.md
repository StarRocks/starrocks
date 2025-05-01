---
displayed_sidebar: docs
---

# HDFS またはクラウドストレージからデータをロードする

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

StarRocks は、MySQL ベースの Broker Load を提供しており、HDFS またはクラウドストレージから大量のデータを StarRocks にロードするのに役立ちます。

Broker Load は非同期ロードモードで動作します。ロードジョブを送信すると、StarRocks は非同期でジョブを実行します。ジョブの結果を確認するには、[SHOW LOAD](../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md) ステートメントまたは `curl` コマンドを使用する必要があります。

Broker Load は、単一テーブルのロードと複数テーブルのロードをサポートしています。1 つの Broker Load ジョブを実行することで、1 つまたは複数のデータファイルを 1 つまたは複数の宛先テーブルにロードできます。Broker Load は、複数のデータファイルをロードする各ロードジョブのトランザクションの原子性を保証します。原子性とは、1 つのロードジョブで複数のデータファイルをロードする際に、すべてのロードが成功するか失敗するかのいずれかであることを意味します。いくつかのデータファイルのロードが成功し、他のファイルのロードが失敗することはありません。

Broker Load は、データロード時のデータ変換をサポートし、データロード中に UPSERT および DELETE 操作によるデータ変更をサポートします。詳細については、[Transform data at loading](../loading/Etl_in_loading.md) および [Change data through loading](../loading/Load_to_Primary_Key_tables.md) を参照してください。

<InsertPrivNote />

## 背景情報

v2.4 以前では、StarRocks は Broker Load ジョブを実行する際に、StarRocks クラスターと外部ストレージシステムとの間に接続を確立するためにブローカーに依存していました。そのため、ロードステートメントで使用するブローカーを指定するために `WITH BROKER "<broker_name>"` を入力する必要があります。これを「ブローカー ベースのロード」と呼びます。ブローカーは、ファイルシステムインターフェースと統合された独立したステートレスサービスです。ブローカーを使用すると、StarRocks は外部ストレージシステムに保存されているデータファイルにアクセスして読み取ることができ、独自のコンピューティングリソースを使用してこれらのデータファイルのデータを事前処理およびロードできます。

v2.5 以降では、StarRocks は Broker Load ジョブを実行する際に、StarRocks クラスターと外部ストレージシステムとの間に接続を確立するためにブローカーに依存しなくなりました。そのため、ロードステートメントでブローカーを指定する必要はありませんが、`WITH BROKER` キーワードは保持する必要があります。これを「ブローカー フリー ロード」と呼びます。

データが HDFS に保存されている場合、ブローカー フリー ロードが機能しない状況に遭遇することがあります。これは、データが複数の HDFS クラスターにまたがって保存されている場合や、複数の Kerberos ユーザーが設定されている場合に発生する可能性があります。このような状況では、代わりにブローカー ベースのロードを使用することができます。これを成功させるには、少なくとも 1 つの独立したブローカーグループが展開されていることを確認してください。これらの状況で認証構成および HA 構成を指定する方法については、[HDFS](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#hdfs) を参照してください。

> **注意**
>
> StarRocks クラスターに展開されているブローカーを確認するには、[SHOW BROKER](../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BROKER.md) ステートメントを使用できます。ブローカーが展開されていない場合は、[Deploy a broker](../deployment/deploy_broker.md) に記載された手順に従ってブローカーを展開できます。

## サポートされているデータファイル形式

Broker Load は、次のデータファイル形式をサポートしています。

- CSV

- Parquet

- ORC

> **注意**
>
> CSV データについては、次の点に注意してください。
>
> - テキストデリミタとして、長さが 50 バイトを超えない UTF-8 文字列（カンマ（,）、タブ、パイプ（|）など）を使用できます。
> - Null 値は `\N` を使用して示されます。たとえば、データファイルが 3 つの列で構成されており、そのデータファイルのレコードが最初と 3 番目の列にデータを保持し、2 番目の列にデータがない場合、この状況では、2 番目の列に `\N` を使用して Null 値を示す必要があります。つまり、レコードは `a,\N,b` としてコンパイルする必要があり、`a,,b` ではありません。`a,,b` は、レコードの 2 番目の列が空の文字列を保持していることを示します。

## サポートされているストレージシステム

Broker Load は、次のストレージシステムをサポートしています。

- HDFS

- AWS S3

- Google GCS

- MinIO などの他の S3 互換ストレージシステム

- Microsoft Azure Storage

## 動作の仕組み

ロードジョブを FE に送信すると、FE はクエリプランを生成し、利用可能な BE の数とロードしたいデータファイルのサイズに基づいてクエリプランを部分に分割し、クエリプランの各部分を利用可能な BE に割り当てます。ロード中、関与する各 BE は HDFS またはクラウドストレージシステムからデータファイルのデータを取得し、データを事前処理してから StarRocks クラスターにデータをロードします。すべての BE がクエリプランの部分を完了した後、FE はロードジョブが成功したかどうかを判断します。

次の図は、Broker Load ジョブのワークフローを示しています。

![Workflow of Broker Load](../_assets/broker_load_how-to-work_en.png)

## 基本操作

### 複数テーブルのロードジョブを作成する

このトピックでは、CSV を例にとり、複数のデータファイルを複数のテーブルにロードする方法を説明します。他のファイル形式でデータをロードする方法や Broker Load の構文およびパラメーターの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

StarRocks では、いくつかのリテラルが SQL 言語によって予約キーワードとして使用されていることに注意してください。これらのキーワードを SQL ステートメントで直接使用しないでください。SQL ステートメントでそのようなキーワードを使用したい場合は、バッククォート (`) で囲んでください。[Keywords](../sql-reference/sql-statements/keywords.md) を参照してください。

#### データ例

1. ローカルファイルシステムに CSV ファイルを作成します。

   a. `file1.csv` という名前の CSV ファイルを作成します。このファイルは、順にユーザー ID、ユーザー名、ユーザースコアを表す 3 つの列で構成されています。

      ```Plain
      1,Lily,23
      2,Rose,23
      3,Alice,24
      4,Julia,25
      ```

   b. `file2.csv` という名前の CSV ファイルを作成します。このファイルは、順に都市 ID と都市名を表す 2 つの列で構成されています。

      ```Plain
      200,'Beijing'
      ```

2. StarRocks データベース `test_db` に StarRocks テーブルを作成します。

   > **注意**
   >
   > v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できるようになりました。バケット数を手動で設定する必要はありません。詳細については、[determine the number of buckets](../table_design/data_distribution/Data_distribution.md#determine-the-number-of-buckets) を参照してください。

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

3. `file1.csv` と `file2.csv` を HDFS クラスターの `/user/starrocks/` パス、AWS S3 バケット `bucket_s3` の `input` フォルダー、Google GCS バケット `bucket_gcs` の `input` フォルダー、MinIO バケット `bucket_minio` の `input` フォルダー、および Azure Storage の指定されたパスにアップロードします。

#### HDFS からデータをロードする

HDFS クラスターの `/user/starrocks` パスから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードするには、次のステートメントを実行します。

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

上記の例では、`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#hdfs) を参照してください。

#### AWS S3 からデータをロードする

AWS S3 バケット `bucket_s3` の `input` フォルダーから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードするには、次のステートメントを実行します。

```SQL
LOAD LABEL test_db.label2
(
    DATA INFILE("s3a://bucket_s3/input/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("s3a://bucket_s3/input/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
);
```

> **注意**
>
> Broker Load は、S3A プロトコルに従って AWS S3 にアクセスすることのみをサポートしています。そのため、AWS S3 からデータをロードする際には、ファイルパスとして渡す S3 URI の `s3://` を `s3a://` に置き換える必要があります。

上記の例では、`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#aws-s3) を参照してください。

v3.1 以降、StarRocks は、Parquet 形式または ORC 形式のファイルのデータを AWS S3 から直接ロードすることをサポートしており、外部テーブルを最初に作成する手間を省くことができます。詳細については、[Load data using INSERT > Insert data directly from files in an external source using TABLE keyword](../loading/InsertInto.md#insert-data-directly-from-files-in-an-external-source-using-files) を参照してください。

#### Google GCS からデータをロードする

Google GCS バケット `bucket_gcs` の `input` フォルダーから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードするには、次のステートメントを実行します。

```SQL
LOAD LABEL test_db.label3
(
    DATA INFILE("gs://bucket_gcs/input/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("gs://bucket_gcs/input/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
);
```

> **注意**
>
> Broker Load は、gs プロトコルに従って Google GCS にアクセスすることのみをサポートしています。そのため、Google GCS からデータをロードする際には、ファイルパスとして渡す GCS URI に `gs://` を含める必要があります。

上記の例では、`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#google-gcs) を参照してください。

#### 他の S3 互換ストレージシステムからデータをロードする

MinIO を例にとります。MinIO バケット `bucket_minio` の `input` フォルダーから `file1.csv` と `file2.csv` をそれぞれ `table1` と `table2` にロードするには、次のステートメントを実行します。

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

上記の例では、`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#other-s3-compatible-storage-system) を参照してください。

#### Microsoft Azure Storage からデータをロードする

Azure Storage の指定されたパスから `file1.csv` と `file2.csv` をロードするには、次のステートメントを実行します。

```SQL
LOAD LABEL test_db.label8
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, city)
)
WITH BROKER
(
    StorageCredentialParams
);
```

> **注意**
  >
  > Azure Storage からデータをロードする際には、使用するアクセスプロトコルと特定のストレージサービスに基づいて、どのプレフィックスを使用するかを決定する必要があります。上記の例では、Blob Storage を例にとっています。
  >
  > - Blob Storage からデータをロードする際には、ストレージアカウントにアクセスするために使用されるプロトコルに基づいて、ファイルパスに `wasb://` または `wasbs://` をプレフィックスとして含める必要があります。
  >   - Blob Storage が HTTP 経由でのみアクセスを許可する場合、プレフィックスとして `wasb://` を使用します。例: `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/*`
  >   - Blob Storage が HTTPS 経由でのみアクセスを許可する場合、プレフィックスとして `wasbs://` を使用します。例: `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/*`
  > - Data Lake Storage Gen1 からデータをロードする際には、ファイルパスに `adl://` をプレフィックスとして含める必要があります。例: `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>`
  > - Data Lake Storage Gen2 からデータをロードする際には、ストレージアカウントにアクセスするために使用されるプロトコルに基づいて、ファイルパスに `abfs://` または `abfss://` をプレフィックスとして含める必要があります。
  >   - Data Lake Storage Gen2 が HTTP 経由でのみアクセスを許可する場合、プレフィックスとして `abfs://` を使用します。例: `abfs://<container>@<storage_account>.dfs.core.windows.net/<file_name>`
  >   - Data Lake Storage Gen2 が HTTPS 経由でのみアクセスを許可する場合、プレフィックスとして `abfss://` を使用します。例: `abfss://<container>@<storage_account>.dfs.core.windows.net/<file_name>`

上記の例では、`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#microsoft-azure-storage) を参照してください。

#### データをクエリする

HDFS クラスター、AWS S3 バケット、または Google GCS バケットからのデータのロードが完了した後、SELECT ステートメントを使用して StarRocks テーブルのデータをクエリし、ロードが成功したことを確認できます。

1. `table1` のデータをクエリするには、次のステートメントを実行します。

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

1. `table2` のデータをクエリするには、次のステートメントを実行します。

   ```SQL
   MySQL [test_db]> SELECT * FROM table2;
   +------+--------+
   | id   | city   |
   +------+--------+
   | 200  | Beijing|
   +------+--------+
   4 rows in set (0.01 sec)
   ```

### 単一テーブルのロードジョブを作成する

指定されたパスから単一のデータファイルまたはすべてのデータファイルを単一の宛先テーブルにロードすることもできます。たとえば、AWS S3 バケット `bucket_s3` に `input` という名前のフォルダーが含まれているとします。この `input` フォルダーには複数のデータファイルが含まれており、そのうちの 1 つは `file1.csv` という名前です。これらのデータファイルは、`table1` と同じ数の列で構成されており、各データファイルの列は `table1` の列と順番に 1 対 1 でマッピングできます。

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

上記の例では、`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#aws-s3) を参照してください。

### ロードジョブを表示する

Broker Load では、SHOW LOAD ステートメントまたは `curl` コマンドを使用して、ロードジョブを表示できます。

#### SHOW LOAD を使用する

詳細については、[SHOW LOAD](../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md) を参照してください。

#### curl を使用する

構文は次のとおりです。

```Bash
curl --location-trusted -u <username>:<password> \
    'http://<fe_host>:<fe_http_port>/api/<database_name>/_load_info?label=<label_name>'
```

> **注意**
>
> パスワードが設定されていないアカウントを使用する場合は、`<username>:` のみを入力する必要があります。

たとえば、`test_db` データベース内の `label1` というラベルを持つロードジョブに関する情報を表示するには、次のコマンドを実行できます。

```Bash
curl --location-trusted -u <username>:<password> \
    'http://<fe_host>:<fe_http_port>/api/test_db/_load_info?label=label1'
```

`curl` コマンドは、指定されたラベルを持つ最も最近実行されたロードジョブに関する情報を JSON オブジェクト `jobInfo` として返します。

```JSON
{"jobInfo":{"dbName":"default_cluster:test_db","tblNames":["table1_simple"],"label":"label1","state":"FINISHED","failMsg":"","trackingUrl":""},"status":"OK","msg":"Success"}%
```

次の表は、`jobInfo` のパラメータを説明しています。

| **パラメータ** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| dbName        | データがロードされるデータベースの名前           |
| tblNames      | データがロードされるテーブルの名前             |
| label         | ロードジョブのラベル                                   |
| state         | ロードジョブのステータス。 有効な値:<ul><li>`PENDING`: ロードジョブはスケジュール待ちのキューにあります。</li><li>`QUEUEING`: ロードジョブはスケジュール待ちのキューにあります。</li><li>`LOADING`: ロードジョブは実行中です。</li><li>`PREPARED`: トランザクションがコミットされました。</li><li>`FINISHED`: ロードジョブは成功しました。</li><li>`CANCELLED`: ロードジョブは失敗しました。</li></ul>詳細については、[Overview of data loading](../loading/Loading_intro.md) の「非同期ロード」セクションを参照してください。 |
| failMsg       | ロードジョブが失敗した理由。ロードジョブの `state` 値が `PENDING`、`LOADING`、または `FINISHED` の場合、`failMsg` パラメータには `NULL` が返されます。ロードジョブの `state` 値が `CANCELLED` の場合、`failMsg` パラメータに返される値は、`type` と `msg` の 2 つの部分で構成されます。<ul><li>`type` 部分は、次のいずれかの値になります。</li><ul><li>`USER_CANCEL`: ロードジョブが手動でキャンセルされました。</li><li>`ETL_SUBMIT_FAIL`: ロードジョブの送信に失敗しました。</li><li>`ETL-QUALITY-UNSATISFIED`: 不合格データの割合が `max-filter-ratio` パラメータの値を超えたため、ロードジョブが失敗しました。</li><li>`LOAD-RUN-FAIL`: ロードジョブが `LOADING` ステージで失敗しました。</li><li>`TIMEOUT`: ロードジョブが指定されたタイムアウト期間内に完了できませんでした。</li><li>`UNKNOWN`: 不明なエラーによりロードジョブが失敗しました。</li></ul><li>`msg` 部分は、ロード失敗の詳細な原因を提供します。</li></ul> |
| trackingUrl   | ロードジョブで検出された不合格データにアクセスするために使用される URL。`curl` または `wget` コマンドを使用して URL にアクセスし、不合格データを取得できます。不合格データが検出されない場合、`trackingUrl` パラメータには `NULL` が返されます。 |
| status        | ロードジョブに対する HTTP リクエストのステータス。 有効な値: `OK` および `Fail`。 |
| msg           | ロードジョブに対する HTTP リクエストのエラー情報。  |

### ロードジョブをキャンセルする

ロードジョブが **CANCELLED** または **FINISHED** ステージにない場合、[CANCEL LOAD](../sql-reference/sql-statements/loading_unloading/CANCEL_LOAD.md) ステートメントを使用してジョブをキャンセルできます。

たとえば、`test_db` データベース内の `label1` というラベルを持つロードジョブをキャンセルするには、次のステートメントを実行できます。

```SQL
CANCEL LOAD
FROM test_db
WHERE LABEL = "label";
```

## ジョブの分割と並行実行

Broker Load ジョブは、1 つ以上のタスクに分割され、並行して実行されることがあります。ロードジョブ内のタスクは単一のトランザクション内で実行されます。すべて成功するか失敗する必要があります。StarRocks は、`LOAD` ステートメントで `data_desc` を宣言する方法に基づいて各ロードジョブを分割します。

- 複数の `data_desc` パラメータを宣言し、それぞれが異なるテーブルを指定している場合、各テーブルのデータをロードするためのタスクが生成されます。

- 複数の `data_desc` パラメータを宣言し、それぞれが同じテーブルの異なるパーティションを指定している場合、各パーティションのデータをロードするためのタスクが生成されます。

さらに、各タスクは 1 つ以上のインスタンスにさらに分割され、StarRocks クラスターの BE に均等に分散され、並行して実行されます。StarRocks は、次の [FE 設定](../administration/management/FE_configuration.md) に基づいて各タスクを分割します。

- `min_bytes_per_broker_scanner`: 各インスタンスによって処理されるデータの最小量。デフォルトの量は 64 MB です。

- `load_parallel_instance_num`: 個々の BE で各ロードジョブに許可される並行インスタンスの数。デフォルトの数は 1 です。
  
  個々のタスク内のインスタンスの数を計算するには、次の式を使用できます。

  **個々のタスク内のインスタンスの数 = min(個々のタスクによってロードされるデータの量/`min_bytes_per_broker_scanner`, `load_parallel_instance_num` x BE の数)**

ほとんどの場合、各ロードジョブには 1 つの `data_desc` のみが宣言され、各ロードジョブは 1 つのタスクにのみ分割され、タスクは BE の数と同じ数のインスタンスに分割されます。

## 関連する設定項目

[FE 設定項目](../administration/management/FE_configuration.md) `max_broker_load_job_concurrency` は、StarRocks クラスター内で同時に実行できる Broker Load ジョブの最大数を指定します。

StarRocks v2.4 以前では、特定の期間内に送信された Broker Load ジョブの総数が最大数を超える場合、過剰なジョブはキューに入れられ、送信時間に基づいてスケジュールされます。

StarRocks v2.5 以降では、特定の期間内に送信された Broker Load ジョブの総数が最大数を超える場合、過剰なジョブは優先順位に基づいてキューに入れられ、スケジュールされます。ジョブの作成時に `priority` パラメータを使用してジョブの優先順位を指定できます。[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md#opt_properties) を参照してください。また、[ALTER LOAD](../sql-reference/sql-statements/loading_unloading/ALTER_LOAD.md) を使用して、**QUEUEING** または **LOADING** 状態の既存のジョブの優先順位を変更することもできます。