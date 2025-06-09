---
displayed_sidebar: docs
unlisted: True
---

# \<SOURCE\> テンプレートからデータをロード

## テンプレートの指示

### スタイルについての注意

技術文書には、他の文書へのリンクが随所にあります。この文書を見ると、ページからのリンクが少なく、ほとんどのリンクがドキュメントの最後の **詳細情報** セクションにあることに気付くかもしれません。すべてのキーワードを別のページにリンクする必要はありません。読者が `CREATE TABLE` の意味を知っていると仮定してください。もし知らない場合は、検索バーをクリックして調べることができます。他のオプションがあり、その詳細が **詳細情報** セクションに記載されていることを読者に伝えるために、ドキュメントにメモを入れることは問題ありません。これにより、情報が必要な人は、現在のタスクを完了した後にそれを読むことができることを知ることができます。

### テンプレート

このテンプレートは、Amazon S3 からデータをロードするプロセスに基づいており、その一部は他のソースからのロードには適用されません。このテンプレートの流れに集中し、すべてのセクションを含めることを心配しないでください。流れは次のように意図されています。

#### はじめに

このガイドに従った場合の最終結果を読者に知らせる導入文です。S3 ドキュメントの場合、最終結果は「非同期または同期の方法で S3 からデータをロードすること」です。

#### なぜ？

- この技術で解決されるビジネス問題の説明
- 記載された方法の利点と欠点（ある場合）

#### データフローまたはその他の図

図や画像は役立つことがあります。複雑な技術を説明している場合、画像が役立つなら使用してください。例えば、Superset を使用してデータを分析する場合のように、視覚的な結果を生む技術を説明している場合は、最終製品の画像を必ず含めてください。

データフローが明白でない場合は、データフロー図を使用してください。コマンドが StarRocks に複数のプロセスを実行させ、それらのプロセスの出力を組み合わせてデータを操作する場合、データフローの説明が必要になることがあります。このテンプレートでは、データをロードする方法が2つ説明されています。1つはシンプルで、データフローセクションがありません。もう1つはより複雑で（StarRocks が複雑な作業を処理し、ユーザーではありません！）、複雑なオプションにはデータフローセクションが含まれています。

#### 検証セクション付きの例

例は構文の詳細や他の深い技術的詳細の前に来るべきです。多くの読者は、コピー、ペースト、修正できる特定の技術を見つけるためにドキュメントを訪れます。

可能であれば、動作する例を提供し、使用するデータセットを含めてください。このテンプレートの例では、AWS アカウントを持ち、キーとシークレットで認証できる人が使用できる S3 に保存されたデータセットを使用しています。データセットを提供することで、読者は記述された技術を完全に体験できるため、例はより価値があります。

例が記述通りに動作することを確認してください。これは2つのことを意味します。

1. 提示された順序でコマンドを実行したこと
2. 必要な前提条件を含めたこと。例えば、例がデータベース `foo` を参照している場合、おそらく `CREATE DATABASE foo;`、`USE foo;` で前置きする必要があります。

検証は非常に重要です。説明しているプロセスにいくつかのステップが含まれている場合、何かが達成されたときに検証ステップを含めてください。これにより、読者が最後に到達し、ステップ10でタイプミスをしていたことに気付くのを避けることができます。この例では、**進捗を確認** と `DESCRIBE user_behavior_inferred;` ステップが検証用です。

#### 詳細情報

テンプレートの最後には、関連情報へのリンクを配置する場所があります。これには、本文で言及したオプション情報へのリンクも含まれます。

### テンプレートに埋め込まれたメモ

テンプレートのメモは、テンプレートを進める際に注意を引くために、ドキュメントのメモのフォーマットとは意図的に異なっています。進める際に太字の斜体のメモを削除してください。

***Note: 説明文***

## 最後に、テンプレートの開始

***Note: 推奨される選択肢が複数ある場合は、イントロで読者に伝えてください。例えば、S3 からのロードには、同期ロードと非同期ロードのオプションがあります。***

StarRocks は S3 からデータをロードするための2つのオプションを提供します。

1. Broker Load を使用した非同期ロード
2. `FILES()` テーブル関数を使用した同期ロード

***Note: なぜ一方を選ぶのかを読者に伝えてください。***

小さなデータセットは通常、`FILES()` テーブル関数を使用して同期的にロードされ、大きなデータセットは Broker Load を使用して非同期的にロードされます。2つの方法には異なる利点があり、以下で説明されています。

> **NOTE**
>
> StarRocks テーブルにデータをロードするには、StarRocks テーブルに対する INSERT 権限を持つユーザーとしてのみ可能です。INSERT 権限を持っていない場合は、[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) に従って、使用する StarRocks クラスターに接続するユーザーに INSERT 権限を付与してください。

## Broker Load の使用

非同期の Broker Load プロセスは、S3 への接続を確立し、データを取得し、StarRocks にデータを保存します。

### Broker Load の利点

- Broker Load は、ロード中にデータ変換、UPSERT、および DELETE 操作をサポートします。
- Broker Load はバックグラウンドで実行され、クライアントはジョブが続行するために接続し続ける必要はありません。
- Broker Load は長時間実行されるジョブに推奨され、デフォルトのタイムアウトは4時間です。
- Parquet および ORC ファイル形式に加えて、Broker Load は CSV ファイルをサポートします。

### データフロー

***Note: 複数のコンポーネントやステップを含むプロセスは、図を用いることで理解しやすくなることがあります。この例には、ユーザーが Broker Load オプションを選択したときに発生するステップを説明するのに役立つ図が含まれています。***

![Broker Load のワークフロー](../_assets/broker_load_how-to-work_en.png)

1. ユーザーがロードジョブを作成します。
2. フロントエンド (FE) がクエリプランを作成し、そのプランをバックエンドノード (BE) に配布します。
3. バックエンドノード (BE) がソースからデータを取得し、StarRocks にデータをロードします。

### 典型的な例

テーブルを作成し、S3 から Parquet ファイルを取得するロードプロセスを開始し、データロードの進捗と成功を確認します。

> **NOTE**
>
> 例では Parquet 形式のサンプルデータセットを使用しています。CSV または ORC ファイルをロードしたい場合、その情報はこのページの下部にリンクされています。

#### テーブルを作成

テーブル用のデータベースを作成します。

```SQL
CREATE DATABASE IF NOT EXISTS project;
USE project;
```

テーブルを作成します。このスキーマは、StarRocks アカウントにホストされている S3 バケット内のサンプルデータセットに一致します。

```SQL
DROP TABLE IF EXISTS user_behavior;

CREATE TABLE `user_behavior` (
    `UserID` int(11),
    `ItemID` int(11),
    `CategoryID` int(11),
    `BehaviorType` varchar(65533),
    `Timestamp` datetime
) ENGINE=OLAP 
DUPLICATE KEY(`UserID`)
DISTRIBUTED BY HASH(`UserID`)
PROPERTIES (
    "replication_num" = "1"
);
```

#### 接続の詳細を収集

> **NOTE**
>
> 例では IAM ユーザー認証を使用しています。他の認証方法も利用可能であり、このページの下部にリンクされています。

S3 からデータをロードするには、以下が必要です。

- S3 バケット
- バケット内の特定のオブジェクトにアクセスする場合の S3 オブジェクトキー（オブジェクト名）。オブジェクトキーには、S3 オブジェクトがサブフォルダに保存されている場合、プレフィックスを含めることができます。完全な構文は **詳細情報** にリンクされています。
- S3 リージョン
- アクセスキーとシークレット

#### Broker Load を開始

このジョブには4つの主要セクションがあります。

- `LABEL`: `LOAD` ジョブの状態をクエリする際に使用される文字列。
- `LOAD` 宣言: ソース URI、宛先テーブル、およびソースデータ形式。
- `BROKER`: ソースの接続詳細。
- `PROPERTIES`: タイムアウト値およびこのジョブに適用するその他のプロパティ。

> **NOTE**
>
> これらの例で使用されているデータセットは、StarRocks アカウントの S3 バケットにホストされています。任意の有効な `aws.s3.access_key` と `aws.s3.secret_key` を使用できます。オブジェクトは任意の AWS 認証済みユーザーが読み取れるためです。以下のコマンドで `AAA` と `BBB` をあなたの資格情報に置き換えてください。

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("s3://starrocks-examples/user_behavior_sample_data.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
 (
    "aws.s3.enable_ssl" = "true",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
 )
PROPERTIES
(
    "timeout" = "72000"
);
```

#### 進捗を確認

`information_schema.loads` テーブルをクエリして進捗を追跡します。複数の `LOAD` ジョブが実行中の場合、ジョブに関連付けられた `LABEL` でフィルタリングできます。以下の出力には、ロードジョブ `user_behavior` の2つのエントリがあります。最初のレコードは `CANCELLED` 状態を示しています。出力の最後までスクロールすると、`listPath failed` と表示されます。2番目のレコードは、有効な AWS IAM アクセスキーとシークレットでの成功を示しています。

```SQL
SELECT * FROM information_schema.loads;
```

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior';
```

```plaintext
JOB_ID|LABEL                                      |DATABASE_NAME|STATE    |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                           |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG                             |TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
------+-------------------------------------------+-------------+---------+-------------------+------+--------+---------+-------------+---------------+---------+--------+----------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+------------+------------+--------------------+
 10121|user_behavior                              |project      |CANCELLED|ETL:N/A; LOAD:N/A  |BROKER|NORMAL  |        0|            0|              0|        0|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:59:30|                   |                   |                   |2023-08-10 14:59:34|{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}}                                                                                        |type:ETL_RUN_FAIL; msg:listPath failed|            |            |                    |
 10106|user_behavior                              |project      |FINISHED |ETL:100%; LOAD:100%|BROKER|NORMAL  | 86953525|            0|              0| 86953525|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:50:15|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:55:10|{"All backends":{"a5fe5e1d-d7d0-4826-ba99-c7348f9a5f2f":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":2710603082,"InternalTableLoadRows":86953525,"ScanBytes":1225637388,"ScanRows":86953525,"TaskNumber":1,"Unfinished backends":{"a5|                                      |            |            |                    |
```

この時点でデータのサブセットを確認することもできます。

```SQL
SELECT * from user_behavior LIMIT 10;
```

```plaintext
UserID|ItemID|CategoryID|BehaviorType|Timestamp          |
------+------+----------+------------+-------------------+
171146| 68873|   3002561|pv          |2017-11-30 07:11:14|
171146|146539|   4672807|pv          |2017-11-27 09:51:41|
171146|146539|   4672807|pv          |2017-11-27 14:08:33|
171146|214198|   1320293|pv          |2017-11-25 22:38:27|
171146|260659|   4756105|pv          |2017-11-30 05:11:25|
171146|267617|   4565874|pv          |2017-11-27 14:01:25|
171146|329115|   2858794|pv          |2017-12-01 02:10:51|
171146|458604|   1349561|pv          |2017-11-25 22:49:39|
171146|458604|   1349561|pv          |2017-11-27 14:03:44|
171146|478802|    541347|pv          |2017-12-02 04:52:39|
```

## `FILES()` テーブル関数の使用

### `FILES()` の利点

`FILES()` は、Parquet データの列のデータ型を推測し、StarRocks テーブルのスキーマを生成することができます。これにより、S3 からファイルを直接 `SELECT` でクエリするか、Parquet ファイルのスキーマに基づいて StarRocks が自動的にテーブルを作成することができます。

> **NOTE**
>
> スキーマ推測はバージョン 3.1 の新機能であり、Parquet 形式のみで提供され、ネストされた型はまだサポートされていません。

### 典型的な例

`FILES()` テーブル関数を使用した3つの例があります。

- S3 から直接データをクエリする
- スキーマ推測を使用してテーブルを作成し、ロードする
- 手動でテーブルを作成し、データをロードする

> **NOTE**
>
> これらの例で使用されているデータセットは、StarRocks アカウントの S3 バケットにホストされています。任意の有効な `aws.s3.access_key` と `aws.s3.secret_key` を使用できます。オブジェクトは任意の AWS 認証済みユーザーが読み取れるためです。以下のコマンドで `AAA` と `BBB` をあなたの資格情報に置き換えてください。

#### S3 から直接クエリ

S3 から `FILES()` を使用して直接クエリすることで、テーブルを作成する前にデータセットの内容をプレビューすることができます。例えば：

- データを保存せずにデータセットをプレビューする。
- 最小値と最大値をクエリして、どのデータ型を使用するかを決定する。
- NULL 値を確認する。

```sql
SELECT * FROM FILES(
    "path" = "s3://starrocks-examples/user_behavior_sample_data.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
) LIMIT 10;
```

> **NOTE**
>
> 列名は Parquet ファイルによって提供されていることに注意してください。

```plaintext
UserID|ItemID |CategoryID|BehaviorType|Timestamp          |
------+-------+----------+------------+-------------------+
     1|2576651|    149192|pv          |2017-11-25 01:21:25|
     1|3830808|   4181361|pv          |2017-11-25 07:04:53|
     1|4365585|   2520377|pv          |2017-11-25 07:49:06|
     1|4606018|   2735466|pv          |2017-11-25 13:28:01|
     1| 230380|    411153|pv          |2017-11-25 21:22:22|
     1|3827899|   2920476|pv          |2017-11-26 16:24:33|
     1|3745169|   2891509|pv          |2017-11-26 19:44:31|
     1|1531036|   2920476|pv          |2017-11-26 22:02:12|
     1|2266567|   4145813|pv          |2017-11-27 00:11:11|
     1|2951368|   1080785|pv          |2017-11-27 02:47:08|
```

#### スキーマ推測を使用してテーブルを作成

これは前の例の続きです。前のクエリを `CREATE TABLE` でラップして、スキーマ推測を使用してテーブル作成を自動化します。Parquet ファイルを使用する場合、列名と型は Parquet 形式に含まれており、StarRocks がスキーマを推測するため、テーブルを作成する際に列名と型は必要ありません。

> **NOTE**
>
> スキーマ推測を使用する場合の `CREATE TABLE` の構文では、レプリカの数を設定することはできませんので、テーブルを作成する前に設定してください。以下の例は、単一のレプリカを持つシステム用です：
>
> `ADMIN SET FRONTEND CONFIG ('default_replication_num' ="1");`

```sql
CREATE DATABASE IF NOT EXISTS project;
USE project;

CREATE TABLE `user_behavior_inferred` AS
SELECT * FROM FILES(
    "path" = "s3://starrocks-examples/user_behavior_sample_data.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
);
```

```SQL
DESCRIBE user_behavior_inferred;
```

```plaintext
Field       |Type            |Null|Key  |Default|Extra|
------------+----------------+----+-----+-------+-----+
UserID      |bigint          |YES |true |       |     |
ItemID      |bigint          |YES |true |       |     |
CategoryID  |bigint          |YES |true |       |     |
BehaviorType|varchar(1048576)|YES |false|       |     |
Timestamp   |varchar(1048576)|YES |false|       |     |
```

> **NOTE**
>
> 手動で作成したスキーマと推測されたスキーマを比較してください：
>
> - データ型
> - NULL 許可
> - キーフィールド

```SQL
SELECT * from user_behavior_inferred LIMIT 10;
```

```plaintext
UserID|ItemID|CategoryID|BehaviorType|Timestamp          |
------+------+----------+------------+-------------------+
171146| 68873|   3002561|pv          |2017-11-30 07:11:14|
171146|146539|   4672807|pv          |2017-11-27 09:51:41|
171146|146539|   4672807|pv          |2017-11-27 14:08:33|
171146|214198|   1320293|pv          |2017-11-25 22:38:27|
171146|260659|   4756105|pv          |2017-11-30 05:11:25|
171146|267617|   4565874|pv          |2017-11-27 14:01:25|
171146|329115|   2858794|pv          |2017-12-01 02:10:51|
171146|458604|   1349561|pv          |2017-11-25 22:49:39|
171146|458604|   1349561|pv          |2017-11-27 14:03:44|
171146|478802|    541347|pv          |2017-12-02 04:52:39|
```

#### 既存のテーブルにロード

挿入するテーブルをカスタマイズしたい場合があります。例えば：

- 列のデータ型、NULL 許可設定、またはデフォルト値
- キーの種類と列
- 分散
- その他

> **NOTE**
>
> 最も効率的なテーブル構造を作成するには、データの使用方法と列の内容に関する知識が必要です。このドキュメントはテーブル設計をカバーしていませんが、ページの最後に **詳細情報** へのリンクがあります。

この例では、テーブルがクエリされる方法と Parquet ファイル内のデータに関する知識に基づいてテーブルを作成しています。Parquet ファイル内のデータに関する知識は、S3 でファイルを直接クエリすることで得ることができます。

- S3 でファイルをクエリすると、`Timestamp` 列が `datetime` データ型に一致するデータを含んでいることが示されるため、以下の DDL で列型が指定されています。
- S3 でデータをクエリすることで、データセットに NULL 値がないことがわかるため、DDL ではどの列も NULL 許可として設定されていません。
- 予想されるクエリタイプに基づいて、ソートキーとバケッティング列は `UserID` 列に設定されています（このデータのユースケースによっては、`ItemID` をソートキーとして使用することを選択するかもしれません）。

```SQL
CREATE TABLE `user_behavior_declared` (
    `UserID` int(11),
    `ItemID` int(11),
    `CategoryID` int(11),
    `BehaviorType` varchar(65533),
    `Timestamp` datetime
) ENGINE=OLAP 
DUPLICATE KEY(`UserID`)
DISTRIBUTED BY HASH(`UserID`)
PROPERTIES (
    "replication_num" = "1"
);
```

テーブルを作成した後、`INSERT INTO` … `SELECT FROM FILES()` を使用してロードできます。

```SQL
INSERT INTO user_behavior_declared
  SELECT * FROM FILES(
    "path" = "s3://starrocks-examples/user_behavior_sample_data.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
);
```

## 詳細情報

- 同期および非同期データロードの詳細については、[Loading concepts](./loading_introduction/loading_concepts.md) を参照してください。
- Broker Load がロード中にデータ変換をサポートする方法については、[Transform data at loading](../loading/Etl_in_loading.md) および [Change data through loading](../loading/Load_to_Primary_Key_tables.md) を参照してください。
- このドキュメントでは IAM ユーザー認証のみをカバーしています。他のオプションについては、[authenticate to AWS resources](../integrations/authenticate_to_aws_resources.md) を参照してください。
- [AWS CLI Command Reference](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html) は S3 URI を詳細にカバーしています。
- [table design](../table_design/StarRocks_table_design.md) についてさらに学びましょう。
- Broker Load は、上記の例よりも多くの構成および使用オプションを提供しており、詳細は [Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) にあります。