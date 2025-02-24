---
displayed_sidebar: docs
unlisted: True
---

# データを \<SOURCE\> からロードするテンプレート

## テンプレートの説明

### スタイルについての注意

技術文書には通常、他の文書へのリンクが多数含まれています。この文書を見ると、ページからのリンクが少なく、ほとんどのリンクが文書の下部にある**詳細情報**セクションにあることに気づくかもしれません。すべてのキーワードを別のページにリンクする必要はありません。読者が `CREATE TABLE` の意味を知っていると仮定し、知らない場合は検索バーをクリックして調べることができると考えてください。文書内に他のオプションがあり、詳細は**詳細情報**セクションに記載されていることを読者に伝えるメモを入れるのは問題ありません。これにより、情報が必要な人が、現在のタスクを完了した後に***後で***読むことができることを知ることができます。

### テンプレート

このテンプレートは、Amazon S3 からデータをロードするプロセスに基づいており、その一部は他のソースからのロードには適用されません。このテンプレートの流れに集中し、すべてのセクションを含めることを心配しないでください。流れは次のようにすることを意図しています：

#### はじめに

このガイドに従うとどのような結果が得られるかを読者に知らせるための導入文です。S3 ドキュメントの場合、最終結果は「非同期または同期の方法で S3 からデータをロードすること」です。

#### なぜ？

- この技術で解決されるビジネス問題の説明
- 記述された方法の利点と欠点（ある場合）

#### データフローまたはその他の図

図や画像は役立つことがあります。複雑な技術を説明している場合、画像が役立つなら使用してください。Superset を使用してデータを分析するなど、視覚的な結果を生み出す技術を説明している場合は、最終製品の画像を必ず含めてください。

データフローが明確でない場合は、データフロー図を使用してください。コマンドが StarRocks に複数のプロセスを実行させ、それらのプロセスの出力を結合してデータを操作する場合、おそらくデータフローの説明が必要です。このテンプレートでは、データをロードするための 2 つの方法が説明されています。1 つは簡単で、データフローセクションがありません。もう 1 つは複雑で（StarRocks が複雑な作業を処理し、ユーザーではありません！）、複雑なオプションにはデータフローセクションが含まれています。

#### 検証セクション付きの例

例は構文の詳細やその他の技術的な詳細の前に来るべきです。多くの読者は、コピー、ペースト、修正できる特定の技術を見つけるために文書を訪れます。

可能であれば、使用するデータセットを含む例を示してください。このテンプレートの例では、AWS アカウントを持ち、キーとシークレットで認証できる人が使用できる S3 に保存されたデータセットを使用しています。データセットを提供することで、例は読者にとってより価値のあるものとなり、説明された技術を完全に体験できます。

例が記述された通りに動作することを確認してください。これは 2 つのことを意味します：

1. 提示された順序でコマンドを実行した
2. 必要な前提条件を含めた。たとえば、例がデータベース `foo` を参照している場合、おそらくそれを `CREATE DATABASE foo;`、`USE foo;` で前置する必要があります。

検証は非常に重要です。説明しているプロセスがいくつかのステップを含む場合、何かが達成されたときに検証ステップを含めてください。これにより、読者が最後に到達し、ステップ 10 でタイプミスがあったことに気づくのを避けることができます。この例では、**進捗の確認**と `DESCRIBE user_behavior_inferred;` ステップが検証のためのものです。

#### 詳細情報

テンプレートの最後には、本文で言及したオプション情報を含む関連情報へのリンクを配置する場所があります。

### テンプレートに埋め込まれたメモ

テンプレートのメモは、テンプレートを進める際に注意を引くために、文書のメモとは異なる形式で意図的にフォーマットされています。進める際には太字の斜体のメモを削除してください：

```markdown
***Note: 説明文***
```

## 最後に、テンプレートの開始

***Note: 複数の推奨される選択肢がある場合、イントロで読者に伝えてください。たとえば、S3 からのロードの場合、同期ロードと非同期ロードのオプションがあります：***

StarRocks は S3 からデータをロードするための 2 つのオプションを提供します：

1. Broker Load を使用した非同期ロード
2. `FILES()` テーブル関数を使用した同期ロード

***Note: なぜ一方を選ぶのか読者に伝えてください：***

小さなデータセットはしばしば `FILES()` テーブル関数を使用して同期的にロードされ、大きなデータセットは Broker Load を使用して非同期的にロードされます。2 つの方法には異なる利点があり、以下で説明されています。

> **NOTE**
>
> StarRocks テーブルにデータをロードするには、INSERT 権限を持つユーザーとしてのみ可能です。INSERT 権限がない場合は、[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) に従って、StarRocks クラスターに接続するために使用するユーザーに INSERT 権限を付与してください。

## Broker Load の使用

非同期の Broker Load プロセスは、S3 への接続を確立し、データを取得し、StarRocks にデータを保存します。

### Broker Load の利点

- Broker Load は、ロード中にデータ変換、UPSERT、および DELETE 操作をサポートします。
- Broker Load はバックグラウンドで実行され、クライアントはジョブが続行するために接続を維持する必要がありません。
- Broker Load は長時間実行されるジョブに推奨され、デフォルトのタイムアウトは 4 時間です。
- Parquet および ORC ファイル形式に加えて、Broker Load は CSV ファイルをサポートします。

### データフロー

***Note: 複数のコンポーネントやステップを含むプロセスは、図を使うことで理解しやすくなるかもしれません。この例では、ユーザーが Broker Load オプションを選択したときに発生するステップを説明するのに役立つ図が含まれています。***

![Broker Load のワークフロー](../_assets/broker_load_how-to-work_en.png)

1. ユーザーがロードジョブを作成します。
2. フロントエンド (FE) がクエリプランを作成し、そのプランをバックエンドノード (BE) に配布します。
3. バックエンド (BE) ノードがソースからデータを取得し、StarRocks にデータをロードします。

### 典型的な例

テーブルを作成し、S3 から Parquet ファイルを取得するロードプロセスを開始し、データロードの進捗と成功を確認します。

> **NOTE**
>
> 例では Parquet 形式のサンプルデータセットを使用しています。CSV または ORC ファイルをロードしたい場合、その情報はこのページの下部にリンクされています。

#### テーブルの作成

テーブル用のデータベースを作成します：

```SQL
CREATE DATABASE IF NOT EXISTS project;
USE project;
```

テーブルを作成します。このスキーマは StarRocks アカウントにホストされている S3 バケット内のサンプルデータセットに一致します。

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

S3 からデータをロードするには、以下が必要です：

- S3 バケット
- バケット内の特定のオブジェクトにアクセスする場合の S3 オブジェクトキー（オブジェクト名）。オブジェクトキーには、S3 オブジェクトがサブフォルダに保存されている場合、プレフィックスを含めることができます。完全な構文は**詳細情報**にリンクされています。
- S3 リージョン
- アクセスキーとシークレット

#### Broker Load の開始

このジョブには 4 つの主要なセクションがあります：

- `LABEL`: `LOAD` ジョブの状態をクエリする際に使用される文字列。
- `LOAD` 宣言: ソース URI、宛先テーブル、およびソースデータ形式。
- `BROKER`: ソースの接続詳細。
- `PROPERTIES`: タイムアウト値およびこのジョブに適用するその他のプロパティ。

> **NOTE**
>
> これらの例で使用されているデータセットは、StarRocks アカウントの S3 バケットにホストされています。任意の有効な `aws.s3.access_key` および `aws.s3.secret_key` を使用できます。オブジェクトは任意の AWS 認証済みユーザーが読み取れるためです。以下のコマンドで `AAA` と `BBB` にあなたの資格情報を置き換えてください。

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

#### 進捗の確認

`information_schema.loads` テーブルをクエリして進捗を追跡します。複数の `LOAD` ジョブが実行中の場合、ジョブに関連付けられた `LABEL` でフィルタリングできます。以下の出力には、ロードジョブ `user_behavior` の 2 つのエントリがあります。最初のレコードは `CANCELLED` の状態を示しています。出力の最後までスクロールすると、`listPath failed` と表示されます。2 番目のレコードは、有効な AWS IAM アクセスキーとシークレットで成功を示しています。

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

`FILES()` は Parquet データの列のデータ型を推測し、StarRocks テーブルのスキーマを生成できます。これにより、S3 から直接ファイルを `SELECT` でクエリするか、Parquet ファイルのスキーマに基づいて StarRocks が自動的にテーブルを作成することができます。

> **NOTE**
>
> スキーマ推測はバージョン 3.1 の新機能であり、Parquet 形式のみで提供され、ネストされた型はまだサポートされていません。

### 典型的な例

`FILES()` テーブル関数を使用した 3 つの例があります：

- S3 から直接データをクエリする
- スキーマ推測を使用してテーブルを作成しロードする
- 手動でテーブルを作成し、その後データをロードする

> **NOTE**
>
> これらの例で使用されているデータセットは、StarRocks アカウントの S3 バケットにホストされています。任意の有効な `aws.s3.access_key` および `aws.s3.secret_key` を使用できます。オブジェクトは任意の AWS 認証済みユーザーが読み取れるためです。以下のコマンドで `AAA` と `BBB` にあなたの資格情報を置き換えてください。

#### S3 から直接クエリする

`FILES()` を使用して S3 から直接クエリすることで、テーブルを作成する前にデータセットの内容をプレビューできます。例えば：

- データを保存せずにデータセットをプレビューする。
- 最小値と最大値をクエリして、使用するデータ型を決定する。
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
> 列名は Parquet ファイルによって提供されることに注意してください。

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

#### スキーマ推測を使用してテーブルを作成する

これは前の例の続きです。前のクエリは `CREATE TABLE` にラップされ、スキーマ推測を使用してテーブル作成を自動化します。Parquet ファイルを使用する場合、列名と型は必要なく、Parquet 形式には列名と型が含まれているため、StarRocks がスキーマを推測します。

> **NOTE**
>
> スキーマ推測を使用する場合の `CREATE TABLE` の構文では、レプリカの数を設定できないため、テーブルを作成する前に設定してください。以下の例は、単一のレプリカを持つシステム用です：
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

#### 既存のテーブルにロードする

挿入するテーブルをカスタマイズしたい場合があります。例えば：

- 列のデータ型、NULL 許可設定、またはデフォルト値
- キーの種類と列
- 分散
- など

> **NOTE**
>
> 最も効率的なテーブル構造を作成するには、データの使用方法と列の内容に関する知識が必要です。この文書ではテーブル設計についてはカバーしていませんが、ページの最後に**詳細情報**へのリンクがあります。

この例では、テーブルがクエリされる方法と Parquet ファイル内のデータに関する知識に基づいてテーブルを作成しています。Parquet ファイル内のデータに関する知識は、S3 でファイルを直接クエリすることで得られます。

- S3 でのファイルクエリにより、`Timestamp` 列が `datetime` データ型に一致するデータを含むことが示されているため、以下の DDL で列タイプが指定されています。
- S3 でのデータクエリにより、データセットに NULL 値がないことがわかるため、DDL では列を NULL 許可に設定していません。
- 予想されるクエリタイプに基づいて、ソートキーとバケット化列は `UserID` 列に設定されています（このデータに対するユースケースによっては、`ItemID` をソートキーとして使用することもできます）：

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

テーブルを作成した後、`INSERT INTO` … `SELECT FROM FILES()` を使用してロードできます：

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

- 同期および非同期データロードの詳細については、[ロードの概念](./loading_introduction/loading_concepts.md) を参照してください。
- Broker Load がロード中にデータ変換をサポートする方法については、[ロード中のデータ変換](../loading/Etl_in_loading.md) および [プライマリキー テーブルへのロードによるデータの変更](../loading/Load_to_Primary_Key_tables.md) を参照してください。
- この文書では IAM ユーザー認証のみをカバーしています。他のオプションについては、[AWS リソースへの認証](../integrations/authenticate_to_aws_resources.md) を参照してください。
- [AWS CLI コマンドリファレンス](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html) では、S3 URI を詳細に説明しています。
- [テーブル設計](../table_design/StarRocks_table_design.md) についてさらに学びましょう。
- Broker Load は、上記の例よりも多くの構成および使用オプションを提供しており、詳細は [Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) に記載されています。