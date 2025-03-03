---
displayed_sidebar: docs
---

# Spark コネクタを使用して StarRocks からデータを読み取る

StarRocks は、Apache Spark™ 用に自社開発したコネクタである StarRocks Connector for Apache Spark™（以下、Spark コネクタ）を提供しており、Spark を使用して StarRocks テーブルからデータを読み取ることができます。Spark を使用して、StarRocks から読み取ったデータに対して複雑な処理や機械学習を行うことができます。

Spark コネクタは、Spark SQL、Spark DataFrame、Spark RDD の3つの読み取り方法をサポートしています。

Spark SQL を使用して StarRocks テーブルに一時ビューを作成し、その一時ビューを使用して StarRocks テーブルから直接データを読み取ることができます。

また、StarRocks テーブルを Spark DataFrame または Spark RDD にマッピングし、そこからデータを読み取ることもできます。Spark DataFrame の使用を推奨します。

> **注意**
>
> StarRocks テーブルの SELECT 権限を持つユーザーのみが、このテーブルからデータを読み取ることができます。ユーザーに権限を付与するには、[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) の指示に従ってください。

## 使用上の注意

- データを読み取る前に StarRocks でデータをフィルタリングすることで、転送されるデータ量を削減できます。
- データ読み取りのオーバーヘッドが大きい場合は、適切なテーブル設計とフィルタ条件を使用して、Spark が一度に過剰なデータを読み取らないようにすることができます。これにより、ディスクとネットワーク接続への I/O 負荷を軽減し、通常のクエリが適切に実行できるようにします。

## バージョン要件

| Spark コネクタ | Spark         | StarRocks       | Java | Scala |
|---------------- | ------------- | --------------- | ---- | ----- |
| 1.1.2           | 3.2, 3.3, 3.4, 3.5 | 2.5 以降       | 8    | 2.12  |
| 1.1.1           | 3.2, 3.3, 3.4 | 2.5 以降       | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, 3.4 | 2.5 以降       | 8    | 2.12  |
| 1.0.0           | 3.x           | 1.18 以降      | 8    | 2.12  |
| 1.0.0           | 2.x           | 1.18 以降      | 8    | 2.11  |

> **注意**
>
> - 異なるコネクタバージョン間の動作変更については、[Upgrade Spark connector](#upgrade-spark-connector) を参照してください。
> - バージョン 1.1.1 以降、コネクタは MySQL JDBC ドライバを提供していないため、Spark のクラスパスに手動でドライバをインポートする必要があります。ドライバは [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/) で見つけることができます。
> - バージョン 1.0.0 では、Spark コネクタは StarRocks からのデータ読み取りのみをサポートしています。バージョン 1.1.0 以降、Spark コネクタは StarRocks からのデータ読み取りと書き込みの両方をサポートしています。
> - バージョン 1.0.0 とバージョン 1.1.0 では、パラメータとデータ型マッピングが異なります。[Upgrade Spark connector](#upgrade-spark-connector) を参照してください。
> - 一般的な場合、バージョン 1.0.0 に新しい機能は追加されません。できるだけ早く Spark コネクタをアップグレードすることをお勧めします。

## Spark コネクタの入手

ビジネスニーズに合った Spark コネクタ **.jar** パッケージを入手するには、次の方法のいずれかを使用します。

- コンパイル済みパッケージをダウンロードする。
- Maven を使用して Spark コネクタに必要な依存関係を追加する。（この方法は Spark コネクタ 1.1.0 以降でのみサポートされています。）
- 手動でパッケージをコンパイルする。

### Spark コネクタ 1.1.0 以降

Spark コネクタ **.jar** パッケージは次の形式で命名されています。

`starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

たとえば、Spark 3.2 と Scala 2.12 で Spark コネクタ 1.1.0 を使用したい場合、`starrocks-spark-connector-3.2_2.12-1.1.0.jar` を選択できます。

> **注意**
>
> 通常、最新の Spark コネクタバージョンは、最新の 3 つの Spark バージョンで使用できます。

#### コンパイル済みパッケージをダウンロードする

さまざまなバージョンの Spark コネクタ **.jar** パッケージは [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) で入手できます。

#### Maven 依存関係を追加する

Spark コネクタに必要な依存関係を次のように設定します。

> **注意**
>
> `spark_version`、`scala_version`、および `connector_version` を使用する Spark バージョン、Scala バージョン、および Spark コネクタバージョンに置き換える必要があります。

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-${spark_version}_${scala_version}</artifactId>
  <version>${connector_version}</version>
</dependency>
```

たとえば、Spark 3.2 と Scala 2.12 で Spark コネクタ 1.1.0 を使用したい場合、依存関係を次のように設定します。

```xml
<dependency>
  <groupId>com.starrocks</groupId>
  <artifactId>starrocks-spark-connector-3.2_2.12</artifactId>
  <version>1.1.0</version>
</dependency>
```

#### 手動でパッケージをコンパイルする

1. [Spark コネクタのコード](https://github.com/StarRocks/starrocks-connector-for-apache-spark) をダウンロードします。

2. 次のコマンドを使用して Spark コネクタをコンパイルします。

   > **注意**
   >
   > `spark_version` を使用する Spark バージョンに置き換える必要があります。

   ```shell
   sh build.sh <spark_version>
   ```

   たとえば、Spark 3.2 で Spark コネクタを使用したい場合、次のように Spark コネクタをコンパイルします。

   ```shell
   sh build.sh 3.2
   ```

3. `target/` パスに移動し、コンパイル後に生成された `starrocks-spark-connector-3.2_2.12-1.1.0-SNAPSHOT.jar` のような Spark コネクタ **.jar** パッケージを確認します。

   > **注意**
   >
   > 公式にリリースされていない Spark コネクタバージョンを使用している場合、生成された Spark コネクタ **.jar** パッケージの名前には `SNAPSHOT` がサフィックスとして含まれます。

### Spark コネクタ 1.0.0

#### コンパイル済みパッケージをダウンロードする

- [Spark 2.x](https://cdn-thirdparty.starrocks.com/spark/starrocks-spark2_2.11-1.0.0.jar)
- [Spark 3.x](https://cdn-thirdparty.starrocks.com/spark/starrocks-spark3_2.12-1.0.0.jar)

#### 手動でパッケージをコンパイルする

1. [Spark コネクタのコード](https://github.com/StarRocks/starrocks-connector-for-apache-spark/tree/spark-1.0) をダウンロードします。

   > **注意**
   >
   > `spark-1.0` に切り替える必要があります。

2. Spark コネクタをコンパイルするために、次のいずれかの操作を行います。

   - Spark 2.x を使用している場合、次のコマンドを実行します。デフォルトで Spark 2.3.4 に適合するように Spark コネクタをコンパイルします。

     ```Plain
     sh build.sh 2
     ```

   - Spark 3.x を使用している場合、次のコマンドを実行します。デフォルトで Spark 3.1.2 に適合するように Spark コネクタをコンパイルします。

     ```Plain
     sh build.sh 3
     ```

3. `output/` パスに移動し、コンパイル後に生成された `starrocks-spark2_2.11-1.0.0.jar` ファイルを確認します。その後、ファイルを Spark のクラスパスにコピーします。

   - Spark クラスターが `Local` モードで実行されている場合、ファイルを `jars/` パスに配置します。
   - Spark クラスターが `Yarn` モードで実行されている場合、ファイルを事前展開パッケージに配置します。

指定された場所にファイルを配置した後にのみ、Spark コネクタを使用して StarRocks からデータを読み取ることができます。

## パラメータ

このセクションでは、Spark コネクタを使用して StarRocks からデータを読み取る際に設定する必要があるパラメータについて説明します。

### 共通パラメータ

次のパラメータは、Spark SQL、Spark DataFrame、Spark RDD の3つの読み取り方法すべてに適用されます。

| パラメータ                            | デフォルト値     | 説明                                                    |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | None              | StarRocks クラスター内の FE の HTTP URL。形式 `<fe_host>:<fe_http_port>`。複数の URL を指定する場合は、カンマ (,) で区切る必要があります。 |
| starrocks.table.identifier           | None              | StarRocks テーブルの名前。形式: `<database_name>.<table_name>`。 |
| starrocks.request.retries            | 3                 | Spark が StarRocks に読み取りリクエストを送信する際の最大リトライ回数。 |
| starrocks.request.connect.timeout.ms | 30000             | StarRocks に送信された読み取りリクエストがタイムアウトするまでの最大時間。 |
| starrocks.request.read.timeout.ms    | 30000             | StarRocks に送信されたリクエストの読み取りがタイムアウトするまでの最大時間。 |
| starrocks.request.query.timeout.s    | 3600              | StarRocks からデータをクエリする際の最大タイムアウト時間。デフォルトのタイムアウト期間は 1 時間です。`-1` はタイムアウト期間が指定されていないことを意味します。 |
| starrocks.request.tablet.size        | Integer.MAX_VALUE | 各 Spark RDD パーティションにグループ化される StarRocks タブレットの数。このパラメータの値が小さいほど、生成される Spark RDD パーティションの数が多くなります。Spark の並行性が高くなる一方で、StarRocks への負荷も大きくなります。 |
| starrocks.batch.size                 | 4096              | 一度に BEs から読み取ることができる最大行数。このパラメータの値を増やすことで、Spark と StarRocks 間で確立される接続数を減らし、ネットワーク遅延による余分な時間オーバーヘッドを軽減できます。 |
| starrocks.exec.mem.limit             | 2147483648        | クエリごとに許可される最大メモリ量。単位: バイト。デフォルトのメモリ制限は 2 GB です。 |
| starrocks.deserialize.arrow.async    | false             | Arrow メモリ形式を Spark コネクタの反復に必要な RowBatches に非同期で変換することをサポートするかどうかを指定します。 |
| starrocks.deserialize.queue.size     | 64                | Arrow メモリ形式を RowBatches に非同期で変換するタスクを保持する内部キューのサイズ。このパラメータは `starrocks.deserialize.arrow.async` が `true` に設定されている場合に有効です。 |
| starrocks.filter.query               | None              | StarRocks 上でデータをフィルタリングするための条件。複数のフィルタ条件を指定する場合は、`and` で結合する必要があります。StarRocks は指定されたフィルタ条件に基づいて StarRocks テーブルからデータをフィルタリングし、その後 Spark によってデータが読み取られます。 |
| starrocks.timezone | JVM のデフォルトタイムゾーン | 1.1.1 以降でサポートされています。StarRocks の `DATETIME` を Spark の `TimestampType` に変換するために使用されるタイムゾーン。デフォルトは `ZoneId#systemDefault()` によって返される JVM のタイムゾーンです。形式は `Asia/Shanghai` のようなタイムゾーン名、または `+08:00` のようなゾーンオフセットです。 |

### Spark SQL および Spark DataFrame 用のパラメータ

次のパラメータは、Spark SQL および Spark DataFrame の読み取り方法にのみ適用されます。

| パラメータ                           | デフォルト値 | 説明                                                    |
| ----------------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.fe.http.url               | None          | FE の HTTP IP アドレス。このパラメータは Spark コネクタ 1.1.0 以降でサポートされています。このパラメータは `starrocks.fenodes` と同等です。どちらか一方を設定するだけで済みます。Spark コネクタ 1.1.0 以降では、`starrocks.fe.http.url` を使用することをお勧めします。`starrocks.fenodes` は廃止される可能性があります。 |
| starrocks.fe.jdbc.url               | None          | FE の MySQL サーバーに接続するために使用されるアドレス。形式: `jdbc:mysql://<fe_host>:<fe_query_port>`。<br />**注意**<br />Spark コネクタ 1.1.0 以降では、このパラメータは必須です。   |
| user                                | None          | StarRocks クラスターアカウントのユーザー名。ユーザーは StarRocks テーブルに対する [SELECT 権限](../sql-reference/sql-statements/account-management/GRANT.md) を持っている必要があります。   |
| starrocks.user                      | None          | StarRocks クラスターアカウントのユーザー名。このパラメータは Spark コネクタ 1.1.0 以降でサポートされています。このパラメータは `user` と同等です。どちらか一方を設定するだけで済みます。Spark コネクタ 1.1.0 以降では、`starrocks.user` を使用することをお勧めします。`user` は廃止される可能性があります。   |
| password                            | None          | StarRocks クラスターアカウントのパスワード。    |
| starrocks.password                  | None          | StarRocks クラスターアカウントのパスワード。このパラメータは Spark コネクタ 1.1.0 以降でサポートされています。このパラメータは `password` と同等です。どちらか一方を設定するだけで済みます。Spark コネクタ 1.1.0 以降では、`starrocks.password` を使用することをお勧めします。`password` は廃止される可能性があります。   |
| starrocks.filter.query.in.max.count | 100           | プレディケートプッシュダウン中に IN 式でサポートされる最大値数。IN 式で指定された値の数がこの制限を超える場合、IN 式で指定されたフィルタ条件は Spark で処理されます。   |

### Spark RDD 用のパラメータ

次のパラメータは、Spark RDD の読み取り方法にのみ適用されます。

| パラメータ                       | デフォルト値 | 説明                                                    |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.request.auth.user     | None          | StarRocks クラスターアカウントのユーザー名。              |
| starrocks.request.auth.password | None          | StarRocks クラスターアカウントのパスワード。              |
| starrocks.read.field            | None          | データを読み取りたい StarRocks テーブルのカラム。複数のカラムを指定する場合は、カンマ (,) で区切る必要があります。 |

## StarRocks と Spark 間のデータ型マッピング

### Spark コネクタ 1.1.0 以降

| StarRocks データ型 | Spark データ型           |
|-------------------- |-------------------------- |
| BOOLEAN             | DataTypes.BooleanType     |
| TINYINT             | DataTypes.ByteType        |
| SMALLINT            | DataTypes.ShortType       |
| INT                 | DataTypes.IntegerType     |
| BIGINT              | DataTypes.LongType        |
| LARGEINT            | DataTypes.StringType      |
| FLOAT               | DataTypes.FloatType       |
| DOUBLE              | DataTypes.DoubleType      |
| DECIMAL             | DecimalType               |
| CHAR                | DataTypes.StringType      |
| VARCHAR             | DataTypes.StringType      |
| STRING              | DataTypes.StringType      |
| DATE                | DataTypes.DateType        |
| DATETIME            | DataTypes.TimestampType   |
| JSON | DataTypes.StringType <br /> **注意:** <br /> このデータ型マッピングは Spark コネクタ v1.1.2 以降でサポートされ、StarRocks バージョン 2.5.13、3.0.3、3.1.0 以降が必要です。 |
| ARRAY               | サポートされていないデータ型      |
| HLL                 | サポートされていないデータ型      |
| BITMAP              | サポートされていないデータ型      |

### Spark コネクタ 1.0.0

| StarRocks データ型  | Spark データ型        |
| -------------------- | ---------------------- |
| BOOLEAN              | DataTypes.BooleanType  |
| TINYINT              | DataTypes.ByteType     |
| SMALLINT             | DataTypes.ShortType    |
| INT                  | DataTypes.IntegerType  |
| BIGINT               | DataTypes.LongType     |
| LARGEINT             | DataTypes.StringType   |
| FLOAT                | DataTypes.FloatType    |
| DOUBLE               | DataTypes.DoubleType   |
| DECIMAL              | DecimalType            |
| CHAR                 | DataTypes.StringType   |
| VARCHAR              | DataTypes.StringType   |
| DATE                 | DataTypes.StringType   |
| DATETIME             | DataTypes.StringType   |
| ARRAY                | サポートされていないデータ型   |
| HLL                  | サポートされていないデータ型   |
| BITMAP               | サポートされていないデータ型   |

StarRocks が使用する基盤ストレージエンジンの処理ロジックは、DATE および DATETIME データ型を直接使用する場合に期待される時間範囲をカバーできません。そのため、Spark コネクタは StarRocks の DATE および DATETIME データ型を Spark の STRING データ型にマッピングし、StarRocks から読み取った日付と時刻データに一致する読みやすい文字列テキストを生成します。

## Spark コネクタのアップグレード

### バージョン 1.0.0 からバージョン 1.1.0 へのアップグレード

- バージョン 1.1.1 以降、Spark コネクタは MySQL の公式 JDBC ドライバである `mysql-connector-java` を提供していません。これは `mysql-connector-java` が使用する GPL ライセンスの制限によるものです。しかし、Spark コネクタはテーブルメタデータに接続するために `mysql-connector-java` を必要とするため、ドライバを Spark のクラスパスに手動で追加する必要があります。ドライバは [MySQL サイト](https://dev.mysql.com/downloads/connector/j/) または [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/) で見つけることができます。
  
- バージョン 1.1.0 では、Spark コネクタは StarRocks にアクセスしてより詳細なテーブル情報を取得するために JDBC を使用します。そのため、`starrocks.fe.jdbc.url` を設定する必要があります。

- バージョン 1.1.0 では、一部のパラメータがリネームされています。現在、古いパラメータと新しいパラメータの両方が保持されています。等価なパラメータのペアのうち、どちらか一方を設定するだけで済みますが、古いパラメータは廃止される可能性があるため、新しいパラメータを使用することをお勧めします。
  - `starrocks.fenodes` は `starrocks.fe.http.url` にリネームされました。
  - `user` は `starrocks.user` にリネームされました。
  - `password` は `starrocks.password` にリネームされました。

- バージョン 1.1.0 では、Spark 3.x に基づいて一部のデータ型のマッピングが調整されています。
  - StarRocks の `DATE` は Spark の `DataTypes.DateType`（元は `DataTypes.StringType`）にマッピングされます。
  - StarRocks の `DATETIME` は Spark の `DataTypes.TimestampType`（元は `DataTypes.StringType`）にマッピングされます。

## 例

次の例では、StarRocks クラスターに `test` という名前のデータベースを作成し、ユーザー `root` の権限を持っていると仮定します。例のパラメータ設定は Spark コネクタ 1.1.0 に基づいています。

### ネットワーク設定

Spark が配置されているマシンが、StarRocks クラスターの FE ノードに [`http_port`](../administration/management/FE_configuration.md#http_port)（デフォルト: `8030`）および [`query_port`](../administration/management/FE_configuration.md#query_port)（デフォルト: `9030`）を介してアクセスでき、BE ノードに [`be_port`](../administration/management/BE_configuration.md#be_port)（デフォルト: `9060`）を介してアクセスできることを確認してください。

### データ例

サンプルテーブルを準備するには、次の手順を実行します。

1. `test` データベースに移動し、`score_board` という名前のテーブルを作成します。

   ```SQL
   MySQL [test]> CREATE TABLE `score_board`
   (
       `id` int(11) NOT NULL COMMENT "",
       `name` varchar(65533) NULL DEFAULT "" COMMENT "",
       `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`id`)
   PROPERTIES (
       "replication_num" = "3"
   );
   ```

2. `score_board` テーブルにデータを挿入します。

   ```SQL
   MySQL [test]> INSERT INTO score_board
   VALUES
       (1, 'Bob', 21),
       (2, 'Stan', 21),
       (3, 'Sam', 22),
       (4, 'Tony', 22),
       (5, 'Alice', 22),
       (6, 'Lucy', 23),
       (7, 'Polly', 23),
       (8, 'Tom', 23),
       (9, 'Rose', 24),
       (10, 'Jerry', 24),
       (11, 'Jason', 24),
       (12, 'Lily', 25),
       (13, 'Stephen', 25),
       (14, 'David', 25),
       (15, 'Eddie', 26),
       (16, 'Kate', 27),
       (17, 'Cathy', 27),
       (18, 'Judy', 27),
       (19, 'Julia', 28),
       (20, 'Robert', 28),
       (21, 'Jack', 29);
   ```

3. `score_board` テーブルをクエリします。

   ```SQL
   MySQL [test]> SELECT * FROM score_board;
   +------+---------+-------+
   | id   | name    | score |
   +------+---------+-------+
   |    1 | Bob     |    21 |
   |    2 | Stan    |    21 |
   |    3 | Sam     |    22 |
   |    4 | Tony    |    22 |
   |    5 | Alice   |    22 |
   |    6 | Lucy    |    23 |
   |    7 | Polly   |    23 |
   |    8 | Tom     |    23 |
   |    9 | Rose    |    24 |
   |   10 | Jerry   |    24 |
   |   11 | Jason   |    24 |
   |   12 | Lily    |    25 |
   |   13 | Stephen |    25 |
   |   14 | David   |    25 |
   |   15 | Eddie   |    26 |
   |   16 | Kate    |    27 |
   |   17 | Cathy   |    27 |
   |   18 | Judy    |    27 |
   |   19 | Julia   |    28 |
   |   20 | Robert  |    28 |
   |   21 | Jack    |    29 |
   +------+---------+-------+
   21 rows in set (0.01 sec)
   ```

### Spark SQL を使用してデータを読み取る

1. Spark ディレクトリで次のコマンドを実行して Spark SQL を開始します。

   ```Plain
   sh spark-sql
   ```

2. `test` データベースに属する `score_board` テーブルに一時ビュー `spark_starrocks` を作成するために、次のコマンドを実行します。

   ```SQL
   spark-sql> CREATE TEMPORARY VIEW spark_starrocks
              USING starrocks
              OPTIONS
              (
                  "starrocks.table.identifier" = "test.score_board",
                  "starrocks.fe.http.url" = "<fe_host>:<fe_http_port>",
                  "starrocks.fe.jdbc.url" = "jdbc:mysql://<fe_host>:<fe_query_port>",
                  "starrocks.user" = "root",
                  "starrocks.password" = ""
              );
   ```

3. 一時ビューからデータを読み取るために、次のコマンドを実行します。

   ```SQL
   spark-sql> SELECT * FROM spark_starrocks;
   ```

   Spark は次のデータを返します。

   ```SQL
   1        Bob        21
   2        Stan        21
   3        Sam        22
   4        Tony        22
   5        Alice        22
   6        Lucy        23
   7        Polly        23
   8        Tom        23
   9        Rose        24
   10        Jerry        24
   11        Jason        24
   12        Lily        25
   13        Stephen        25
   14        David        25
   15        Eddie        26
   16        Kate        27
   17        Cathy        27
   18        Judy        27
   19        Julia        28
   20        Robert        28
   21        Jack        29
   Time taken: 1.883 seconds, Fetched 21 row(s)
   22/08/09 15:29:36 INFO thriftserver.SparkSQLCLIDriver: Time taken: 1.883 seconds, Fetched 21 row(s)
   ```

### Spark DataFrame を使用してデータを読み取る

1. Spark ディレクトリで次のコマンドを実行して Spark Shell を開始します。

   ```Plain
   sh spark-shell
   ```

2. `test` データベースに属する `score_board` テーブルに DataFrame `starrocksSparkDF` を作成するために、次のコマンドを実行します。

   ```Scala
   scala> val starrocksSparkDF = spark.read.format("starrocks")
              .option("starrocks.table.identifier", s"test.score_board")
              .option("starrocks.fe.http.url", s"<fe_host>:<fe_http_port>")
              .option("starrocks.fe.jdbc.url", s"jdbc:mysql://<fe_host>:<fe_query_port>")
              .option("starrocks.user", s"root")
              .option("starrocks.password", s"")
              .load()
   ```

3. DataFrame からデータを読み取ります。たとえば、最初の 10 行を読み取りたい場合、次のコマンドを実行します。

   ```Scala
   scala> starrocksSparkDF.show(10)
   ```

   Spark は次のデータを返します。

   ```Scala
   +---+-----+-----+
   | id| name|score|
   +---+-----+-----+
   |  1|  Bob|   21|
   |  2| Stan|   21|
   |  3|  Sam|   22|
   |  4| Tony|   22|
   |  5|Alice|   22|
   |  6| Lucy|   23|
   |  7|Polly|   23|
   |  8|  Tom|   23|
   |  9| Rose|   24|
   | 10|Jerry|   24|
   +---+-----+-----+
   only showing top 10 rows
   ```

   > **注意**
   >
   > デフォルトでは、読み取りたい行数を指定しない場合、Spark は最初の 20 行を返します。

### Spark RDD を使用してデータを読み取る

1. Spark ディレクトリで次のコマンドを実行して Spark Shell を開始します。

   ```Plain
   sh spark-shell
   ```

2. `test` データベースに属する `score_board` テーブルに RDD `starrocksSparkRDD` を作成するために、次のコマンドを実行します。

   ```Scala
   scala> import com.starrocks.connector.spark._
   scala> val starrocksSparkRDD = sc.starrocksRDD
              (
              tableIdentifier = Some("test.score_board"),
              cfg = Some(Map(
                  "starrocks.fenodes" -> "<fe_host>:<fe_http_port>",
                  "starrocks.request.auth.user" -> "root",
                  "starrocks.request.auth.password" -> ""
              ))
              )
   ```

3. RDD からデータを読み取ります。たとえば、最初の 10 要素を読み取りたい場合、次のコマンドを実行します。

   ```Scala
   scala> starrocksSparkRDD.take(10)
   ```

   Spark は次のデータを返します。

   ```Scala
   res0: Array[AnyRef] = Array([1, Bob, 21], [2, Stan, 21], [3, Sam, 22], [4, Tony, 22], [5, Alice, 22], [6, Lucy, 23], [7, Polly, 23], [8, Tom, 23], [9, Rose, 24], [10, Jerry, 24])
   ```

   全体の RDD を読み取るには、次のコマンドを実行します。

   ```Scala
   scala> starrocksSparkRDD.collect()
   ```

   Spark は次のデータを返します。

   ```Scala
   res1: Array[AnyRef] = Array([1, Bob, 21], [2, Stan, 21], [3, Sam, 22], [4, Tony, 22], [5, Alice, 22], [6, Lucy, 23], [7, Polly, 23], [8, Tom, 23], [9, Rose, 24], [10, Jerry, 24], [11, Jason, 24], [12, Lily, 25], [13, Stephen, 25], [14, David, 25], [15, Eddie, 26], [16, Kate, 27], [17, Cathy, 27], [18, Judy, 27], [19, Julia, 28], [20, Robert, 28], [21, Jack, 29])
   ```

## ベストプラクティス

Spark コネクタを使用して StarRocks からデータを読み取る際に、`starrocks.filter.query` パラメータを使用してフィルタ条件を指定することで、Spark がパーティション、バケット、プレフィックスインデックスをプルーニングし、データ取得のコストを削減することができます。このセクションでは、Spark DataFrame を例にとって、これがどのように達成されるかを示します。

### 環境設定

| コンポーネント       | バージョン                                                      |
| --------------- | ------------------------------------------------------------ |
| Spark           | Spark 2.4.4 および Scala 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_302) |
| StarRocks       | 2.2.0                                                        |
| Spark コネクタ | starrocks-spark2_2.11-1.0.0.jar                              |

### データ例

サンプルテーブルを準備するには、次の手順を実行します。

1. `test` データベースに移動し、`mytable` という名前のテーブルを作成します。

   ```SQL
   MySQL [test]> CREATE TABLE `mytable`
   (
       `k` int(11) NULL COMMENT "bucket",
       `b` int(11) NULL COMMENT "",
       `dt` datetime NULL COMMENT "",
       `v` int(11) NULL COMMENT ""
   )
   ENGINE=OLAP
   DUPLICATE KEY(`k`,`b`, `dt`)
   COMMENT "OLAP"
   PARTITION BY RANGE(`dt`)
   (
       PARTITION p202201 VALUES [('2022-01-01 00:00:00'), ('2022-02-01 00:00:00')),
       PARTITION p202202 VALUES [('2022-02-01 00:00:00'), ('2022-03-01 00:00:00')),
       PARTITION p202203 VALUES [('2022-03-01 00:00:00'), ('2022-04-01 00:00:00'))
   )
   DISTRIBUTED BY HASH(`k`)
   PROPERTIES (
       "replication_num" = "3"
   );
   ```

2. `mytable` にデータを挿入します。

   ```SQL
   MySQL [test]> INSERT INTO mytable
   VALUES
        (1, 11, '2022-01-02 08:00:00', 111),
        (2, 22, '2022-02-02 08:00:00', 222),
        (3, 33, '2022-03-02 08:00:00', 333);
   ```

3. `mytable` テーブルをクエリします。

   ```SQL
   MySQL [test]> select * from mytable;
   +------+------+---------------------+------+
   | k    | b    | dt                  | v    |
   +------+------+---------------------+------+
   |    1 |   11 | 2022-01-02 08:00:00 |  111 |
   |    2 |   22 | 2022-02-02 08:00:00 |  222 |
   |    3 |   33 | 2022-03-02 08:00:00 |  333 |
   +------+------+---------------------+------+
   3 rows in set (0.01 sec)
   ```

### フルテーブルスキャン

1. Spark ディレクトリで次のコマンドを実行して、`test` データベースに属する `mytable` テーブルに DataFrame `df` を作成します。

   ```Scala
   scala>  val df = spark.read.format("starrocks")
           .option("starrocks.table.identifier", s"test.mytable")
           .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
           .option("user", s"root")
           .option("password", s"")
           .load()
   ```

2. StarRocks クラスターの FE ログファイル **fe.log** を確認し、データを読み取るために実行された SQL 文を見つけます。例：

   ```SQL
   2022-08-09 18:57:38,091 INFO (nioEventLoopGroup-3-10|196) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable`] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. `test` データベースで、SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` 文の実行計画を取得するために EXPLAIN を使用します。

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable`;
   +-----------------------------------------------------------------------+
   | Explain String                                                        |
   +-----------------------------------------------------------------------+
   | PLAN FRAGMENT 0                                                       |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v                              |
   |   PARTITION: UNPARTITIONED                                            |
   |                                                                       |
   |   RESULT SINK                                                         |
   |                                                                       |
   |   1:EXCHANGE                                                          |
   |                                                                       |
   | PLAN FRAGMENT 1                                                       |
   |  OUTPUT EXPRS:                                                        |
   |   PARTITION: RANDOM                                                   |
   |                                                                       |
   |   STREAM DATA SINK                                                    |
   |     EXCHANGE ID: 01                                                   |
   |     UNPARTITIONED                                                     |
   |                                                                       |
   |   0:OlapScanNode                                                      |
   |      TABLE: mytable                                                   |
   |      PREAGGREGATION: ON                                               |
   |      partitions=3/3                                                   |
   |      rollup: mytable                                                  |
   |      tabletRatio=9/9                                                  |
   |      tabletList=41297,41299,41301,41303,41305,41307,41309,41311,41313 |
   |      cardinality=3                                                    |
   |      avgRowSize=4.0                                                   |
   |      numNodes=0                                                       |
   +-----------------------------------------------------------------------+
   26 rows in set (0.00 sec)
   ```

この例では、プルーニングは行われていません。そのため、Spark はデータを保持する3つのパーティションすべて（`partitions=3/3` と示されているように）をスキャンし、それらの3つのパーティション内の9つのタブレットすべて（`tabletRatio=9/9` と示されているように）をスキャンします。

### パーティションプルーニング

1. Spark ディレクトリで次のコマンドを実行し、`starrocks.filter.query` パラメータを使用してパーティションプルーニングのためのフィルタ条件 `dt='2022-01-02 08:00:00'` を指定し、`test` データベースに属する `mytable` テーブルに DataFrame `df` を作成します。

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "dt='2022-01-02 08:00:00'")
          .load()
   ```

2. StarRocks クラスターの FE ログファイル **fe.log** を確認し、データを読み取るために実行された SQL 文を見つけます。例：

   ```SQL
   2022-08-09 19:02:31,253 INFO (nioEventLoopGroup-3-14|204) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00'] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. `test` データベースで、SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00' 文の実行計画を取得するために EXPLAIN を使用します。

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable` where dt='2022-01-02 08:00:00';
   +------------------------------------------------+
   | Explain String                                 |
   +------------------------------------------------+
   | PLAN FRAGMENT 0                                |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v       |
   |   PARTITION: UNPARTITIONED                     |
   |                                                |
   |   RESULT SINK                                  |
   |                                                |
   |   1:EXCHANGE                                   |
   |                                                |
   | PLAN FRAGMENT 1                                |
   |  OUTPUT EXPRS:                                 |
   |   PARTITION: RANDOM                            |
   |                                                |
   |   STREAM DATA SINK                             |
   |     EXCHANGE ID: 01                            |
   |     UNPARTITIONED                              |
   |                                                |
   |   0:OlapScanNode                               |
   |      TABLE: mytable                            |
   |      PREAGGREGATION: ON                        |
   |      PREDICATES: 3: dt = '2022-01-02 08:00:00' |
   |      partitions=1/3                            |
   |      rollup: mytable                           |
   |      tabletRatio=3/3                           |
   |      tabletList=41297,41299,41301              |
   |      cardinality=1                             |
   |      avgRowSize=20.0                           |
   |      numNodes=0                                |
   +------------------------------------------------+
   27 rows in set (0.01 sec)
   ```

この例では、パーティションプルーニングのみが行われ、バケットプルーニングは行われていません。そのため、Spark は3つのパーティションのうち1つ（`partitions=1/3` と示されているように）をスキャンし、そのパーティション内のすべてのタブレット（`tabletRatio=3/3` と示されているように）をスキャンします。

### バケットプルーニング

1. Spark ディレクトリで次のコマンドを実行し、`starrocks.filter.query` パラメータを使用してバケットプルーニングのためのフィルタ条件 `k=1` を指定し、`test` データベースに属する `mytable` テーブルに DataFrame `df` を作成します。

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "k=1")
          .load()
   ```

2. StarRocks クラスターの FE ログファイル **fe.log** を確認し、データを読み取るために実行された SQL 文を見つけます。例：

   ```SQL
   2022-08-09 19:04:44,479 INFO (nioEventLoopGroup-3-16|208) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1] from external service [ user ['root'@'%']] for database [test] table [mytable]
   ```

3. `test` データベースで、SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1 文の実行計画を取得するために EXPLAIN を使用します。

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=1;
   +------------------------------------------+
   | Explain String                           |
   +------------------------------------------+
   | PLAN FRAGMENT 0                          |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v |
   |   PARTITION: UNPARTITIONED               |
   |                                          |
   |   RESULT SINK                            |
   |                                          |
   |   1:EXCHANGE                             |
   |                                          |
   | PLAN FRAGMENT 1                          |
   |  OUTPUT EXPRS:                           |
   |   PARTITION: RANDOM                      |
   |                                          |
   |   STREAM DATA SINK                       |
   |     EXCHANGE ID: 01                      |
   |     UNPARTITIONED                        |
   |                                          |
   |   0:OlapScanNode                         |
   |      TABLE: mytable                      |
   |      PREAGGREGATION: ON                  |
   |      PREDICATES: 1: k = 1                |
   |      partitions=3/3                      |
   |      rollup: mytable                     |
   |      tabletRatio=3/9                     |
   |      tabletList=41299,41305,41311        |
   |      cardinality=1                       |
   |      avgRowSize=20.0                     |
   |      numNodes=0                          |
   +------------------------------------------+
   27 rows in set (0.01 sec)
   ```

この例では、バケットプルーニングのみが行われ、パーティションプルーニングは行われていません。そのため、Spark はデータを保持する3つのパーティションすべて（`partitions=3/3` と示されているように）をスキャンし、3つのパーティション内の3つのタブレット（`tabletRatio=3/9` と示されているように）をスキャンして、`k = 1` フィルタ条件を満たすハッシュ値を取得します。

### パーティションプルーニングとバケットプルーニング

1. Spark ディレクトリで次のコマンドを実行し、`starrocks.filter.query` パラメータを使用してバケットプルーニングとパーティションプルーニングのための2つのフィルタ条件 `k=7` および `dt='2022-01-02 08:00:00'` を指定し、`test` データベースに属する `mytable` テーブルに DataFrame `df` を作成します。

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"")
          .option("password", s"")
          .option("starrocks.filter.query", "k=7 and dt='2022-01-02 08:00:00'")
          .load()
   ```

2. StarRocks クラスターの FE ログファイル **fe.log** を確認し、データを読み取るために実行された SQL 文を見つけます。例：

   ```SQL
   2022-08-09 19:06:34,939 INFO (nioEventLoopGroup-3-18|212) [TableQueryPlanAction.executeWithoutPassword():126] receive SQL statement [select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00'] from external service [ user ['root'@'%']] for database [test] t
   able [mytable]
   ```

3. `test` データベースで、SELECT `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00' 文の実行計画を取得するために EXPLAIN を使用します。

   ```Scala
   MySQL [test]> EXPLAIN select `k`,`b`,`dt`,`v` from `test`.`mytable` where k=7 and dt='2022-01-02 08:00:00';
   +----------------------------------------------------------+
   | Explain String                                           |
   +----------------------------------------------------------+
   | PLAN FRAGMENT 0                                          |
   |  OUTPUT EXPRS:1: k | 2: b | 3: dt | 4: v                 |
   |   PARTITION: RANDOM                                      |
   |                                                          |
   |   RESULT SINK                                            |
   |                                                          |
   |   0:OlapScanNode                                         |
   |      TABLE: mytable                                      |
   |      PREAGGREGATION: ON                                  |
   |      PREDICATES: 1: k = 7, 3: dt = '2022-01-02 08:00:00' |
   |      partitions=1/3                                      |
   |      rollup: mytable                                     |
   |      tabletRatio=1/3                                     |
   |      tabletList=41301                                    |
   |      cardinality=1                                       |
   |      avgRowSize=20.0                                     |
   |      numNodes=0                                          |
   +----------------------------------------------------------+
   17 rows in set (0.00 sec)
   ```

この例では、パーティションプルーニングとバケットプルーニングの両方が行われています。そのため、Spark は3つのパーティションのうち1つ（`partitions=1/3` と示されているように）をスキャンし、そのパーティション内の1つのタブレット（`tabletRatio=1/3` と示されているように）をスキャンします。

### プレフィックスインデックスフィルタリング

1. `test` データベースに属する `mytable` テーブルのパーティションにさらにデータレコードを挿入します。

```Scala
   MySQL [test]> INSERT INTO mytable
   VALUES
       (1, 11, "2022-01-02 08:00:00", 111), 
       (3, 33, "2022-01-02 08:00:00", 333), 
       (3, 33, "2022-01-02 08:00:00", 333), 
       (3, 33, "2022-01-02 08:00:00", 333);
   ```

2. `mytable` テーブルをクエリします。

   ```Scala
   MySQL [test]> SELECT * FROM mytable;
   +------+------+---------------------+------+
   | k    | b    | dt                  | v    |
   +------+------+---------------------+------+
   |    1 |   11 | 2022-01-02 08:00:00 |  111 |
   |    1 |   11 | 2022-01-02 08:00:00 |  111 |
   |    3 |   33 | 2022-01-02 08:00:00 |  333 |
   |    3 |   33 | 2022-01-02 08:00:00 |  333 |
   |    3 |   33 | 2022-01-02 08:00:00 |  333 |
   |    2 |   22 | 2022-02-02 08:00:00 |  222 |
   |    3 |   33 | 2022-03-02 08:00:00 |  333 |
   +------+------+---------------------+------+
   7 rows in set (0.01 sec)
   ```

3. Spark ディレクトリで次のコマンドを実行し、`starrocks.filter.query` パラメータを使用してプレフィックスインデックスフィルタリングのためのフィルタ条件 `k=1` を指定し、`test` データベースに属する `mytable` テーブルに DataFrame `df` を作成します。

   ```Scala
   scala> val df = spark.read.format("starrocks")
          .option("starrocks.table.identifier", s"test.mytable")
          .option("starrocks.fenodes", s"<fe_host>:<fe_http_port>")
          .option("user", s"root")
          .option("password", s"")
          .option("starrocks.filter.query", "k=1")
          .load()
   ```

4. `test` データベースで、プロファイル報告を有効にするために `is_report_success` を `true` に設定します。

   ```SQL
   MySQL [test]> SET is_report_success = true;
   Query OK, 0 rows affected (0.00 sec)
   ```

5. ブラウザを使用して `http://<fe_host>:<http_http_port>/query` ページを開き、SELECT * FROM mytable where k=1 文のプロファイルを確認します。例：

   ```SQL
   OLAP_SCAN (plan_node_id=0):
     CommonMetrics:
        - CloseTime: 1.255ms
        - OperatorTotalTime: 1.404ms
        - PeakMemoryUsage: 0.00 
        - PullChunkNum: 8
        - PullRowNum: 2
          - __MAX_OF_PullRowNum: 2
          - __MIN_OF_PullRowNum: 0
        - PullTotalTime: 148.60us
        - PushChunkNum: 0
        - PushRowNum: 0
        - PushTotalTime: 0ns
        - SetFinishedTime: 136ns
        - SetFinishingTime: 129ns
     UniqueMetrics:
        - Predicates: 1: k = 1
        - Rollup: mytable
        - Table: mytable
        - BytesRead: 88.00 B
          - __MAX_OF_BytesRead: 88.00 B
          - __MIN_OF_BytesRead: 0.00 
        - CachedPagesNum: 0
        - CompressedBytesRead: 844.00 B
          - __MAX_OF_CompressedBytesRead: 844.00 B
          - __MIN_OF_CompressedBytesRead: 0.00 
        - CreateSegmentIter: 18.582us
        - IOTime: 4.425us
        - LateMaterialize: 17.385us
        - PushdownPredicates: 3
        - RawRowsRead: 2
          - __MAX_OF_RawRowsRead: 2
          - __MIN_OF_RawRowsRead: 0
        - ReadPagesNum: 12
          - __MAX_OF_ReadPagesNum: 12
          - __MIN_OF_ReadPagesNum: 0
        - RowsRead: 2
          - __MAX_OF_RowsRead: 2
          - __MIN_OF_RowsRead: 0
        - ScanTime: 154.367us
        - SegmentInit: 95.903us
          - BitmapIndexFilter: 0ns
          - BitmapIndexFilterRows: 0
          - BloomFilterFilterRows: 0
          - ShortKeyFilterRows: 3
            - __MAX_OF_ShortKeyFilterRows: 3
            - __MIN_OF_ShortKeyFilterRows: 0
          - ZoneMapIndexFilterRows: 0
        - SegmentRead: 2.559us
          - BlockFetch: 2.187us
          - BlockFetchCount: 2
            - __MAX_OF_BlockFetchCount: 2
            - __MIN_OF_BlockFetchCount: 0
          - BlockSeek: 7.789us
          - BlockSeekCount: 2
            - __MAX_OF_BlockSeekCount: 2
            - __MIN_OF_BlockSeekCount: 0
          - ChunkCopy: 25ns
          - DecompressT: 0ns
          - DelVecFilterRows: 0
          - IndexLoad: 0ns
          - PredFilter: 353ns
          - PredFilterRows: 0
          - RowsetsReadCount: 7
          - SegmentsReadCount: 3
            - __MAX_OF_SegmentsReadCount: 2
            - __MIN_OF_SegmentsReadCount: 0
          - TotalColumnsDataPageCount: 8
            - __MAX_OF_TotalColumnsDataPageCount: 8
            - __MIN_OF_TotalColumnsDataPageCount: 0
        - UncompressedBytesRead: 508.00 B
          - __MAX_OF_UncompressedBytesRead: 508.00 B
          - __MIN_OF_UncompressedBytesRead: 0.00 
   ```

この例では、フィルタ条件 `k = 1` がプレフィックスインデックスにヒットすることができます。そのため、Spark は3行（`ShortKeyFilterRows: 3` と示されているように）をフィルタリングできます。
```