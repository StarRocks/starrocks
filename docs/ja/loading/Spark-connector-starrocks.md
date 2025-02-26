---
displayed_sidebar: docs
---

# Spark コネクタを使用してデータをロードする（推奨）

StarRocks は、Apache Spark™ 用に開発したコネクタである StarRocks Connector for Apache Spark™（以下、Spark コネクタ）を提供しています。これを使用して、Spark を介して StarRocks テーブルにデータをロードできます。基本的な原理は、データを蓄積し、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を通じて一度に StarRocks にロードすることです。Spark コネクタは Spark DataSource V2 に基づいて実装されています。DataSource は Spark DataFrames または Spark SQL を使用して作成できます。バッチモードと構造化ストリーミングモードの両方がサポートされています。

> **注意**
>
> StarRocks テーブルにデータをロードできるのは、SELECT および INSERT 権限を持つユーザーのみです。[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) の指示に従って、これらの権限をユーザーに付与できます。

## バージョン要件

| Spark コネクタ | Spark            | StarRocks     | Java | Scala |
| --------------- | ---------------- | ------------- | ---- | ----- |
| 1.1.2           | 3.2, 3.3, 3.4, 3.5 | 2.5 以降   | 8    | 2.12  |
| 1.1.1           | 3.2, 3.3, または 3.4 | 2.5 以降 | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, または 3.4 | 2.5 以降 | 8    | 2.12  |

> **注意**
>
> - Spark コネクタの異なるバージョン間の動作の変更については、[Upgrade Spark connector](#upgrade-spark-connector) を参照してください。
> - Spark コネクタはバージョン 1.1.1 以降、MySQL JDBC ドライバを提供していません。ドライバを手動で Spark クラスパスにインポートする必要があります。ドライバは [MySQL サイト](https://dev.mysql.com/downloads/connector/j/) または [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/) で見つけることができます。

## Spark コネクタの取得

Spark コネクタの JAR ファイルは以下の方法で取得できます：

- コンパイル済みの Spark コネクタ JAR ファイルを直接ダウンロードする。
- Maven プロジェクトに Spark コネクタを依存関係として追加し、JAR ファイルをダウンロードする。
- Spark コネクタのソースコードを自分でコンパイルして JAR ファイルを作成する。

Spark コネクタ JAR ファイルの命名形式は `starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar` です。

例えば、環境に Spark 3.2 と Scala 2.12 をインストールしており、Spark コネクタ 1.1.0 を使用したい場合、`starrocks-spark-connector-3.2_2.12-1.1.0.jar` を使用できます。

> **注意**
>
> 一般的に、最新バージョンの Spark コネクタは Spark の最新の 3 つのバージョンとのみ互換性を維持します。

### コンパイル済みの Jar ファイルをダウンロード

[Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から対応するバージョンの Spark コネクタ JAR を直接ダウンロードします。

### Maven 依存関係

1. Maven プロジェクトの `pom.xml` ファイルに、以下の形式で Spark コネクタを依存関係として追加します。`spark_version`、`scala_version`、`connector_version` をそれぞれのバージョンに置き換えてください。

    ```xml
    <dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-spark-connector-${spark_version}_${scala_version}</artifactId>
    <version>${connector_version}</version>
    </dependency>
    ```

2. 例えば、環境の Spark バージョンが 3.2、Scala バージョンが 2.12 で、Spark コネクタ 1.1.0 を選択した場合、以下の依存関係を追加する必要があります：

    ```xml
    <dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-spark-connector-3.2_2.12</artifactId>
    <version>1.1.0</version>
    </dependency>
    ```

### 自分でコンパイル

1. [Spark コネクタパッケージ](https://github.com/StarRocks/starrocks-connector-for-apache-spark) をダウンロードします。
2. Spark コネクタのソースコードを JAR ファイルにコンパイルするために、以下のコマンドを実行します。`spark_version` は対応する Spark バージョンに置き換えてください。

      ```bash
      sh build.sh <spark_version>
      ```

   例えば、環境の Spark バージョンが 3.2 の場合、以下のコマンドを実行する必要があります：

      ```bash
      sh build.sh 3.2
      ```

3. `target/` ディレクトリに移動し、コンパイル時に生成された Spark コネクタ JAR ファイル（例：`starrocks-spark-connector-3.2_2.12-1.1.0-SNAPSHOT.jar`）を見つけます。

> **注意**
>
> 正式にリリースされていない Spark コネクタの名前には `SNAPSHOT` サフィックスが含まれています。

## パラメータ

### starrocks.fe.http.url

**必須**:  YES<br/>
**デフォルト値**:  なし<br/>
**説明**:  StarRocks クラスター内の FE の HTTP URL。複数の URL を指定することができ、カンマ（,）で区切る必要があります。形式: `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>`。バージョン 1.1.1 以降、URL に `http://` プレフィックスを追加することもできます。例：`http://<fe_host1>:<fe_http_port1>,http://<fe_host2>:<fe_http_port2>`。

### starrocks.fe.jdbc.url

**必須**:  YES<br/>
**デフォルト値**:  なし<br/>
**説明**:  FE の MySQL サーバーに接続するために使用されるアドレス。形式: `jdbc:mysql://<fe_host>:<fe_query_port>`。

### starrocks.table.identifier

**必須**:  YES<br/>
**デフォルト値**:  なし<br/>
**説明**:  StarRocks テーブルの名前。形式: `<database_name>.<table_name>`。

### starrocks.user

**必須**:  YES<br/>
**デフォルト値**:  なし<br/>
**説明**:  StarRocks クラスターアカウントのユーザー名。ユーザーは StarRocks テーブルに対する [SELECT および INSERT 権限](../sql-reference/sql-statements/account-management/GRANT.md) を持っている必要があります。

### starrocks.password

**必須**:  YES<br/>
**デフォルト値**:  なし<br/>
**説明**:  StarRocks クラスターアカウントのパスワード。

### starrocks.write.label.prefix

**必須**:  NO<br/>
**デフォルト値**:  spark-<br/>
**説明**:  Stream Load で使用されるラベルプレフィックス。

### starrocks.write.enable.transaction-stream-load

**必須**:  NO<br/>
**デフォルト値**:  TRUE<br/>
**説明**:  [Stream Load トランザクションインターフェース](../loading/Stream_Load_transaction_interface.md) を使用してデータをロードするかどうか。StarRocks v2.5 以降が必要です。この機能は、トランザクション内でより多くのデータを少ないメモリ使用量でロードし、パフォーマンスを向上させます。<br/> **注意:** バージョン 1.1.1 以降、このパラメータは `starrocks.write.max.retries` の値が非正の場合にのみ有効です。なぜなら、Stream Load トランザクションインターフェースはリトライをサポートしていないためです。

### starrocks.write.buffer.size

**必須**:  NO<br/>
**デフォルト値**:  104857600<br/>
**説明**:  一度に StarRocks に送信される前にメモリ内に蓄積できるデータの最大サイズ。このパラメータを大きな値に設定すると、ロードパフォーマンスが向上しますが、ロードの遅延が増加する可能性があります。

### starrocks.write.buffer.rows

**必須**:  NO<br/>
**デフォルト値**:  Integer.MAX_VALUE<br/>
**説明**:  バージョン 1.1.1 以降でサポートされています。一度に StarRocks に送信される前にメモリ内に蓄積できる行の最大数。

### starrocks.write.flush.interval.ms

**必須**:  NO<br/>
**デフォルト値**:  300000<br/>
**説明**:  データが StarRocks に送信される間隔。このパラメータはロードの遅延を制御するために使用されます。

### starrocks.write.max.retries

**必須**:  NO<br/>
**デフォルト値**:  3<br/>
**説明**:  バージョン 1.1.1 以降でサポートされています。ロードが失敗した場合に同じバッチのデータに対して Stream Load を再試行する回数。<br/> **注意:** Stream Load トランザクションインターフェースはリトライをサポートしていないため、このパラメータが正の場合、コネクタは常に Stream Load インターフェースを使用し、`starrocks.write.enable.transaction-stream-load` の値を無視します。

### starrocks.write.retry.interval.ms

**必須**:  NO<br/>
**デフォルト値**:  10000<br/>
**説明**:  バージョン 1.1.1 以降でサポートされています。ロードが失敗した場合に同じバッチのデータに対して Stream Load を再試行する間隔。

### starrocks.columns

**必須**:  NO<br/>
**デフォルト値**:  なし<br/>
**説明**:  データをロードしたい StarRocks テーブルの列。複数の列を指定することができ、カンマ（,）で区切る必要があります。例：`"col0,col1,col2"`。

### starrocks.column.types

**必須**: NO<br/>
**デフォルト値**:  なし<br/>
**説明**:  バージョン 1.1.1 以降でサポートされています。StarRocks テーブルから推測されるデフォルトのデータ型と[デフォルトマッピング](#data-type-mapping-between-spark-and-starrocks)を使用する代わりに、Spark 用の列データ型をカスタマイズします。パラメータ値は Spark の [StructType#toDDL](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala#L449) の出力と同じ DDL 形式のスキーマです。例：`col0 INT, col1 STRING, col2 BIGINT`。カスタマイズが必要な列のみを指定する必要があります。使用例として、[BITMAP](#load-data-into-columns-of-bitmap-type) または [HLL](#load-data-into-columns-of-hll-type) 型の列にデータをロードすることが挙げられます。

### starrocks.write.properties.*

**必須**:  NO<br/>
**デフォルト値**:  なし<br/>
**説明**:  Stream Load の動作を制御するために使用されるパラメータ。例えば、パラメータ `starrocks.write.properties.format` はロードされるデータの形式を指定します。サポートされているパラメータとその説明のリストについては、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### starrocks.write.properties.format

**必須**:  NO<br/>
**デフォルト値**:  CSV<br/>
**説明**:  Spark コネクタがデータを StarRocks に送信する前に各バッチのデータを変換する際のファイル形式。有効な値：CSV および JSON。

### starrocks.write.properties.row_delimiter

**必須**:  NO<br/>
**デフォルト値**:  \n<br/>
**説明**:  CSV 形式のデータの行区切り文字。

### starrocks.write.properties.column_separator

**必須**:  NO<br/>
**デフォルト値**:  \t<br/>
**説明**:  CSV 形式のデータの列区切り文字。

### starrocks.write.properties.partial_update

**必須**:  NO<br/>
**デフォルト値**: `FALSE`<br/>
**説明**: 部分更新を使用するかどうか。有効な値：`TRUE` および `FALSE`。デフォルト値：`FALSE`、この機能を無効にすることを示します。

### starrocks.write.properties.partial_update_mode

**必須**:  NO<br/>
**デフォルト値**: `row`<br/>
**説明**: 部分更新のモードを指定します。有効な値：`row` および `column`。<ul><li>値 `row`（デフォルト）は行モードでの部分更新を意味し、多くの列と小さなバッチでのリアルタイム更新に適しています。</li><li>値 `column` は列モードでの部分更新を意味し、少ない列と多くの行でのバッチ更新に適しています。このようなシナリオでは、列モードを有効にすると更新速度が速くなります。例えば、100 列のテーブルで、すべての行に対して 10 列（全体の 10%）のみが更新される場合、列モードの更新速度は 10 倍速くなります。</li></ul>

### starrocks.write.num.partitions

**必須**:  NO<br/>
**デフォルト値**:  なし<br/>
**説明**:  Spark がデータを書き込む際に並行して使用できるパーティションの数。データ量が少ない場合、パーティションの数を減らしてロードの同時実行性と頻度を下げることができます。このパラメータのデフォルト値は Spark によって決定されます。ただし、この方法は Spark Shuffle コストを引き起こす可能性があります。

### starrocks.write.partition.columns

**必須**:  NO<br/>
**デフォルト値**:  なし<br/>
**説明**:  Spark のパーティション列。このパラメータは `starrocks.write.num.partitions` が指定されている場合にのみ有効です。このパラメータが指定されていない場合、書き込まれるすべての列がパーティションに使用されます。

### starrocks.timezone

**必須**:  NO<br/>
**デフォルト値**:  JVM のデフォルトタイムゾーン<br/>
**説明**:  バージョン 1.1.1 以降でサポートされています。Spark の `TimestampType` を StarRocks の `DATETIME` に変換する際に使用されるタイムゾーン。デフォルトは `ZoneId#systemDefault()` によって返される JVM のタイムゾーンです。形式は `Asia/Shanghai` のようなタイムゾーン名、または `+08:00` のようなゾーンオフセットで指定できます。

## Spark と StarRocks のデータ型マッピング

- デフォルトのデータ型マッピングは以下の通りです：

  | Spark データ型 | StarRocks データ型                                          |
  | --------------- | ------------------------------------------------------------ |
  | BooleanType     | BOOLEAN                                                      |
  | ByteType        | TINYINT                                                      |
  | ShortType       | SMALLINT                                                     |
  | IntegerType     | INT                                                          |
  | LongType        | BIGINT                                                       |
  | StringType      | LARGEINT                                                     |
  | FloatType       | FLOAT                                                        |
  | DoubleType      | DOUBLE                                                       |
  | DecimalType     | DECIMAL                                                      |
  | StringType      | CHAR                                                         |
  | StringType      | VARCHAR                                                      |
  | StringType      | STRING                                                       |
  | StringType      | JSON                                                       |
  | DateType        | DATE                                                         |
  | TimestampType   | DATETIME                                                     |
  | ArrayType       | ARRAY <br /> **注意:** <br /> **バージョン 1.1.1 以降でサポートされています**。詳細な手順については、[Load data into columns of ARRAY type](#load-data-into-columns-of-array-type) を参照してください。 |

- データ型マッピングをカスタマイズすることもできます。

  例えば、StarRocks テーブルに BITMAP および HLL 列が含まれているが、Spark はこれらのデータ型をサポートしていません。Spark で対応するデータ型をカスタマイズする必要があります。詳細な手順については、[BITMAP](#load-data-into-columns-of-bitmap-type) および [HLL](#load-data-into-columns-of-hll-type) 列にデータをロードする方法を参照してください。**BITMAP および HLL はバージョン 1.1.1 以降でサポートされています**。

## Spark コネクタのアップグレード

### バージョン 1.1.0 から 1.1.1 へのアップグレード

- バージョン 1.1.1 以降、Spark コネクタは MySQL の公式 JDBC ドライバである `mysql-connector-java` を提供していません。これは `mysql-connector-java` が使用する GPL ライセンスの制限によるものです。しかし、Spark コネクタは StarRocks のテーブルメタデータに接続するために MySQL JDBC ドライバを必要とするため、ドライバを手動で Spark クラスパスに追加する必要があります。ドライバは [MySQL サイト](https://dev.mysql.com/downloads/connector/j/) または [Maven Central](https://repo1.maven.org/maven2/mysql/mysql-connector-java/) で見つけることができます。
- バージョン 1.1.1 以降、コネクタはデフォルトで Stream Load インターフェースを使用し、バージョン 1.1.0 の Stream Load トランザクションインターフェースではありません。Stream Load トランザクションインターフェースを引き続き使用したい場合は、オプション `starrocks.write.max.retries` を `0` に設定できます。詳細については、`starrocks.write.enable.transaction-stream-load` および `starrocks.write.max.retries` の説明を参照してください。

## 例

以下の例は、Spark DataFrames または Spark SQL を使用して Spark コネクタで StarRocks テーブルにデータをロードする方法を示しています。Spark DataFrames はバッチモードと構造化ストリーミングモードの両方をサポートしています。

詳細な例については、[Spark Connector Examples](https://github.com/StarRocks/starrocks-connector-for-apache-spark/tree/main/src/test/java/com/starrocks/connector/spark/examples) を参照してください。

### 準備

#### StarRocks テーブルを作成

データベース `test` を作成し、主キーテーブル `score_board` を作成します。

```sql
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`);
```

#### ネットワーク設定

Spark が配置されているマシンが、StarRocks クラスターの FE ノードに [`http_port`](../administration/management/FE_configuration.md#http_port)（デフォルト: `8030`）および [`query_port`](../administration/management/FE_configuration.md#query_port)（デフォルト: `9030`）を介してアクセスでき、BE ノードに [`be_http_port`](../administration/management/BE_configuration.md#be_http_port)（デフォルト: `8040`）を介してアクセスできることを確認します。

#### Spark 環境のセットアップ

以下の例は Spark 3.2.4 で実行され、`spark-shell`、`pyspark`、および `spark-sql` を使用します。例を実行する前に、Spark コネクタ JAR ファイルを `$SPARK_HOME/jars` ディレクトリに配置してください。

### Spark DataFrames でデータをロード

以下の 2 つの例は、Spark DataFrames バッチまたは構造化ストリーミングモードでデータをロードする方法を説明しています。

#### バッチ

メモリ内でデータを構築し、StarRocks テーブルにデータをロードします。

1. Scala または Python を使用して Spark アプリケーションを書くことができます。

  Scala の場合、`spark-shell` で以下のコードスニペットを実行します：

  ```Scala
  // 1. シーケンスから DataFrame を作成します。
  val data = Seq((1, "starrocks", 100), (2, "spark", 100))
  val df = data.toDF("id", "name", "score")

  // 2. フォーマットを "starrocks" として設定し、以下のオプションを設定して StarRocks に書き込みます。
  // 自分の環境に応じてオプションを変更する必要があります。
  df.write.format("starrocks")
      .option("starrocks.fe.http.url", "127.0.0.1:8030")
      .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
      .option("starrocks.table.identifier", "test.score_board")
      .option("starrocks.user", "root")
      .option("starrocks.password", "")
      .mode("append")
      .save()
  ```

  Python の場合、`pyspark` で以下のコードスニペットを実行します：

   ```python
   from pyspark.sql import SparkSession
   
   spark = SparkSession \
        .builder \
        .appName("StarRocks Example") \
        .getOrCreate()
   
    # 1. シーケンスから DataFrame を作成します。
    data = [(1, "starrocks", 100), (2, "spark", 100)]
    df = spark.sparkContext.parallelize(data) \
            .toDF(["id", "name", "score"])

    # 2. フォーマットを "starrocks" として設定し、以下のオプションを設定して StarRocks に書き込みます。
    # 自分の環境に応じてオプションを変更する必要があります。
    df.write.format("starrocks") \
        .option("starrocks.fe.http.url", "127.0.0.1:8030") \
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030") \
        .option("starrocks.table.identifier", "test.score_board") \
        .option("starrocks.user", "root") \
        .option("starrocks.password", "") \
        .mode("append") \
        .save()
    ```

2. StarRocks テーブルのデータをクエリします。

    ```sql
    MySQL [test]> SELECT * FROM `score_board`;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | spark     |   100 |
    +------+-----------+-------+
    2 rows in set (0.00 sec)
    ```

#### 構造化ストリーミング

CSV ファイルからデータのストリーミング読み取りを構築し、StarRocks テーブルにデータをロードします。

1. ディレクトリ `csv-data` に、以下のデータを含む CSV ファイル `test.csv` を作成します：

    ```csv
    3,starrocks,100
    4,spark,100
    ```

2. Scala または Python を使用して Spark アプリケーションを書くことができます。

  Scala の場合、`spark-shell` で以下のコードスニペットを実行します：

    ```Scala
    import org.apache.spark.sql.types.StructType

    // 1. CSV から DataFrame を作成します。
    val schema = (new StructType()
            .add("id", "integer")
            .add("name", "string")
            .add("score", "integer")
        )
    val df = (spark.readStream
            .option("sep", ",")
            .schema(schema)
            .format("csv") 
            // ディレクトリ "csv-data" へのパスに置き換えてください。
            .load("/path/to/csv-data")
        )
    
    // 2. フォーマットを "starrocks" として設定し、以下のオプションを設定して StarRocks に書き込みます。
    // 自分の環境に応じてオプションを変更する必要があります。
    val query = (df.writeStream.format("starrocks")
            .option("starrocks.fe.http.url", "127.0.0.1:8030")
            .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
            .option("starrocks.table.identifier", "test.score_board")
            .option("starrocks.user", "root")
            .option("starrocks.password", "")
            // チェックポイントディレクトリに置き換えてください
            .option("checkpointLocation", "/path/to/checkpoint")
            .outputMode("append")
            .start()
        )
    ```

  Python の場合、`pyspark` で以下のコードスニペットを実行します：
   
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.types import IntegerType, StringType, StructType, StructField
   
   spark = SparkSession \
        .builder \
        .appName("StarRocks SS Example") \
        .getOrCreate()
   
    # 1. CSV から DataFrame を作成します。
    schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", IntegerType())
        ])
    df = (
        spark.readStream
        .option("sep", ",")
        .schema(schema)
        .format("csv")
        # ディレクトリ "csv-data" へのパスに置き換えてください。
        .load("/path/to/csv-data")
    )

    # 2. フォーマットを "starrocks" として設定し、以下のオプションを設定して StarRocks に書き込みます。
    # 自分の環境に応じてオプションを変更する必要があります。
    query = (
        df.writeStream.format("starrocks")
        .option("starrocks.fe.http.url", "127.0.0.1:8030")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
        .option("starrocks.table.identifier", "test.score_board")
        .option("starrocks.user", "root")
        .option("starrocks.password", "")
        # チェックポイントディレクトリに置き換えてください
        .option("checkpointLocation", "/path/to/checkpoint")
        .outputMode("append")
        .start()
    )
    ```

3. StarRocks テーブルのデータをクエリします。

    ```SQL
    MySQL [test]> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    4 | spark     |   100 |
    |    3 | starrocks |   100 |
    +------+-----------+-------+
    2 rows in set (0.67 sec)
    ```

### Spark SQL でデータをロード

以下の例は、[Spark SQL CLI](https://spark.apache.org/docs/latest/sql-distributed-sql-engine-spark-sql-cli.html) の `INSERT INTO` ステートメントを使用して Spark SQL でデータをロードする方法を説明しています。

1. `spark-sql` で以下の SQL ステートメントを実行します：

    ```SQL
    -- 1. データソースを `starrocks` として設定し、以下のオプションを設定してテーブルを作成します。
    -- 自分の環境に応じてオプションを変更する必要があります。
    CREATE TABLE `score_board`
    USING starrocks
    OPTIONS(
    "starrocks.fe.http.url"="127.0.0.1:8030",
    "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
    "starrocks.table.identifier"="test.score_board",
    "starrocks.user"="root",
    "starrocks.password"=""
    );

    -- 2. テーブルに 2 行を挿入します。
    INSERT INTO `score_board` VALUES (5, "starrocks", 100), (6, "spark", 100);
    ```

2. StarRocks テーブルのデータをクエリします。

    ```SQL
    MySQL [test]> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    6 | spark     |   100 |
    |    5 | starrocks |   100 |
    +------+-----------+-------+
    2 rows in set (0.00 sec)
    ```

## ベストプラクティス

### 主キーテーブルにデータをロードする

このセクションでは、StarRocks 主キーテーブルにデータをロードして部分更新や条件付き更新を実現する方法を示します。これらの機能の詳細な紹介については、[Change data through loading](../loading/Load_to_Primary_Key_tables.md) を参照してください。これらの例では Spark SQL を使用します。

#### 準備

StarRocks でデータベース `test` を作成し、主キーテーブル `score_board` を作成します。

```SQL
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`);
```

#### 部分更新

この例では、ロードを通じて `name` 列のデータのみを更新する方法を示します：

1. MySQL クライアントで StarRocks テーブルに初期データを挿入します。

   ```sql
   mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'spark', 100);

   mysql> select * from score_board;
   +------+-----------+-------+
   | id   | name      | score |
   +------+-----------+-------+
   |    1 | starrocks |   100 |
   |    2 | spark     |   100 |
   +------+-----------+-------+
   2 rows in set (0.02 sec)
   ```

2. Spark SQL クライアントで Spark テーブル `score_board` を作成します。

   - コネクタに部分更新を行うことを伝えるために、オプション `starrocks.write.properties.partial_update` を `true` に設定します。
   - コネクタに書き込む列を伝えるために、オプション `starrocks.columns` を `"id,name"` に設定します。

   ```SQL
   CREATE TABLE `score_board`
   USING starrocks
   OPTIONS(
       "starrocks.fe.http.url"="127.0.0.1:8030",
       "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
       "starrocks.table.identifier"="test.score_board",
       "starrocks.user"="root",
       "starrocks.password"="",
       "starrocks.write.properties.partial_update"="true",
       "starrocks.columns"="id,name"
    );
   ```

3. Spark SQL クライアントでテーブルにデータを挿入し、`name` 列のみを更新します。

   ```SQL
   INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'spark-update');
   ```

4. MySQL クライアントで StarRocks テーブルをクエリします。

   `name` の値のみが変更され、`score` の値は変更されていないことがわかります。

   ```SQL
   mysql> select * from score_board;
   +------+------------------+-------+
   | id   | name             | score |
   +------+------------------+-------+
   |    1 | starrocks-update |   100 |
   |    2 | spark-update     |   100 |
   +------+------------------+-------+
   2 rows in set (0.02 sec)
   ```

#### 条件付き更新

この例では、`score` 列の値に基づいて条件付き更新を行う方法を示します。`id` の更新は、新しい `score` の値が古い値以上の場合にのみ有効になります。

1. MySQL クライアントで StarRocks テーブルに初期データを挿入します。

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'spark', 100);

    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | spark     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Spark テーブル `score_board` を以下の方法で作成します。

   - コネクタに `score` 列を条件として使用することを伝えるために、オプション `starrocks.write.properties.merge_condition` を `score` に設定します。
   - コネクタが Stream Load インターフェースを使用してデータをロードすることを確認します。Stream Load トランザクションインターフェースはこの機能をサポートしていません。

   ```SQL
   CREATE TABLE `score_board`
   USING starrocks
   OPTIONS(
       "starrocks.fe.http.url"="127.0.0.1:8030",
       "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
       "starrocks.table.identifier"="test.score_board",
       "starrocks.user"="root",
       "starrocks.password"="",
       "starrocks.write.properties.merge_condition"="score"
    );
   ```

3. Spark SQL クライアントでテーブルにデータを挿入し、`id` が 1 の行を小さい `score` 値で更新し、`id` が 2 の行を大きい `score` 値で更新します。

   ```SQL
   INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'spark-update', 101);
   ```

4. MySQL クライアントで StarRocks テーブルをクエリします。

   `id` が 2 の行のみが変更され、`id` が 1 の行は変更されていないことがわかります。

   ```SQL
   mysql> select * from score_board;
   +------+--------------+-------+
   | id   | name         | score |
   +------+--------------+-------+
   |    1 | starrocks    |   100 |
   |    2 | spark-update |   101 |
   +------+--------------+-------+
   2 rows in set (0.03 sec)
   ```

### BITMAP 型の列にデータをロード

[`BITMAP`](../sql-reference/data-types/other-data-types/BITMAP.md) は、UV のカウントのような count distinct を高速化するためによく使用されます。[Use Bitmap for exact Count Distinct](../using_starrocks/distinct_values/Using_bitmap.md) を参照してください。ここでは、UV のカウントを例に、`BITMAP` 型の列にデータをロードする方法を示します。**`BITMAP` はバージョン 1.1.1 以降でサポートされています**。

1. StarRocks 集計テーブルを作成します。

   データベース `test` に、`visit_users` 列が `BITMAP` 型として定義され、集計関数 `BITMAP_UNION` で設定された集計テーブル `page_uv` を作成します。

    ```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Spark テーブルを作成します。

    Spark テーブルのスキーマは StarRocks テーブルから推測され、Spark は `BITMAP` 型をサポートしていません。そのため、Spark で対応する列データ型をカスタマイズする必要があります。例えば、`BIGINT` として、オプション `"starrocks.column.types"="visit_users BIGINT"` を設定します。Stream Load を使用してデータを取り込む際、コネクタは [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) 関数を使用して `BIGINT` 型のデータを `BITMAP` 型に変換します。

    `spark-sql` で以下の DDL を実行します：

    ```SQL
    CREATE TABLE `page_uv`
    USING starrocks
    OPTIONS(
       "starrocks.fe.http.url"="127.0.0.1:8030",
       "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
       "starrocks.table.identifier"="test.page_uv",
       "starrocks.user"="root",
       "starrocks.password"="",
       "starrocks.column.types"="visit_users BIGINT"
    );
    ```

3. StarRocks テーブルにデータをロードします。

    `spark-sql` で以下の DML を実行します：

    ```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. StarRocks テーブルからページ UV を計算します。

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `page_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       2 |                           1 |
    |       1 |                           3 |
    +---------+-----------------------------+
    2 rows in set (0.01 sec)
    ```

> **注意:**
>
> コネクタは [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) 関数を使用して、Spark の `TINYINT`、`SMALLINT`、`INTEGER`、および `BIGINT` 型のデータを StarRocks の `BITMAP` 型に変換し、他の Spark データ型には [`bitmap_hash`](../sql-reference/sql-functions/bitmap-functions/bitmap_hash.md) 関数を使用します。

### HLL 型の列にデータをロード

[`HLL`](../sql-reference/data-types/other-data-types/HLL.md) は、近似 count distinct に使用できます。[Use HLL for approximate count distinct](../using_starrocks/distinct_values/Using_HLL.md) を参照してください。

ここでは、UV のカウントを例に、`HLL` 型の列にデータをロードする方法を示します。**`HLL` はバージョン 1.1.1 以降でサポートされています**。

1. StarRocks 集計テーブルを作成します。

   データベース `test` に、`visit_users` 列が `HLL` 型として定義され、集計関数 `HLL_UNION` で設定された集計テーブル `hll_uv` を作成します。

    ```SQL
    CREATE TABLE `hll_uv` (
    `page_id` INT NOT NULL COMMENT 'page ID',
    `visit_date` datetime NOT NULL COMMENT 'access time',
    `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Spark テーブルを作成します。

   Spark テーブルのスキーマは StarRocks テーブルから推測され、Spark は `HLL` 型をサポートしていません。そのため、Spark で対応する列データ型をカスタマイズする必要があります。例えば、`BIGINT` として、オプション `"starrocks.column.types"="visit_users BIGINT"` を設定します。Stream Load を使用してデータを取り込む際、コネクタは [`hll_hash`](../sql-reference/sql-functions/scalar-functions/hll_hash.md) 関数を使用して `BIGINT` 型のデータを `HLL` 型に変換します。

    `spark-sql` で以下の DDL を実行します：

    ```SQL
    CREATE TABLE `hll_uv`
    USING starrocks
    OPTIONS(
       "starrocks.fe.http.url"="127.0.0.1:8030",
       "starrocks.fe.jdbc.url"="jdbc:mysql://127.0.0.1:9030",
       "starrocks.table.identifier"="test.hll_uv",
       "starrocks.user"="root",
       "starrocks.password"="",
       "starrocks.column.types"="visit_users BIGINT"
    );
    ```

3. StarRocks テーブルにデータをロードします。

    `spark-sql` で以下の DML を実行します：

    ```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. StarRocks テーブルからページ UV を計算します。

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `hll_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       4 |                           1 |
    |       3 |                           2 |
    +---------+-----------------------------+
    2 rows in set (0.01 sec)
    ```

### ARRAY 型の列にデータをロード

以下の例は、[`ARRAY`](../sql-reference/data-types/semi_structured/Array.md) 型の列にデータをロードする方法を説明しています。

1. StarRocks テーブルを作成します。

   データベース `test` に、1 つの `INT` 列と 2 つの `ARRAY` 列を含む主キーテーブル `array_tbl` を作成します。

   ```SQL
   CREATE TABLE `array_tbl` (
       `id` INT NOT NULL,
       `a0` ARRAY<STRING>,
       `a1` ARRAY<ARRAY<INT>>
    )
    ENGINE=OLAP
    PRIMARY KEY(`id`)
    DISTRIBUTED BY HASH(`id`)
    ;
    ```

2. StarRocks にデータを書き込みます。

   一部のバージョンの StarRocks は `ARRAY` 列のメタデータを提供していないため、コネクタはこの列の対応する Spark データ型を推測できません。ただし、オプション `starrocks.column.types` で列の対応する Spark データ型を明示的に指定できます。この例では、オプションを `a0 ARRAY<STRING>,a1 ARRAY<ARRAY<INT>>` として設定できます。

   `spark-shell` で以下のコードを実行します：

   ```scala
    val data = Seq(
       |  (1, Seq("hello", "starrocks"), Seq(Seq(1, 2), Seq(3, 4))),
       |  (2, Seq("hello", "spark"), Seq(Seq(5, 6, 7), Seq(8, 9, 10)))
       | )
    val df = data.toDF("id", "a0", "a1")
    df.write
         .format("starrocks")
         .option("starrocks.fe.http.url", "127.0.0.1:8030")
         .option("starrocks.fe.jdbc.url", "jdbc:mysql://127.0.0.1:9030")
         .option("starrocks.table.identifier", "test.array_tbl")
         .option("starrocks.user", "root")
         .option("starrocks.password", "")
         .option("starrocks.column.types", "a0 ARRAY<STRING>,a1 ARRAY<ARRAY<INT>>")
         .mode("append")
         .save()
    ```

3. StarRocks テーブルのデータをクエリします。

   ```SQL
   MySQL [test]> SELECT * FROM `array_tbl`;
   +------+-----------------------+--------------------+
   | id   | a0                    | a1                 |
   +------+-----------------------+--------------------+
   |    1 | ["hello","starrocks"] | [[1,2],[3,4]]      |
   |    2 | ["hello","spark"]     | [[5,6,7],[8,9,10]] |
   +------+-----------------------+--------------------+
   2 rows in set (0.01 sec)
   ```