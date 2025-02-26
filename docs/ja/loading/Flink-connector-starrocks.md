---
displayed_sidebar: docs
---

# Apache Flink® からデータを継続的にロードする

StarRocks は、Apache Flink® 用の StarRocks Connector（以下、Flink コネクタ）という独自開発のコネクタを提供しており、Flink を使用して StarRocks テーブルにデータをロードするのに役立ちます。基本的な原理は、データを蓄積し、それを一度に StarRocks に [STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を通じてロードすることです。

Flink コネクタは DataStream API、Table API & SQL、Python API をサポートしています。これは、Apache Flink® が提供する [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/) よりも高い安定したパフォーマンスを持っています。

> **注意**
>
> Flink コネクタを使用して StarRocks テーブルにデータをロードするには、対象の StarRocks テーブルに対する SELECT および INSERT 権限が必要です。これらの権限がない場合は、[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) に従って、StarRocks クラスターに接続するために使用するユーザーにこれらの権限を付与してください。

## バージョン要件

| コネクタ | Flink                    | StarRocks     | Java | Scala     |
|-----------|--------------------------|---------------| ---- |-----------|
| 1.2.10    | 1.15,1.16,1.17,1.18,1.19 | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.9     | 1.15,1.16,1.17,1.18      | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.8     | 1.13,1.14,1.15,1.16,1.17 | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.7     | 1.11,1.12,1.13,1.14,1.15 | 2.1 以降      | 8    | 2.11,2.12 |

## Flink コネクタの取得

Flink コネクタの JAR ファイルは以下の方法で取得できます。

- コンパイル済みの Flink コネクタ JAR ファイルを直接ダウンロードする。
- Flink コネクタを Maven プロジェクトの依存関係として追加し、JAR ファイルをダウンロードする。
- Flink コネクタのソースコードを自分でコンパイルして JAR ファイルを作成する。

Flink コネクタ JAR ファイルの命名形式は以下の通りです。

- Flink 1.15 以降では、`flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar` です。例えば、Flink 1.15 をインストールし、Flink コネクタ 1.2.7 を使用したい場合、`flink-connector-starrocks-1.2.7_flink-1.15.jar` を使用できます。

- Flink 1.15 より前では、`flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar` です。例えば、Flink 1.14 と Scala 2.12 を環境にインストールし、Flink コネクタ 1.2.7 を使用したい場合、`flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar` を使用できます。

> **注意**
>
> 一般的に、Flink コネクタの最新バージョンは、Flink の最新の 3 つのバージョンとのみ互換性を維持します。

### コンパイル済みの Jar ファイルをダウンロード

[Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から対応するバージョンの Flink コネクタ Jar ファイルを直接ダウンロードします。

### Maven 依存関係

Maven プロジェクトの `pom.xml` ファイルに、以下の形式で Flink コネクタを依存関係として追加します。`flink_version`、`scala_version`、`connector_version` をそれぞれのバージョンに置き換えてください。

- Flink 1.15 以降

    ```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}</version>
    </dependency>
    ```

- Flink 1.15 より前のバージョン

    ```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}_${scala_version}</version>
    </dependency>
    ```

### 自分でコンパイル

1. [Flink コネクタのソースコード](https://github.com/StarRocks/starrocks-connector-for-apache-flink) をダウンロードします。
2. 以下のコマンドを実行して、Flink コネクタのソースコードを JAR ファイルにコンパイルします。`flink_version` は対応する Flink バージョンに置き換えてください。

      ```bash
      sh build.sh <flink_version>
      ```

   例えば、環境の Flink バージョンが 1.15 の場合、以下のコマンドを実行する必要があります。

      ```bash
      sh build.sh 1.15
      ```

3. `target/` ディレクトリに移動し、コンパイルによって生成された Flink コネクタ JAR ファイル（例: `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`）を見つけます。

> **注意**
>
> 正式にリリースされていない Flink コネクタの名前には `SNAPSHOT` サフィックスが含まれています。

## オプション

### connector

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: 使用したいコネクタ。値は "starrocks" でなければなりません。

### jdbc-url

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: FE の MySQL サーバーに接続するために使用されるアドレス。複数のアドレスを指定でき、カンマ (,) で区切る必要があります。形式: `jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>`。

### load-url

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: FE の HTTP サーバーに接続するために使用されるアドレス。複数のアドレスを指定でき、セミコロン (;) で区切る必要があります。形式: `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`。

### database-name

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: データをロードしたい StarRocks データベースの名前。

### table-name

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: StarRocks にデータをロードするために使用したいテーブルの名前。

### username

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: StarRocks にデータをロードするために使用したいアカウントのユーザー名。アカウントには、対象の StarRocks テーブルに対する [SELECT および INSERT 権限](../sql-reference/sql-statements/account-management/GRANT.md) が必要です。

### password

**必須**: はい<br/>
**デフォルト値**: NONE<br/>
**説明**: 前述のアカウントのパスワード。

### sink.version

**必須**: いいえ<br/>
**デフォルト値**: AUTO<br/>
**説明**: データをロードするために使用されるインターフェース。このパラメータは Flink コネクタバージョン 1.2.4 以降でサポートされています。 <ul><li>`V1`: [Stream Load](../loading/StreamLoad.md) インターフェースを使用してデータをロードします。1.2.4 より前のコネクタはこのモードのみをサポートします。</li> <li>`V2`: [Stream Load transaction](./Stream_Load_transaction_interface.md) インターフェースを使用してデータをロードします。StarRocks のバージョンが少なくとも 2.4 である必要があります。`V2` を推奨します。これはメモリ使用量を最適化し、より安定した exactly-once 実装を提供します。</li> <li>`AUTO`: StarRocks のバージョンがトランザクション Stream Load をサポートしている場合、自動的に `V2` を選択し、そうでない場合は `V1` を選択します。</li></ul>

### sink.label-prefix

**必須**: いいえ<br/>
**デフォルト値**: NONE<br/>
**説明**: Stream Load に使用されるラベルプレフィックス。コネクタ 1.2.8 以降で exactly-once を使用する場合、設定を推奨します。[exactly-once 使用メモ](#exactly-once) を参照してください。

### sink.semantic

**必須**: いいえ<br/>
**デフォルト値**: at-least-once<br/>
**説明**: sink によって保証されるセマンティクス。有効な値: **at-least-once** および **exactly-once**。

### sink.buffer-flush.max-bytes

**必須**: いいえ<br/>
**デフォルト値**: 94371840(90M)<br/>
**説明**: 一度に StarRocks に送信される前にメモリに蓄積できるデータの最大サイズ。最大値は 64 MB から 10 GB の範囲です。このパラメータを大きな値に設定すると、ロードパフォーマンスが向上しますが、ロードレイテンシが増加する可能性があります。このパラメータは `sink.semantic` が `at-least-once` に設定されている場合にのみ有効です。`sink.semantic` が `exactly-once` に設定されている場合、Flink チェックポイントがトリガーされたときにメモリ内のデータがフラッシュされます。この場合、このパラメータは効果を発揮しません。

### sink.buffer-flush.max-rows

**必須**: いいえ<br/>
**デフォルト値**: 500000<br/>
**説明**: 一度に StarRocks に送信される前にメモリに蓄積できる行の最大数。このパラメータは `sink.version` が `V1` であり、`sink.semantic` が `at-least-once` の場合にのみ利用可能です。有効な値: 64000 から 5000000。

### sink.buffer-flush.interval-ms

**必須**: いいえ<br/>
**デフォルト値**: 300000<br/>
**説明**: データがフラッシュされる間隔。このパラメータは `sink.semantic` が `at-least-once` の場合にのみ利用可能です。有効な値: 1000 から 3600000。単位: ms。

### sink.max-retries

**必須**: いいえ<br/>
**デフォルト値**: 3<br/>
**説明**: Stream Load ジョブを実行するためにシステムが再試行する回数。このパラメータは `sink.version` を `V1` に設定した場合にのみ利用可能です。有効な値: 0 から 10。

### sink.connect.timeout-ms

**必須**: いいえ<br/>
**デフォルト値**: 30000<br/>
**説明**: HTTP 接続を確立するためのタイムアウト。有効な値: 100 から 60000。単位: ms。Flink コネクタ v1.2.9 より前では、デフォルト値は `1000` です。

### sink.socket.timeout-ms

**必須**: いいえ<br/>
**デフォルト値**: -1<br/>
**説明**: 1.2.10 以降でサポートされています。HTTP クライアントがデータを待機する時間の長さ。単位: ms。デフォルト値 `-1` はタイムアウトがないことを意味します。

### sink.wait-for-continue.timeout-ms

**必須**: いいえ<br/>
**デフォルト値**: 10000<br/>
**説明**: 1.2.7 以降でサポートされています。FE からの HTTP 100-continue 応答を待機するためのタイムアウト。有効な値: `3000` から `60000`。単位: ms

### sink.ignore.update-before

**必須**: いいえ<br/>
**デフォルト値**: true<br/>
**説明**: バージョン 1.2.8 以降でサポートされています。Primary Key テーブルにデータをロードする際に Flink からの `UPDATE_BEFORE` レコードを無視するかどうか。このパラメータが false に設定されている場合、レコードは StarRocks テーブルへの削除操作として扱われます。

### sink.parallelism

**必須**: いいえ<br/>
**デフォルト値**: NONE<br/>
**説明**: ロードの並行性。Flink SQL のみで利用可能です。このパラメータが指定されていない場合、Flink プランナーが並行性を決定します。**複数並行性のシナリオでは、ユーザーはデータが正しい順序で書き込まれることを保証する必要があります。**

### sink.properties.*

**必須**: いいえ<br/>
**デフォルト値**: NONE<br/>
**説明**: Stream Load の動作を制御するために使用されるパラメータ。例えば、パラメータ `sink.properties.format` は Stream Load に使用される形式を指定し、CSV や JSON などがあります。サポートされているパラメータとその説明のリストについては、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### sink.properties.format

**必須**: いいえ<br/>
**デフォルト値**: csv<br/>
**説明**: Stream Load に使用される形式。Flink コネクタは、各バッチのデータを StarRocks に送信する前に形式に変換します。有効な値: `csv` および `json`。

### sink.properties.column_separator  

**必須**: いいえ<br/>
**デフォルト値**: \t<br/>
**説明**: CSV 形式のデータのカラムセパレータ。

### sink.properties.row_delimiter

**必須**: いいえ<br/>
**デフォルト値**: \n<br/>
**説明**: CSV 形式のデータの行区切り文字。

### sink.properties.max_filter_ratio  

**必須**: いいえ<br/>
**デフォルト値**: 0<br/>
**説明**: Stream Load の最大エラー許容度。データ品質が不十分なためにフィルタリングされるデータレコードの最大割合です。有効な値: `0` から `1`。デフォルト値: `0`。[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### sink.properties.partial_update

**必須**: いいえ<br/>
**デフォルト値**: `FALSE`<br/>
**説明**: 部分更新を使用するかどうか。有効な値: `TRUE` および `FALSE`。デフォルト値: `FALSE`、この機能を無効にすることを示します。

### sink.properties.partial_update_mode

**必須**: いいえ<br/>
**デフォルト値**: `row`<br/>
**説明**: 部分更新のモードを指定します。有効な値: `row` および `column`。<ul><li>値 `row`（デフォルト）は行モードでの部分更新を意味し、多くのカラムと小さなバッチでのリアルタイム更新に適しています。</li><li>値 `column` はカラムモードでの部分更新を意味し、少ないカラムと多くの行でのバッチ更新に適しています。このようなシナリオでは、カラムモードを有効にすることで更新速度が速くなります。例えば、100 カラムのテーブルで、すべての行に対して 10 カラム（全体の 10%）のみが更新される場合、カラムモードの更新速度は 10 倍速くなります。</li></ul>

### sink.properties.strict_mode

**必須**: いいえ<br/>
**デフォルト値**: false<br/>
**説明**: Stream Load の厳密モードを有効にするかどうかを指定します。不適格な行（カラム値の不一致など）がある場合のロード動作に影響します。有効な値: `true` および `false`。デフォルト値: `false`。[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### sink.properties.compression

**必須**: いいえ<br/>
**デフォルト値**: NONE<br/>
**説明**: 1.2.10 以降でサポートされています。Stream Load に使用される圧縮アルゴリズム。現在、圧縮は JSON 形式でのみサポートされています。有効な値: `lz4_frame`。JSON 形式の圧縮は StarRocks v3.2.7 以降でのみサポートされています。

## Flink と StarRocks 間のデータ型マッピング

| Flink データ型                   | StarRocks データ型   |
|-----------------------------------|-----------------------|
| BOOLEAN                           | BOOLEAN               |
| TINYINT                           | TINYINT               |
| SMALLINT                          | SMALLINT              |
| INTEGER                           | INTEGER               |
| BIGINT                            | BIGINT                |
| FLOAT                             | FLOAT                 |
| DOUBLE                            | DOUBLE                |
| DECIMAL                           | DECIMAL               |
| BINARY                            | INT                   |
| CHAR                              | STRING                |
| VARCHAR                           | STRING                |
| STRING                            | STRING                |
| DATE                              | DATE                  |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME              |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME              |
| ARRAY&lt;T&gt;                        | ARRAY&lt;T&gt;              |
| MAP&lt;KT,VT&gt;                        | JSON STRING           |
| ROW&lt;arg T...&gt;                     | JSON STRING           |

## 使用上の注意

### Exactly Once

- sink が exactly-once セマンティクスを保証する場合、StarRocks を 2.5 以降に、Flink コネクタを 1.2.4 以降にアップグレードすることをお勧めします。
  - Flink コネクタ 1.2.4 以降、exactly-once は StarRocks 2.4 以降で提供される [Stream Load transaction interface](./Stream_Load_transaction_interface.md) に基づいて再設計されています。以前の非トランザクション Stream Load インターフェースに基づく実装と比較して、新しい実装はメモリ使用量とチェックポイントのオーバーヘッドを削減し、ロードのリアルタイムパフォーマンスと安定性を向上させます。

  - StarRocks のバージョンが 2.4 より前、または Flink コネクタのバージョンが 1.2.4 より前の場合、sink は自動的に非トランザクション Stream Load インターフェースに基づく実装を選択します。

- exactly-once を保証するための設定

  - `sink.semantic` の値は `exactly-once` である必要があります。

  - Flink コネクタのバージョンが 1.2.8 以降の場合、`sink.label-prefix` の値を指定することをお勧めします。ラベルプレフィックスは、Flink ジョブ、Routine Load、Broker Load など、StarRocks のすべての種類のロード間で一意である必要があります。

    - ラベルプレフィックスが指定されている場合、Flink コネクタはラベルプレフィックスを使用して、Flink の失敗シナリオで生成される可能性のある残存トランザクションをクリーンアップします。これらの残存トランザクションは、Flink ジョブがチェックポイント中に失敗した場合などに一般的に `PREPARED` ステータスになります。Flink ジョブがチェックポイントから復元されると、Flink コネクタはラベルプレフィックスとチェックポイントの情報に基づいてこれらの残存トランザクションを見つけて中止します。Flink ジョブが終了すると、exactly-once を実装するための二段階コミットメカニズムのため、Flink コネクタはこれらのトランザクションが成功したチェックポイントに含まれるべきかどうかの通知を受け取っていないため、Flink ジョブが終了するときに中止することはできません。これらのトランザクションが中止されるとデータが失われる可能性があります。Flink でエンドツーエンドの exactly-once を達成する方法については、この [ブログ記事](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/) を参照してください。

    - ラベルプレフィックスが指定されていない場合、残存トランザクションはタイムアウト後にのみ StarRocks によってクリーンアップされます。ただし、Flink ジョブが頻繁に失敗すると、トランザクションがタイムアウトする前に StarRocks の `max_running_txn_num_per_db` の制限に達する可能性があります。タイムアウトの長さは、StarRocks FE の設定 `prepared_transaction_default_timeout_second` によって制御され、デフォルト値は `86400`（1 日）です。ラベルプレフィックスが指定されていない場合は、トランザクションがより早く期限切れになるように、これを小さい値に設定できます。

- Flink ジョブが停止または継続的なフェイルオーバーの後に長時間のダウンタイムから最終的にチェックポイントまたはセーブポイントから復元されることが確実である場合、データ損失を避けるために次の StarRocks 設定を適切に調整してください。

  - `prepared_transaction_default_timeout_second`: StarRocks FE の設定で、デフォルト値は `86400` です。この設定の値は Flink ジョブのダウンタイムよりも大きくする必要があります。そうしないと、Flink ジョブを再起動する前にタイムアウトのために成功したチェックポイントに含まれる残存トランザクションが中止され、データ損失が発生する可能性があります。

    この設定に大きな値を設定する場合、ラベルプレフィックスの値を指定することをお勧めします。これにより、残存トランザクションはタイムアウトによる（データ損失を引き起こす可能性がある）代わりに、ラベルプレフィックスとチェックポイントの情報に基づいてクリーンアップされます。

  - `label_keep_max_second` および `label_keep_max_num`: StarRocks FE の設定で、デフォルト値はそれぞれ `259200` および `1000` です。詳細については、[FE 設定](./loading_introduction/loading_considerations.md#fe-configurations) を参照してください。`label_keep_max_second` の値は Flink ジョブのダウンタイムよりも大きくする必要があります。そうしないと、Flink コネクタは Flink のセーブポイントまたはチェックポイントに保存されたトランザクションラベルを使用して StarRocks のトランザクションの状態を確認し、これらのトランザクションがコミットされているかどうかを判断できず、最終的にデータ損失につながる可能性があります。

  これらの設定は変更可能であり、`ADMIN SET FRONTEND CONFIG` を使用して変更できます。

  ```SQL
    ADMIN SET FRONTEND CONFIG ("prepared_transaction_default_timeout_second" = "3600");
    ADMIN SET FRONTEND CONFIG ("label_keep_max_second" = "259200");
    ADMIN SET FRONTEND CONFIG ("label_keep_max_num" = "1000");
  ```

### フラッシュポリシー

Flink コネクタはデータをメモリにバッファし、Stream Load を介して StarRocks にバッチでフラッシュします。フラッシュがトリガーされる方法は、at-least-once と exactly-once で異なります。

at-least-once の場合、次の条件のいずれかが満たされたときにフラッシュがトリガーされます。

- バッファされた行のバイト数が `sink.buffer-flush.max-bytes` の制限に達したとき
- バッファされた行の数が `sink.buffer-flush.max-rows` の制限に達したとき（sink バージョン V1 のみ有効）
- 最後のフラッシュからの経過時間が `sink.buffer-flush.interval-ms` の制限に達したとき
- チェックポイントがトリガーされたとき

exactly-once の場合、フラッシュはチェックポイントがトリガーされたときにのみ発生します。

### ロードメトリクスの監視

Flink コネクタは、ロードを監視するための以下のメトリクスを提供します。

| メトリクス                     | タイプ    | 説明                                                     |
|--------------------------|---------|-----------------------------------------------------------------|
| totalFlushBytes          | カウンター | 成功したフラッシュのバイト数。                                     |
| totalFlushRows           | カウンター | 成功したフラッシュの行数。                                      |
| totalFlushSucceededTimes | カウンター | データが成功裏にフラッシュされた回数。  |
| totalFlushFailedTimes    | カウンター | データのフラッシュが失敗した回数。                  |
| totalFilteredRows        | カウンター | フィルタリングされた行数（totalFlushRows にも含まれます）。    |

## 例

以下の例は、Flink コネクタを使用して Flink SQL または Flink DataStream で StarRocks テーブルにデータをロードする方法を示しています。

### 準備

#### StarRocks テーブルの作成

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

#### Flink 環境のセットアップ

- Flink バイナリ [Flink 1.15.2](https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz) をダウンロードし、`flink-1.15.2` ディレクトリに解凍します。
- [Flink コネクタ 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar) をダウンロードし、`flink-1.15.2/lib` ディレクトリに配置します。
- 以下のコマンドを実行して Flink クラスターを起動します。

    ```shell
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

#### ネットワーク設定

Flink が配置されているマシンが、StarRocks クラスターの FE ノードに [`http_port`](../administration/management/FE_configuration.md#http_port)（デフォルト: `8030`）および [`query_port`](../administration/management/FE_configuration.md#query_port)（デフォルト: `9030`）を介してアクセスでき、BE ノードに [`be_http_port`](../administration/management/BE_configuration.md#be_http_port)（デフォルト: `8040`）を介してアクセスできることを確認してください。

### Flink SQL で実行

- 以下のコマンドを実行して Flink SQL クライアントを起動します。

    ```shell
    ./bin/sql-client.sh
    ```

- Flink テーブル `score_board` を作成し、Flink SQL クライアントを介してテーブルに値を挿入します。
StarRocks の Primary Key テーブルにデータをロードしたい場合、Flink DDL で主キーを定義する必要があります。他のタイプの StarRocks テーブルではオプションです。

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = ''
    );

    INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    ```

### Flink DataStream で実行

入力レコードのタイプに応じて、Flink DataStream ジョブを実装する方法はいくつかあります。例えば、CSV Java `String`、JSON Java `String`、またはカスタム Java オブジェクトです。

- 入力レコードが CSV 形式の `String` の場合。[LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) を参照してください。

    ```java
    /**
     * CSV 形式のレコードを生成します。各レコードには "\t" で区切られた 3 つの値があります。
     * これらの値は StarRocks テーブルのカラム `id`、`name`、`score` にロードされます。
     */
    String[] records = new String[]{
            "1\tstarrocks-csv\t100",
            "2\tflink-csv\t100"
    };
    DataStream<String> source = env.fromElements(records);

    /**
     * 必要なプロパティでコネクタを設定します。
     * また、プロパティ "sink.properties.format" と "sink.properties.column_separator" を追加して、
     * 入力レコードが CSV 形式であり、カラムセパレータが "\t" であることをコネクタに伝えます。
     * CSV 形式のレコードで他のカラムセパレータを使用することもできますが、
     * "sink.properties.column_separator" を対応するように変更することを忘れないでください。
     */
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "csv")
            .withProperty("sink.properties.column_separator", "\t")
            .build();
    // オプションを使用してシンクを作成します。
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- 入力レコードが JSON 形式の `String` の場合。[LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) を参照してください。

```java
    /**
     * JSON 形式のレコードを生成します。
     * 各レコードには、StarRocks テーブルのカラム `id`、`name`、`score` に対応する 3 つのキーと値のペアがあります。
     */
    String[] records = new String[]{
            "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}",
            "{\"id\":2, \"name\":\"flink-json\", \"score\":100}",
    };
    DataStream<String> source = env.fromElements(records);

    /** 
     * 必要なプロパティでコネクタを設定します。
     * また、プロパティ "sink.properties.format" と "sink.properties.strip_outer_array" を追加して、
     * 入力レコードが JSON 形式であり、最外部の配列構造を削除することをコネクタに伝えます。
     */
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .build();
    // オプションを使用してシンクを作成します。
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- 入力レコードがカスタム Java オブジェクトの場合。[LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) を参照してください。

  - この例では、入力レコードはシンプルな POJO `RowData` です。

      ```java
      public static class RowData {
              public int id;
              public String name;
              public int score;
    
              public RowData() {}
    
              public RowData(int id, String name, int score) {
                  this.id = id;
                  this.name = name;
                  this.score = score;
              }
        }
      ```

  - メインプログラムは以下の通りです。

    ```java
    // RowData をコンテナとして使用するレコードを生成します。
    RowData[] records = new RowData[]{
            new RowData(1, "starrocks-rowdata", 100),
            new RowData(2, "flink-rowdata", 100),
        };
    DataStream<RowData> source = env.fromElements(records);

    // 必要なプロパティでコネクタを設定します。
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .build();

    /**
     * Flink コネクタは、Java オブジェクト配列 (Object[]) を使用して StarRocks テーブルにロードされる行を表現し、
     * 各要素はカラムの値です。
     * StarRocks テーブルのスキーマに一致する Object[] のスキーマを定義する必要があります。
     */
    TableSchema schema = TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("name", DataTypes.STRING())
            .field("score", DataTypes.INT())
            // StarRocks テーブルが主キーテーブルの場合、主キー `id` に対して notNull() を指定する必要があります。
            .primaryKey("id")
            .build();
    // RowData をスキーマに従って Object[] に変換します。
    RowDataTransformer transformer = new RowDataTransformer();
    // スキーマ、オプション、トランスフォーマーを使用してシンクを作成します。
    SinkFunction<RowData> starRockSink = StarRocksSink.sink(schema, options, transformer);
    source.addSink(starRockSink);
    ```

  - メインプログラム内の `RowDataTransformer` は以下のように定義されています。

    ```java
    private static class RowDataTransformer implements StarRocksSinkRowBuilder<RowData> {
    
        /**
         * 入力 RowData に従ってオブジェクト配列の各要素を設定します。
         * 配列のスキーマは StarRocks テーブルのスキーマに一致します。
         */
        @Override
        public void accept(Object[] internalRow, RowData rowData) {
            internalRow[0] = rowData.id;
            internalRow[1] = rowData.name;
            internalRow[2] = rowData.score;
            // StarRocks テーブルが主キーテーブルの場合、データロードが UPSERT または DELETE 操作であるかを示すために最後の要素を設定する必要があります。
            internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
        }
    }  
    ```

### Flink CDC 3.0 でデータを同期する（スキーマ変更をサポート）

[Flink CDC 3.0](https://nightlies.apache.org/flink/flink-cdc-docs-stable) フレームワークを使用して、CDC ソース（MySQL や Kafka など）から StarRocks へのストリーミング ELT パイプラインを簡単に構築できます。このパイプラインは、ソースから StarRocks へのデータベース全体の同期、シャーディングテーブルのマージ、スキーマ変更の同期を行うことができます。

v1.2.9 以降、StarRocks 用の Flink コネクタはこのフレームワークに [StarRocks Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/pipeline-connectors/starrocks/) として統合されています。StarRocks Pipeline Connector は以下をサポートします。

- データベースとテーブルの自動作成
- スキーマ変更の同期
- フルおよびインクリメンタルデータの同期

クイックスタートについては、[Flink CDC 3.0 を使用して MySQL から StarRocks へのストリーミング ELT を行う](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/get-started/quickstart/mysql-to-starrocks) を参照してください。

[fast_schema_evolution](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) を有効にするために、StarRocks v3.2.1 以降のバージョンを使用することをお勧めします。これにより、カラムの追加や削除の速度が向上し、リソース使用量が削減されます。

## ベストプラクティス

### 主キーテーブルにデータをロードする

このセクションでは、StarRocks 主キーテーブルにデータをロードして部分更新や条件付き更新を実現する方法を示します。これらの機能の紹介については、[ロードによるデータの変更](./Load_to_Primary_Key_tables.md) を参照してください。
これらの例では Flink SQL を使用します。

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

この例では、カラム `id` と `name` のみを対象にデータをロードする方法を示します。

1. MySQL クライアントで StarRocks テーブル `score_board` に 2 行のデータを挿入します。

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);

    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | flink     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Flink SQL クライアントで Flink テーブル `score_board` を作成します。

   - カラム `id` と `name` のみを含む DDL を定義します。
   - Flink コネクタに部分更新を実行することを伝えるために、オプション `sink.properties.partial_update` を `true` に設定します。
   - Flink コネクタバージョンが `<=` 1.2.7 の場合、オプション `sink.properties.columns` を `id,name,__op` に設定して、Flink コネクタにどのカラムを更新する必要があるかを伝えます。フィールド `__op` を末尾に追加する必要があります。フィールド `__op` はデータロードが UPSERT または DELETE 操作であることを示し、その値はコネクタによって自動的に設定されます。

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = '',
        'sink.properties.partial_update' = 'true',
        -- Flink コネクタバージョン <= 1.2.7 のみ
        'sink.properties.columns' = 'id,name,__op'
    ); 
    ```

3. Flink テーブルに 2 行のデータを挿入します。データ行の主キーは StarRocks テーブルの行と同じですが、カラム `name` の値が変更されています。

    ```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'flink-update');
    ```

4. MySQL クライアントで StarRocks テーブルをクエリします。
  
    ```SQL
    mysql> select * from score_board;
    +------+------------------+-------+
    | id   | name             | score |
    +------+------------------+-------+
    |    1 | starrocks-update |   100 |
    |    2 | flink-update     |   100 |
    +------+------------------+-------+
    2 rows in set (0.02 sec)
    ```

    `name` の値のみが変更され、`score` の値は変更されていないことがわかります。

#### 条件付き更新

この例では、カラム `score` の値に基づいて条件付き更新を行う方法を示します。`id` の更新は、新しい `score` の値が古い値以上の場合にのみ有効です。

1. MySQL クライアントで StarRocks テーブルに 2 行のデータを挿入します。

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    
    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | flink     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Flink テーブル `score_board` を以下の方法で作成します。
  
    - すべてのカラムを含む DDL を定義します。
    - コネクタにカラム `score` を条件として使用することを伝えるために、オプション `sink.properties.merge_condition` を `score` に設定します。
    - コネクタに Stream Load を使用することを伝えるために、オプション `sink.version` を `V1` に設定します。

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = '',
        'sink.properties.merge_condition' = 'score',
        'sink.version' = 'V1'
        );
    ```

3. Flink テーブルに 2 行のデータを挿入します。データ行の主キーは StarRocks テーブルの行と同じですが、最初のデータ行はカラム `score` の値が小さく、2 番目のデータ行はカラム `score` の値が大きくなっています。

    ```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'flink-update', 101);
    ```

4. MySQL クライアントで StarRocks テーブルをクエリします。

    ```SQL
    mysql> select * from score_board;
    +------+--------------+-------+
    | id   | name         | score |
    +------+--------------+-------+
    |    1 | starrocks    |   100 |
    |    2 | flink-update |   101 |
    +------+--------------+-------+
    2 rows in set (0.03 sec)
    ```

   2 番目のデータ行の値のみが変更され、最初のデータ行の値は変更されていないことがわかります。

### BITMAP 型のカラムにデータをロードする

[`BITMAP`](../sql-reference/data-types/other-data-types/BITMAP.md) は、ユニークカウントを加速するために使用されることが多く、UV のカウントなどに使用されます。[正確なユニークカウントに Bitmap を使用する](../using_starrocks/distinct_values/Using_bitmap.md) を参照してください。
ここでは、UV のカウントを例にとり、`BITMAP` 型のカラムにデータをロードする方法を示します。

1. MySQL クライアントで StarRocks 集計テーブルを作成します。

   データベース `test` に、カラム `visit_users` が `BITMAP` 型として定義され、集計関数 `BITMAP_UNION` が設定された集計テーブル `page_uv` を作成します。

    ```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Flink SQL クライアントで Flink テーブルを作成します。

    Flink テーブルのカラム `visit_user_id` は `BIGINT` 型であり、このカラムを StarRocks テーブルの `BITMAP` 型のカラム `visit_users` にロードしたいと考えています。したがって、Flink テーブルの DDL を定義する際には以下に注意してください。
    - Flink は `BITMAP` をサポートしていないため、StarRocks テーブルの `BITMAP` 型のカラム `visit_users` を表すために `BIGINT` 型のカラム `visit_user_id` を定義する必要があります。
    - Flink テーブルと StarRocks テーブルのカラムマッピングをコネクタに伝えるために、オプション `sink.properties.columns` を `page_id,visit_date,user_id,visit_users=to_bitmap(visit_user_id)` に設定する必要があります。また、[`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) 関数を使用して、`BIGINT` 型のデータを `BITMAP` 型に変換することをコネクタに伝える必要があります。

    ```SQL
    CREATE TABLE `page_uv` (
        `page_id` INT,
        `visit_date` TIMESTAMP,
        `visit_user_id` BIGINT
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'page_uv',
        'username' = 'root',
        'password' = '',
        'sink.properties.columns' = 'page_id,visit_date,visit_user_id,visit_users=to_bitmap(visit_user_id)'
    );
    ```

3. Flink SQL クライアントで Flink テーブルにデータをロードします。

```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. MySQL クライアントで StarRocks テーブルからページ UV を計算します。

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `page_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       2 |                           1 |
    |       1 |                           3 |
    +---------+-----------------------------+
    2 rows in set (0.05 sec)
    ```

### HLL 型のカラムにデータをロードする

[`HLL`](../sql-reference/data-types/other-data-types/HLL.md) は、近似的なユニークカウントに使用されます。[近似的なユニークカウントに HLL を使用する](../using_starrocks/distinct_values/Using_HLL.md) を参照してください。

ここでは、UV のカウントを例にとり、`HLL` 型のカラムにデータをロードする方法を示します。

1. StarRocks 集計テーブルを作成します。

   データベース `test` に、カラム `visit_users` が `HLL` 型として定義され、集計関数 `HLL_UNION` が設定された集計テーブル `hll_uv` を作成します。

    ```SQL
    CREATE TABLE `hll_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Flink SQL クライアントで Flink テーブルを作成します。

   Flink テーブルのカラム `visit_user_id` は `BIGINT` 型であり、このカラムを StarRocks テーブルの `HLL` 型のカラム `visit_users` にロードしたいと考えています。したがって、Flink テーブルの DDL を定義する際には以下に注意してください。
    - Flink は `BITMAP` をサポートしていないため、StarRocks テーブルの `HLL` 型のカラム `visit_users` を表すために `BIGINT` 型のカラム `visit_user_id` を定義する必要があります。
    - Flink テーブルと StarRocks テーブルのカラムマッピングをコネクタに伝えるために、オプション `sink.properties.columns` を `page_id,visit_date,user_id,visit_users=hll_hash(visit_user_id)` に設定する必要があります。また、[`hll_hash`](../sql-reference/sql-functions/scalar-functions/hll_hash.md) 関数を使用して、`BIGINT` 型のデータを `HLL` 型に変換することをコネクタに伝える必要があります。

    ```SQL
    CREATE TABLE `hll_uv` (
        `page_id` INT,
        `visit_date` TIMESTAMP,
        `visit_user_id` BIGINT
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'hll_uv',
        'username' = 'root',
        'password' = '',
        'sink.properties.columns' = 'page_id,visit_date,visit_user_id,visit_users=hll_hash(visit_user_id)'
    );
    ```

3. Flink SQL クライアントで Flink テーブルにデータをロードします。

    ```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. MySQL クライアントで StarRocks テーブルからページ UV を計算します。

    ```SQL
    mysql> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `hll_uv` GROUP BY `page_id`;
    **+---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       3 |                           2 |
    |       4 |                           1 |
    +---------+-----------------------------+
    2 rows in set (0.04 sec)
    ```
```