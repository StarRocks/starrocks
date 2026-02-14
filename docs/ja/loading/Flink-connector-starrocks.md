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

| コネクタ | Flink                         | StarRocks     | Java | Scala     |
|-----------|-------------------------------|---------------| ---- |-----------|
| 1.2.14    | 1.16,1.17,1.18,1.19,1.20      | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.12    | 1.16,1.17,1.18,1.19,1.20      | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.11    | 1.15,1.16,1.17,1.18,1.19,1.20 | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.10    | 1.15,1.16,1.17,1.18,1.19      | 2.1 以降      | 8    | 2.11,2.12 |

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

### 一般的なオプション

#### connector

- **Required**: Yes
- **Default value**: NONE
- **Description**: 使用するコネクタ。値は "starrocks" である必要があります。

#### jdbc-url

- **必須**: はい
- **デフォルト値**: NONE
- **説明**: FE の MySQL サーバーへの接続に使用されるアドレスです。複数のアドレスを指定できます。その場合、カンマ (,) で区切る必要があります。形式: `jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>`。

#### load-url

- **必須**: はい
- **デフォルト値**: NONE
- **説明**: FE の HTTP サーバーへの接続に使用されるアドレス。 複数のアドレスを指定できます。複数のアドレスはセミコロン (;) で区切る必要があります。 形式: `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`。

#### database-name

- **必須**: はい
- **デフォルト値**: NONE
- **説明**: データをロードする StarRocks データベースの名前です。

#### table-name

- **必須**: はい
- **デフォルト値**: NONE
- **説明**: データを StarRocks にロードするために使用するテーブルの名前。

#### username

- **必須**: はい
- **デフォルト値**: NONE
- **説明**: データを StarRocks にロードするために使用するアカウントのユーザー名。アカウントには、ターゲットの StarRocks テーブルに対する [SELECT および INSERT 権限](../sql-reference/sql-statements/account-management/GRANT.md) が必要です。

#### password

- **必須**: はい
- **デフォルト値**: NONE
- **説明**: 前述のアカウントのパスワード。

#### sink.version

- **必須**: いいえ
- **デフォルト値**: AUTO
- **説明**: データのロードに使用されるインターフェース。このパラメータは、Flink connector バージョン 1.2.4 以降でサポートされています。有効な値:
  - `V1`: [Stream Load](../loading/StreamLoad.md) インターフェースを使用してデータをロードします。1.2.4 より前のコネクタは、このモードのみをサポートしています。
  - `V2`: [Stream Load transaction](./Stream_Load_transaction_interface.md) インターフェースを使用してデータをロードします。StarRocks のバージョンが 2.4 以上である必要があります。メモリ使用量を最適化し、より安定した exactly-once の実装を提供するため、`V2` を推奨します。
  - `AUTO`: StarRocks のバージョンがトランザクション Stream Load をサポートしている場合、自動的に `V2` を選択し、そうでない場合は `V1` を選択します。

#### sink.label-prefix

- **必須**: いいえ
- **デフォルト値**: NONE
- **説明**: Stream Load が使用するラベルのプレフィックス。connector 1.2.8 以降で exactly-once を使用している場合は、設定することを推奨します。[exactly-once の使用に関する注意事項](#exactly-once) を参照してください。

#### sink.semantic

- **必須**: いいえ
- **デフォルト値**: at-least-once
- **説明**: sink によって保証されるセマンティクス。有効な値: **at-least-once** および **exactly-once**。

#### sink.buffer-flush.max-bytes

- **必須**: いいえ
- **デフォルト値**: 94371840(90M)
- **説明**: 一度に StarRocks に送信する前にメモリに蓄積できるデータの最大サイズ。最大値の範囲は 64 MB から 10 GB です。このパラメータをより大きな値に設定すると、データロードのパフォーマンスが向上しますが、データロードのレイテンシーが増加する可能性があります。このパラメータは、`sink.semantic` が `at-least-once` に設定されている場合にのみ有効です。`sink.semantic` が `exactly-once` に設定されている場合、Flink チェックポイントがトリガーされると、メモリ内のデータはフラッシュされます。この場合、このパラメータは有効になりません。

#### sink.buffer-flush.max-rows

- **必須**: いいえ
- **デフォルト値**: 500000
- **説明**: 一度に StarRocks に送信する前にメモリに蓄積できる最大行数。このパラメータは、`sink.version` が `V1` で、`sink.semantic` が `at-least-once` の場合にのみ使用できます。有効な値: 64000 ～ 5000000。

#### sink.buffer-flush.interval-ms

- **必須**: いいえ
- **デフォルト値**: 300000
- **説明**: データをフラッシュする間隔。このパラメータは、`sink.semantic` が `at-least-once` の場合にのみ使用できます。単位: ms。有効な値の範囲:
  - v1.2.14 より前のバージョン: [1000, 3600000]
  - v1.2.14 以降: (0, 3600000]

#### sink.max-retries

- **必須**: いいえ
- **デフォルト値**: 3
- **説明**: システムが Stream Load ジョブの実行をリトライする回数。このパラメータは、`sink.version` を `V1` に設定した場合にのみ使用できます。有効な値: 0～10。

#### sink.connect.timeout-ms

- **必須**: いいえ
- **デフォルト値**: 30000
- **説明**: HTTP接続を確立するためのタイムアウト。有効な値：100～60000。単位：ms。Flink connector v1.2.9より前のバージョンでは、デフォルト値は `1000` です。

#### sink.socket.timeout-ms

- **必須**: いいえ
- **デフォルト値**: -1
- **説明**: 1.2.10 からサポートされています。 HTTP クライアントがデータを待機する時間。単位: ミリ秒。デフォルト値の `-1` は、タイムアウトがないことを意味します。

#### sink.sanitize-error-log

- **必須**: いいえ
- **デフォルト値**: false
- **説明**: 1.2.12 からサポートされています。 本番環境のセキュリティのために、エラーログ内の機密データをサニタイズするかどうかを指定します。 この項目が `true` に設定されている場合、Stream Load のエラーログ内の機密性の高い行データと列の値は、コネクタと SDK のログの両方で編集されます。 互換性を保つため、デフォルト値は `false` です。

#### sink.wait-for-continue.timeout-ms

- **必須**: いいえ
- **デフォルト値**: 10000
- **説明**: 1.2.7 以降でサポートされています。 FE からの HTTP 100-continue の応答を待機するタイムアウト。有効な値: `3000` ～ `60000`。単位: ミリ秒

#### sink.ignore.update-before

- **必須**: いいえ
- **デフォルト値**: true
- **説明**: バージョン 1.2.8 以降でサポートされています。Primary Key テーブルにデータをロードする際に、Flink からの `UPDATE_BEFORE` レコードを無視するかどうかを指定します。このパラメータを false に設定すると、レコードは StarRocks テーブルに対する削除操作として扱われます。

#### sink.parallelism

- **必須**: いいえ
- **デフォルト値**: NONE
- **説明**: ロードの並行性。Flink SQL でのみ利用可能です。このパラメータが指定されていない場合、Flink プランナーが並行性を決定します。**マルチ並行性のシナリオでは、ユーザーはデータが正しい順序で書き込まれることを保証する必要があります。**

#### sink.properties.*

- **必須**: いいえ
- **デフォルト値**: NONE
- **説明**: Stream Load の動作を制御するために使用されるパラメータです。たとえば、パラメータ `sink.properties.format` は、CSV や JSON など、Stream Load に使用される形式を指定します。サポートされているパラメータとその説明のリストについては、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

#### sink.properties.format

- **必須**: いいえ
- **デフォルト値**: csv
- **説明**: Stream Load で使用される形式。Flink コネクタは、各バッチのデータを StarRocks に送信する前に、この形式に変換します。有効な値: `csv` および `json`。

#### sink.properties.column_separator

- **必須**: いいえ
- **デフォルト値**: \t
- **説明**: CSV形式のデータの列区切り文字。

#### sink.properties.row_delimiter

- **必須**: いいえ
- **デフォルト値**: \n
- **説明**: CSV形式のデータの行区切り文字。

#### sink.properties.max_filter_ratio

- **必須**: いいえ
- **デフォルト値**: 0
- **説明**: Stream Load の最大許容誤差。データ品質が不十分なために除外できるデータレコードの最大パーセンテージです。有効な値: `0` ～ `1`。デフォルト値: `0`。[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

#### sink.properties.partial_update

- **必須**: いいえ
- **デフォルト値**: `FALSE`
- **説明**: 部分更新を使用するかどうか。有効な値: `TRUE` と `FALSE`。デフォルト値: `FALSE`。この機能を無効にすることを示します。

#### sink.properties.partial_update_mode

- **必須**: いいえ
- **デフォルト値**: `row`
- **説明**: 部分更新のモードを指定します。有効な値は、`row` と `column` です。
  - `row` (デフォルト) は、行モードでの部分更新を意味し、多数の列と小さなバッチでのリアルタイム更新に適しています。
  - `column` は、列モードでの部分更新を意味し、少数の列と多数の行でのバッチ更新に適しています。このようなシナリオでは、列モードを有効にすると、更新速度が向上します。たとえば、100 列のテーブルで、すべての行に対して 10 列 (全体の 10%) のみが更新される場合、列モードの更新速度は 10 倍速くなります。

#### sink.properties.strict_mode

- **必須**: いいえ
- **デフォルト値**: false
- **説明**: ストリームロードに厳格モードを有効にするかどうかを指定します。これは、列の値の不整合など、不適格な行がある場合のロード動作に影響します。有効な値：`true` と `false`。デフォルト値：`false`。詳細については、[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

#### sink.properties.compression

- **必須**: いいえ
- **デフォルト値**: NONE
- **説明**: Stream Load で使用される圧縮アルゴリズム。有効な値: `lz4_frame`。JSON 形式の圧縮には、Flink connector 1.2.10 以降と StarRocks v3.2.7 以降が必要です。CSV 形式の圧縮に必要なのは、Flink connector 1.2.11 以降のみです。

#### sink.properties.prepared_timeout

- **必須**: いいえ
- **デフォルト値**: NONE
- **説明**: 1.2.12 以降でサポートされ、`sink.version` が `V2` に設定されている場合にのみ有効です。StarRocks 3.5.4 以降が必要です。トランザクション Stream Load フェーズが `PREPARED` から `COMMITTED` に移行するまでのタイムアウトを秒単位で設定します。通常、必要なのは exactly-once の場合のみです。at-least-once の場合は通常、これを設定する必要はありません (コネクタのデフォルトは 300 秒です)。exactly-once で設定されていない場合、StarRocks FE の構成 `prepared_transaction_default_timeout_second` (デフォルト 86400 秒) が適用されます。[StarRocks トランザクションタイムアウト管理](./Stream_Load_transaction_interface.md#transaction-timeout-management) を参照してください。

#### sink.publish-timeout.ms

- **必須**: いいえ
- **デフォルト値**: -1
- **説明**: 1.2.14 以降でサポートされており、`sink.version` が `V2` に設定されている場合にのみ有効です。Publish フェーズのタイムアウト（ミリ秒単位）。トランザクションがこのタイムアウトよりも長く COMMITTED ステータスのままの場合、システムはそれを成功と見なします。デフォルト値の `-1` は、StarRocks サーバー側のデフォルトの動作を使用することを意味します。Merge Commit が有効になっている場合、デフォルトのタイムアウトは 10000 ミリ秒です。

v1.2.14 以降でサポートされています。Merge Commit を使用すると、システムは複数のサブタスクからのデータを単一の Stream Load トランザクションにマージして、パフォーマンスを向上させることができます。`sink.properties.enable_merge_commit` を `true` に設定すると、この機能を有効にできます。StarRocks の Merge Commit 機能の詳細については、[Merge Commit parameters](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#merge-commit-parameters) を参照してください。

以下の Stream Load プロパティは、Merge Commit の動作を制御するために使用されます。

#### sink.properties.enable_merge_commit

- **必須**: いいえ
- **デフォルト値**: false
- **説明**: Merge Commit を有効にするかどうか。

#### sink.properties.merge_commit_interval_ms

- **必須**: はい（Merge Commit が有効な場合）
- **デフォルト値**: NONE
- **説明**: Merge Commit の時間ウィンドウ（ミリ秒単位）。 システムは、このウィンドウ内に受信したロードリクエストを 1 つのトランザクションにマージします。 値が大きいほど、マージ効率は向上しますが、レイテンシーが増加します。 このプロパティは、`enable_merge_commit` が `true` に設定されている場合に設定する必要があります。

#### sink.properties.merge_commit_parallel

- **必須**: いいえ
- **デフォルト値**: 3
- **説明**: Merge Commit が有効なトランザクションごとに作成されるロードプランの並列度。Flink sink operator の並列度を制御する `sink.parallelism` とは異なります。

#### sink.properties.merge_commit_async

- **必須**: いいえ
- **デフォルト値**: true
- **説明**: Merge Commit のサーバーの戻りモード。デフォルト値は `true` (非同期) であり、スループットを向上させるために、システムデフォルトの動作 (同期) をオーバーライドします。非同期モードでは、サーバーはデータを受信するとすぐに戻ります。コネクタは、Flink のチェックポイントメカニズムを利用して、非同期モードでのデータ損失がないことを保証し、少なくとも 1 回の保証を提供します。ほとんどの場合、この値を変更する必要はありません。

#### sink.merge-commit.max-concurrent-requests

- **必須**: いいえ
- **デフォルト値**: Integer.MAX_VALUE
- **説明**: 同時 Stream Load リクエストの最大数。このプロパティを `0` に設定すると、順序どおりの（シリアル）ロードが保証されます。これは、主キーテーブルに役立ちます。負の値は `Integer.MAX_VALUE` （無制限の並行性）として扱われます。

#### sink.merge-commit.chunk.size

- **必須**: いいえ
- **デフォルト値**: 20971520
- **説明**: チャンクに蓄積されるデータの最大サイズ（バイト単位）。このサイズを超えると、データはフラッシュされ、Stream Load リクエストを介して StarRocks に送信されます。値を大きくするとスループットは向上しますが、メモリ使用量とレイテンシが増加します。値を小さくするとメモリ使用量とレイテンシは減少しますが、スループットが低下する可能性があります。`max-concurrent-requests` が `0` （インオーダーモード）に設定されている場合、一度に実行されるリクエストは 1 つだけであるため、バッチサイズを大きくしてスループットを最大化するために、このプロパティのデフォルト値は 500 MB に変更されます。

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
| ARRAY&lt;T&gt;                    | ARRAY&lt;T&gt;        |
| MAP&lt;KT,VT&gt;                  | JSON STRING           |
| ROW&lt;arg T...&gt;               | JSON STRING           |

## 使用上の注意

### Exactly Once

- sink が exactly-once セマンティクスを保証する場合、StarRocks を 2.5 以降に、Flink コネクタを 1.2.4 以降にアップグレードすることをお勧めします。
  - Flink コネクタ 1.2.4 以降、exactly-once は StarRocks 2.4 以降で提供される [Stream Load transaction interface](./Stream_Load_transaction_interface.md) に基づいて再設計されています。以前の非トランザクション Stream Load インターフェースに基づく実装と比較して、新しい実装はメモリ使用量とチェックポイントのオーバーヘッドを削減し、ロードのリアルタイムパフォーマンスと安定性を向上させます。

  - StarRocks のバージョンが 2.4 より前、または Flink コネクタのバージョンが 1.2.4 より前の場合、sink は自動的に非トランザクション Stream Load インターフェースに基づく実装を選択します。

- exactly-once を保証するための設定

  - `sink.semantic` の値は `exactly-once` である必要があります。

  - Flink コネクタのバージョンが 1.2.8 以降の場合、`sink.label-prefix` の値を指定することをお勧めします。ラベルプレフィックスは、Flink ジョブ、Routine Load、Broker Load など、StarRocks のすべての種類のロード間で一意である必要があります。

    - ラベルプレフィックスが指定されている場合、Flink コネクタはラベルプレフィックスを使用して、Flink の失敗シナリオで生成される可能性のある残存トランザクションをクリーンアップします。これらの残存トランザクションは、Flink ジョブがチェックポイント中に失敗した場合などに一般的に `PREPARED` ステータスになります。Flink ジョブがチェックポイントから復元されると、Flink コネクタはラベルプレフィックスとチェックポイントの情報に基づいてこれらの残存トランザクションを見つけて中止します。Flink ジョブが終了すると、exactly-once を実装するための二段階コミットメカニズムのため、Flink コネクタはこれらのトランザクションが成功したチェックポイントに含まれるべきかどうかの通知を受け取っていないため、Flink ジョブが終了するときに中止することはできません。これらのトランザクションが中止されるとデータが失われる可能性があります。Flink でエンドツーエンドの exactly-once を達成する方法については、この [ブログ記事](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/) を参照してください。

    - ラベルプレフィックスが指定されていない場合、長期間実行中のトランザクションはタイムアウト後にのみ StarRocks によってクリーンアップされます。ただし、トランザクションがタイムアウトする前にFlinkジョブが頻繁に失敗すると、実行中のトランザクション数がStarRocksの `max_running_txn_num_per_db` 制限に達する可能性があります。ラベルプレフィックスが指定されていない場合、`PREPARED` トランザクションのタイムアウトを短く設定することで、より早く期限切れにすることができます。準備済みトランザクションのタイムアウト設定方法については以下を参照してください。

- Flink ジョブが停止または継続的なフェイルオーバーの後に長時間のダウンタイムから最終的にチェックポイントまたはセーブポイントから復元されることが確実である場合、データ損失を避けるために次の StarRocks 設定を適切に調整してください。

  - `PREPARED` トランザクションのタイムアウトを調整します。タイムアウトの設定方法については以下を参照してください。

    タイムアウトは Flink ジョブのダウンタイムよりも長く設定する必要があります。そうしないと、正常なチェックポイントに含まれる未処理トランザクションが、Flink ジョブを再起動する前にタイムアウトにより中止され、データ損失が発生する可能性があります。

  - `label_keep_max_second` および `label_keep_max_num`: StarRocks FE の設定で、デフォルト値はそれぞれ `259200` および `1000` です。詳細については、[FE 設定](./loading_introduction/loading_considerations.md#fe-configurations) を参照してください。`label_keep_max_second` の値は Flink ジョブのダウンタイムよりも大きくする必要があります。そうしないと、Flink コネクタは Flink のセーブポイントまたはチェックポイントに保存されたトランザクションラベルを使用して StarRocks のトランザクションの状態を確認し、これらのトランザクションがコミットされているかどうかを判断できず、最終的にデータ損失につながる可能性があります。

- PREPARED トランザクションのタイムアウト設定方法

  - コネクタ 1.2.12 以降および StarRocks 3.5.4 以降では、コネクタパラメータ `sink.properties.prepared_timeout` を設定することでタイムアウトを設定できます。デフォルトでは値は設定されておらず、StarRocks FE のグローバル設定 `prepared_transaction_default_timeout_second`（デフォルト値は `86400`）がフォールバックされます。

  - その他のバージョンのコネクタまたは StarRocks では、StarRocks FEのグローバル設定 `prepared_transaction_default_timeout_second`（デフォルト値は`86400`）を設定することでタイムアウトを設定できます。

### フラッシュポリシー

Flink コネクタはデータをメモリにバッファし、Stream Load を介して StarRocks にバッチでフラッシュします。フラッシュがトリガーされる方法は、at-least-once と exactly-once で異なります。

at-least-once の場合、次の条件のいずれかが満たされたときにフラッシュがトリガーされます。

- バッファされた行のバイト数が `sink.buffer-flush.max-bytes` の制限に達したとき
- バッファされた行の数が `sink.buffer-flush.max-rows` の制限に達したとき（sink バージョン V1 のみ有効）
- 最後のフラッシュからの経過時間が `sink.buffer-flush.interval-ms` の制限に達したとき
- チェックポイントがトリガーされたとき

exactly-once の場合、フラッシュはチェックポイントがトリガーされたときにのみ発生します。

### Merge Commit

Merge Commit は、StarRocks のトランザクションのオーバーヘッドを比例的に増加させることなく、スループットを拡張するのに役立ちます。Merge Commit がない場合、Flink シンクのサブタスクはそれぞれ独自の Stream Load トランザクションを維持するため、`sink.parallelism` を増やすと、より多くの同時トランザクションが発生し、StarRocks での I/O と Compaction のコストが高くなります。逆に、並行処理を低く抑えると、パイプライン全体の容量が制限されます。Merge Commit を有効にすると、複数のシンクサブタスクからのデータが、各マージウィンドウ内の単一のトランザクションにマージされます。これにより、トランザクションの数を増やすことなく、より高いスループットを得るために `sink.parallelism` を増やすことができます。設定例については、[Merge Commit を使用したデータロード](#load-data-with-merge-commit) を参照してください。

Merge Commit を使用する際の重要な注意事項を以下に示します。

- **単一の並行処理ではメリットがない**

  Flink シンクの並行処理が 1 の場合、データを送信するサブタスクが 1 つしかないため、Merge Commit を有効にしてもメリットはありません。サーバー側の Merge Commit タイムウィンドウにより、追加のレイテンシーが発生する可能性さえあります。

- **少なくとも 1 回のセマンティクスのみ**

  Merge Commit は、少なくとも 1 回のセマンティクスのみを保証します。正確に 1 回のセマンティクスはサポートしていません。Merge Commit が有効になっている場合は、`sink.semantic` を `exactly-once` に設定しないでください。

- **主キーテーブルの順序付け**

  デフォルトでは、`sink.merge-commit.max-concurrent-requests` は `Integer.MAX_VALUE` です。これは、単一のシンクサブタスクが複数の Stream Load リクエストを同時に送信できることを意味します。これにより、順序が狂ったロードが発生する可能性があり、主キーテーブルでは問題になる可能性があります。順序どおりのロードを保証するには、`sink.merge-commit.max-concurrent-requests` を `0` に設定します。ただし、これによりスループットが低下します。または、条件付き更新を使用して、古いデータが新しいデータで上書きされるのを防ぐことができます。設定例については、[主キーテーブルの順序どおりのロード](#in-order-loading-for-primary-key-tables) を参照してください。

- **エンドツーエンドのロードレイテンシー**

  合計ロードレイテンシーは、次の 2 つの部分で構成されます。
  - **コネクタのバッチ処理レイテンシー**: `sink.buffer-flush.interval-ms` と `sink.merge-commit.chunk.size` によって制御されます。チャンクサイズの制限に達したか、フラッシュ間隔が経過したかのいずれか早い方で、データはコネクタからフラッシュされます。コネクタ側の最大レイテンシーは `sink.buffer-flush.interval-ms` です。`sink.buffer-flush.interval-ms` を小さくすると、コネクタ側のレイテンシーは短縮されますが、データはより小さなバッチで送信されます。
  - **StarRocks マージウィンドウ**: `sink.properties.merge_commit_interval_ms` によって制御されます。システムは、この期間待機して、複数のサブタスクからのリクエストを単一のトランザクションにマージします。値を大きくすると、マージ効率が向上しますが (より多くのリクエストが 1 つのトランザクションにマージされます)、サーバー側のレイテンシーが増加します。
  - 一般的なガイドラインとして、`sink.buffer-flush.interval-ms` を `sink.properties.merge_commit_interval_ms` 以下に設定して、各サブタスクが各マージウィンドウ内で少なくとも 1 回はフラッシュできるようにします。たとえば、`merge_commit_interval_ms` が `10000` (10 秒) の場合、`sink.buffer-flush.interval-ms` を `5000` (5 秒) 以下に設定できます。

- **`sink.parallelism` と `sink.properties.merge_commit_parallel` のチューニング**

  これら 2 つのパラメータは、異なるレイヤーで並行処理を制御するため、個別に調整する必要があります。
  - `sink.parallelism` は、Flink シンクサブタスクの数を制御します。各サブタスクは、データをバッファリングして StarRocks に送信します。Flink シンク operator が CPU またはメモリにバインドされている場合は、この値を増やします。Flink の operator ごとの CPU とメモリの使用量を監視して、より多くのサブタスクが必要かどうかを判断できます。
  - `sink.properties.merge_commit_parallel` は、StarRocks が各 Merge Commit トランザクション用に作成するロードプランの並行処理の度合いを制御します。StarRocks がボトルネックになる場合は、この値を増やします。StarRocks のメトリクス [merge_commit_pending_total](../administration/management/monitoring/metrics.md#merge_commit_pending_total) (保留中の Merge Commit タスクの数) と [merge_commit_pending_bytes](../administration/management/monitoring/metrics.md#merge_commit_pending_bytes) (保留中のタスクが保持するバイト数) を監視して、StarRocks 側でより多くの並行処理が必要かどうかを判断できます。高い値が持続する場合は、ロードプランが受信データに追いついていないことを示します。

- **`sink.merge-commit.chunk.size` と `sink.buffer-flush.max-bytes` の関係**:
  - `sink.merge-commit.chunk.size` は、個々の Stream Load リクエスト (チャンクごと) あたりの最大データサイズを制御します。チャンク内のデータがこのサイズに達すると、すぐにフラッシュされます。
  - `sink.buffer-flush.max-bytes` は、すべてのテーブルにわたるキャッシュされたデータすべての合計メモリ制限を制御します。キャッシュされたデータの合計がこの制限を超えると、コネクタはメモリを解放するためにチャンクを早期に削除します。
  - したがって、少なくとも 1 つの完全なチャンクを累積できるように、`sink.buffer-flush.max-bytes` は `sink.merge-commit.chunk.size` よりも大きく設定する必要があります。一般に、特に複数のテーブルがある場合や並行処理が高い場合は、`sink.buffer-flush.max-bytes` は `sink.merge-commit.chunk.size` よりも数倍大きくする必要があります。

### データロードのメトリクスのモニタリング

Flink コネクタは、データロードをモニタリングするために、以下のメトリクスを提供します。

| メトリクス                 | タイプ    | 説明                                                           |
|--------------------------|---------|-----------------------------------------------------------------|
| totalFlushBytes          | counter | フラッシュに成功したバイト数。                                       |
| totalFlushRows           | counter | フラッシュに成功した行数。                                          |
| totalFlushSucceededTimes | counter | データが正常にフラッシュされた回数。                                  |
| totalFlushFailedTimes    | counter | データのフラッシュに失敗した回数。                                    |
| totalFilteredRows        | counter | フィルタリングされた行数。totalFlushRows にも含まれます。              |

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

### Merge Commit を使用したデータロード

このセクションでは、複数の Flink シンクサブタスクが同じ StarRocks テーブルに書き込む場合に、Merge Commit を使用してロードスループットを向上させる方法について説明します。これらの例では、Flink SQL と StarRocks v3.4.0 以降を使用します。

#### 事前準備

データベース `test` を作成し、StarRocks に主キーテーブル `score_board` を作成します。

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

#### 基本設定

この Flink SQL は、10 秒のマージウィンドウでマージコミットを有効にします。すべてのシンクサブタスクからのデータは、各ウィンドウ内で単一のトランザクションにマージされます。

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
    'sink.properties.enable_merge_commit' = 'true',
    'sink.properties.merge_commit_interval_ms' = '10000',
    'sink.buffer-flush.interval-ms' = '5000'
);
```

Flink テーブルにデータを挿入します。データはマージコミットを介して StarRocks にロードされます。

```SQL
INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

#### 主キーテーブルの順序どおりのロード

デフォルトでは、単一のシンクサブタスクが複数の Stream Load リクエストを同時に送信する可能性があり、順序どおりのロードにならない場合があります。データの順序が重要な主キーテーブルの場合、この問題を処理するには 2 つの方法があります。

**方法 1: `sink.merge-commit.max-concurrent-requests` を使用する**

`sink.merge-commit.max-concurrent-requests` を `0` に設定して、各サブタスクが一度に 1 つずつリクエストを送信するようにします。これにより、順序どおりのロードが保証されますが、スループットが低下する可能性があります。

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
    'sink.properties.enable_merge_commit' = 'true',
    'sink.properties.merge_commit_interval_ms' = '10000',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.merge-commit.max-concurrent-requests' = '0'
);

INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

**方法 2: 条件付き更新を使用する**

より高いスループットのために同時リクエストを維持しつつ、古いデータが新しいデータを上書きするのを防ぎたい場合は、[条件付き更新](#conditional-update) を使用できます。`sink.properties.merge_condition` を列（例えば、バージョン列やタイムスタンプ列）に設定することで、入力値が既存の値以上の場合にのみ更新が有効になるようにします。

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
    'sink.properties.enable_merge_commit' = 'true',
    'sink.properties.merge_commit_interval_ms' = '10000',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.merge_condition' = 'score'
);

INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

この構成では、同時リクエストが許可されます（デフォルトの `sink.merge-commit.max-concurrent-requests` は `Integer.MAX_VALUE` です）。ただし、行の更新は、新しい `score` が既存の `score` 以上の場合にのみ有効になります。これにより、順序が異なるデータロードの場合でも、新しいデータが古いデータで上書きされるのを防ぎます。

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
