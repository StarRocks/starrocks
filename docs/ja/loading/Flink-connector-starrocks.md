---
displayed_sidebar: docs
---

# Apache Flink® からの継続的なデータロード

StarRocks は、Apache Flink® 用の StarRocks Connector (略して Flink connector) という独自のコネクタを提供しており、Flink を使用して StarRocks テーブルにデータをロードするのに役立ちます。基本的な原則は、データを蓄積し、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を介して一度に StarRocks にロードすることです。

Flink connector は、DataStream API、Table API & SQL、および Python API をサポートしています。Apache Flink® が提供する [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/) よりも、パフォーマンスが高く、安定しています。

> **注意**
>
> Flink connector を使用して StarRocks テーブルにデータをロードするには、ターゲットの StarRocks テーブルに対する SELECT および INSERT 権限が必要です。これらの権限がない場合は、[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) に記載されている手順に従って、StarRocks クラスタへの接続に使用するユーザーにこれらの権限を付与してください。

## バージョン要件

| コネクタ | Flink                         | StarRocks     | Java | Scala     |
|-----------|-------------------------------|---------------| ---- |-----------|
| 1.2.14    | 1.16,1.17,1.18,1.19,1.20      | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.12    | 1.16,1.17,1.18,1.19,1.20      | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.11    | 1.15,1.16,1.17,1.18,1.19,1.20 | 2.1 以降      | 8    | 2.11,2.12 |
| 1.2.10    | 1.15,1.16,1.17,1.18,1.19      | 2.1 以降      | 8    | 2.11,2.12 |

## Flink コネクタの取得

Flink コネクタの JAR ファイルは、以下の方法で取得できます。

- コンパイル済みの Flink コネクタ JAR ファイルを直接ダウンロードする。
- Maven プロジェクトに Flink コネクタを依存関係として追加し、JAR ファイルをダウンロードする。
- Flink コネクタのソースコードを自分でコンパイルして JAR ファイルを作成する。

Flink コネクタ JAR ファイルの命名規則は以下のとおりです。

- Flink 1.15 以降では、`flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar` となります。たとえば、Flink 1.15 をインストールし、Flink コネクタ 1.2.7 を使用する場合は、`flink-connector-starrocks-1.2.7_flink-1.15.jar` を使用できます。

- Flink 1.15 より前では、`flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar` となります。たとえば、Flink 1.14 と Scala 2.12 を環境にインストールし、Flink コネクタ 1.2.7 を使用する場合は、`flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar` を使用できます。

> **注意**
>
> 一般的に、Flink コネクタの最新バージョンは、Flink の直近 3 つのバージョンとのみ互換性を維持します。

### コンパイル済みの Jar ファイルをダウンロードする

[Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から、対応するバージョンの Flink connector Jar ファイルを直接ダウンロードします。

### Maven の依存関係

Maven プロジェクトの `pom.xml` ファイルに、次の形式に従って Flink コネクタを依存関係として追加します。 `flink_version` 、 `scala_version` 、および `connector_version` をそれぞれのバージョンに置き換えます。

- Flink 1.15 以降

```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}</version>
    </dependency>
    ```

- Flink 1.15より前のバージョン

```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}_${scala_version}</version>
    </dependency>
    ```

### 自分でコンパイルする

1.  [Flink connector のソースコード](https://github.com/StarRocks/starrocks-connector-for-apache-flink) をダウンロードします。
2.  次のコマンドを実行して、Flink connector のソースコードを JAR ファイルにコンパイルします。`flink_version` は、対応する Flink のバージョンに置き換えてください。

```bash
      sh build.sh <flink_version>
      ```

たとえば、環境内の Flink のバージョンが 1.15 の場合は、次のコマンドを実行する必要があります。

```bash
      sh build.sh 1.15
      ```

3. `target/` ディレクトリに移動して、コンパイル時に生成された Flink コネクタ JAR ファイル（例：`flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`）を見つけます。

> **NOTE**
>
> 正式にリリースされていない Flink コネクタの名前には、`SNAPSHOT` サフィックスが含まれています。

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

## Usage notes

## 使用上の注意

### Exactly Once

- Exactly Onceのセマンティクスを保証するために、StarRocksを2.5以降、Flink connectorを1.2.4以降にアップグレードすることを推奨します。
  - Flink connector 1.2.4以降、Exactly Onceは、StarRocks 2.4以降で提供される[Stream Load transaction interface](./Stream_Load_transaction_interface.md) に基づいて再設計されました。トランザクションを使用しないStream Loadインターフェースに基づく以前の実装と比較して、新しい実装ではメモリ使用量とチェックポイントのオーバーヘッドが削減され、ロードのリアルタイムパフォーマンスと安定性が向上します。

  - StarRocksのバージョンが2.4より前、またはFlink connectorのバージョンが1.2.4より前の場合、シンクはトランザクションを使用しないStream Loadインターフェースに基づく実装を自動的に選択します。

- Exactly Onceを保証するための構成

  - `sink.semantic` の値は `exactly-once` である必要があります。

  - Flink connectorのバージョンが1.2.8以降の場合、`sink.label-prefix` の値を指定することを推奨します。ラベルのプレフィックスは、Flinkジョブ、Routine Load、Broker Loadなど、StarRocksのすべてのタイプのロード間で一意である必要があることに注意してください。

    - ラベルのプレフィックスが指定されている場合、Flink connectorはラベルのプレフィックスを使用して、チェックポイントがまだ進行中のときにFlinkジョブが失敗するなど、一部のFlinkの障害シナリオで生成される可能性のある未完了のトランザクションをクリーンアップします。これらの未完了のトランザクションは、`SHOW PROC '/transactions/<db_id>/running';` を使用してStarRocksで表示すると、通常 `PREPARED` ステータスになっています。Flinkジョブがチェックポイントから復元されると、Flink connectorはラベルのプレフィックスとチェックポイント内の一部の情報に基づいてこれらの未完了のトランザクションを見つけ、それらを中止します。Flink connectorは、Exactly Onceを実装するための2相コミットメカニズムのため、Flinkジョブが終了したときにそれらを中止できません。Flinkジョブが終了すると、Flink connectorはトランザクションを成功したチェックポイントに含める必要があるかどうかについて、Flinkチェックポイントコーディネーターからの通知を受信しておらず、これらのトランザクションを中止するとデータが失われる可能性があります。FlinkでエンドツーエンドのExactly Onceを達成する方法の概要については、この[ブログ記事](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/) を参照してください。

    - ラベルのプレフィックスが指定されていない場合、未完了のトランザクションは、タイムアウト後にのみStarRocksによってクリーンアップされます。ただし、トランザクションがタイムアウトする前にFlinkジョブが頻繁に失敗すると、実行中のトランザクションの数がStarRocksの `max_running_txn_num_per_db` の制限に達する可能性があります。ラベルのプレフィックスが指定されていない場合は、`PREPARED` トランザクションのタイムアウトを小さくして、より早く期限切れになるように設定できます。PREPAREDのタイムアウトを設定する方法については、以下を参照してください。

- 停止または継続的なフェイルオーバーにより、Flinkジョブが長時間のダウンタイム後にチェックポイントまたはセーブポイントから最終的に回復することが確実な場合は、データ損失を回避するために、次のStarRocks構成を適宜調整してください。

  - `PREPARED` トランザクションのタイムアウトを調整します。タイムアウトの設定方法については、以下を参照してください。

    タイムアウトは、Flinkジョブのダウンタイムよりも長くする必要があります。そうしないと、Flinkジョブを再起動する前に、成功したチェックポイントに含まれる未完了のトランザクションがタイムアウトのために中止され、データが失われる可能性があります。

    この構成に大きな値を設定する場合は、タイムアウトが原因ではなく、ラベルのプレフィックスとチェックポイント内の一部の情報に基づいて未完了のトランザクションをクリーンアップできるように、`sink.label-prefix` の値を指定することをお勧めします（タイムアウトが原因でデータが失われる可能性があります）。

  - `label_keep_max_second` と `label_keep_max_num`: StarRocks FEの構成で、デフォルト値はそれぞれ `259200` と `1000` です。詳細については、[FE configurations](./loading_introduction/loading_considerations.md#fe-configurations) を参照してください。`label_keep_max_second` の値は、Flinkジョブのダウンタイムよりも長くする必要があります。そうしないと、Flink connectorは、Flinkのセーブポイントまたはチェックポイントに保存されているトランザクションラベルを使用してStarRocksのトランザクションの状態を確認し、これらのトランザクションがコミットされたかどうかを判断できず、最終的にデータが失われる可能性があります。

- PREPAREDトランザクションのタイムアウトを設定する方法

  - Connector 1.2.12+ および StarRocks 3.5.4+ の場合、コネクターパラメーター `sink.properties.prepared_timeout` を構成してタイムアウトを設定できます。デフォルトでは、値は設定されておらず、StarRocks FEのグローバル構成 `prepared_transaction_default_timeout_second` （デフォルト値は `86400`）にフォールバックします。

  - その他のバージョンのConnectorまたはStarRocksの場合、StarRocks FEのグローバル構成 `prepared_transaction_default_timeout_second` （デフォルト値は `86400`）を構成してタイムアウトを設定できます。

### Flush Policy

Flink コネクタは、メモリにデータをバッファリングし、Stream Load を介して StarRocks にバッチでフラッシュします。フラッシュがトリガーされる方法は、少なくとも 1 回と正確に 1 回の間で異なります。

少なくとも 1 回の場合、フラッシュは次のいずれかの条件が満たされた場合にトリガーされます。

- バッファリングされた行のバイト数が制限 `sink.buffer-flush.max-bytes` に達した場合
- バッファリングされた行の数が制限 `sink.buffer-flush.max-rows` に達した場合（シンクバージョン V1 のみ有効）
- 最後のフラッシュからの経過時間が制限 `sink.buffer-flush.interval-ms` に達した場合
- チェックポイントがトリガーされた場合

正確に 1 回の場合、フラッシュはチェックポイントがトリガーされた場合にのみ発生します。

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

| メトリクス                   | タイプ    | 説明                                                              |
|--------------------------|---------|-----------------------------------------------------------------|
| totalFlushBytes          | counter | フラッシュに成功したバイト数。                                                  |
| totalFlushRows           | counter | フラッシュに成功した行数。                                                      |
| totalFlushSucceededTimes | counter | データが正常にフラッシュされた回数。                                                |
| totalFlushFailedTimes    | counter | データのフラッシュに失敗した回数。                                                  |
| totalFilteredRows        | counter | フィルタリングされた行数。totalFlushRows にも含まれます。                                |

## Examples

以下の例では、Flink コネクタを使用して、Flink SQL または Flink DataStream でデータを StarRocks テーブルにロードする方法を示します。

### 事前準備

#### StarRocks テーブルを作成する

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

- Flink のバイナリファイル [Flink 1.15.2](https://archive.apache.org/dist.flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz) をダウンロードし、`flink-1.15.2` ディレクトリに解凍します。
- [Flink connector 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar) をダウンロードし、`flink-1.15.2/lib` ディレクトリに配置します。
- 次のコマンドを実行して、Flink クラスタを起動します。

```shell
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

#### ネットワーク構成

Flink が配置されているマシンが、StarRocks クラスタの FE ノードに [`http_port`](../administration/management/FE_configuration.md#http_port) （デフォルト：`8030`）と [`query_port`](../administration/management/FE_configuration.md#query_port) （デフォルト：`9030`）を介して、また BE ノードに [`be_http_port`](../administration/management/BE_configuration.md#be_http_port) （デフォルト：`8040`）を介してアクセスできることを確認してください。

### Flink SQL で実行する

- 次のコマンドを実行して、Flink SQL クライアントを起動します。

```shell
    ./bin/sql-client.sh
    ```

- Flink テーブル `score_board` を作成し、Flink SQL Client 経由でテーブルに値を挿入します。
  StarRocks の主キーテーブルにデータをロードする場合は、Flink DDL で主キーを定義する必要があります。他のタイプの StarRocks テーブルではオプションです。

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

### Flink DataStream での実行

入力レコードのタイプ（CSV Java `String`、JSON Java `String`、またはカスタム Java オブジェクトなど）に応じて、Flink DataStream ジョブを実装する方法がいくつかあります。

- 入力レコードが CSV 形式の `String` である場合。[LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) に完全な例があります。

```java
    /**
     * Generate CSV-format records. Each record has three values separated by "\t". 
     * These values will be loaded to the columns `id`, `name`, and `score` in the StarRocks table.
     */
    String[] records = new String[]{
            "1\tstarrocks-csv\t100",
            "2\tflink-csv\t100"
    };
    DataStream<String> source = env.fromElements(records);

    /**
     * Configure the connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.column_separator"
     * to tell the connector the input records are CSV-format, and the column separator is "\t".
     * You can also use other column separators in the CSV-format records,
     * but remember to modify the "sink.properties.column_separator" correspondingly.
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
    // Create the sink with the options.
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- 入力レコードは JSON 形式の `String` です。完全な例については、 [LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) を参照してください。

```java
    /**
     * Generate JSON-format records. 
     * Each record has three key-value pairs corresponding to the columns `id`, `name`, and `score` in the StarRocks table.
     */
    String[] records = new String[]{
            "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}",
            "{\"id\":2, \"name\":\"flink-json\", \"score\":100}",
    };
    DataStream<String> source = env.fromElements(records);

    /** 
     * Configure the connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.strip_outer_array"
     * to tell the connector the input records are JSON-format and to strip the outermost array structure. 
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
    // Create the sink with the options.
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- 入力レコードは、カスタム Java オブジェクトです。完全な例については、[LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) を参照してください。

  - この例では、入力レコードは単純な POJO `RowData` です。

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

- メインプログラムは以下のとおりです。

```java
    // Generate records which use RowData as the container.
    RowData[] records = new RowData[]{
            new RowData(1, "starrocks-rowdata", 100),
            new RowData(2, "flink-rowdata", 100),
        };
    DataStream<RowData> source = env.fromElements(records);

    // Configure the connector with the required properties.
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .build();

    /**
     * The Flink connector will use a Java object array (Object[]) to represent a row to be loaded into the StarRocks table,
     * and each element is the value for a column.
     * You need to define the schema of the Object[] which matches that of the StarRocks table.
     */
    TableSchema schema = TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("name", DataTypes.STRING())
            .field("score", DataTypes.INT())
            // When the StarRocks table is a Primary Key table, you must specify notNull(), for example, DataTypes.INT().notNull(), for the primary key `id`.
            .primaryKey("id")
            .build();
    // Transform the RowData to the Object[] according to the schema.
    RowDataTransformer transformer = new RowDataTransformer();
    // Create the sink with the schema, options, and transformer.
    SinkFunction<RowData> starRockSink = StarRocksSink.sink(schema, options, transformer);
    source.addSink(starRockSink);
    ```

- メインプログラムの `RowDataTransformer` は、次のように定義されています。

```java
    private static class RowDataTransformer implements StarRocksSinkRowBuilder<RowData> {
    
        /**
         * Set each element of the object array according to the input RowData.
         * The schema of the array matches that of the StarRocks table.
         */
        @Override
        public void accept(Object[] internalRow, RowData rowData) {
            internalRow[0] = rowData.id;
            internalRow[1] = rowData.name;
            internalRow[2] = rowData.score;
            // When the StarRocks table is a Primary Key table, you need to set the last element to indicate whether the data loading is an UPSERT or DELETE operation.
            internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
        }
    }  
    ```

### Flink CDC 3.0 でのデータ同期 (スキーマ変更のサポート付き)

[Flink CDC 3.0](https://nightlies.apache.org/flink/flink-cdc-docs-stable) フレームワークを使用すると、CDC ソース (MySQL や Kafka など) から StarRocks へのストリーミング ELT パイプラインを簡単に構築できます。このパイプラインは、ソースから StarRocks へのデータベース全体、マージされたシャーディングテーブル、およびスキーマの変更を同期できます。

v1.2.9 以降、StarRocks 用の Flink コネクタは、[StarRocks Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/pipeline-connectors/starrocks/) としてこのフレームワークに統合されています。StarRocks Pipeline Connector は以下をサポートしています。

- データベースとテーブルの自動作成
- スキーマ変更の同期
- 完全および増分データ同期

クイックスタートについては、[Flink CDC 3.0 with StarRocks Pipeline Connector を使用した MySQL から StarRocks へのストリーミング ELT](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/get-started/quickstart/mysql-to-starrocks) を参照してください。

[fast_schema_evolution](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) を有効にするには、StarRocks v3.2.1 以降のバージョンを使用することをお勧めします。これにより、列の追加または削除の速度が向上し、リソースの使用量が削減されます。

## ベストプラクティス

### 主キーテーブルへのデータロード

このセクションでは、部分更新と条件付き更新を実現するために、 StarRocks の主キーテーブルにデータをロードする方法について説明します。
これらの機能の概要については、 [Change data through loading](./Load_to_Primary_Key_tables.md) を参照してください。
これらの例では、Flink SQLを使用します。

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

#### 部分更新

この例では、`id` と `name` の列にのみデータをロードする方法を示します。

1. MySQL クライアントで、2 つのデータ行を StarRocks テーブル `score_board` に挿入します。

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

2. Flink SQL client で Flink テーブル `score_board` を作成します。

   - `id` と `name` のカラムのみを含む DDL を定義します。
   - オプション `sink.properties.partial_update` を `true` に設定して、Flink コネクタに部分更新を実行するように指示します。
   - Flink コネクタのバージョンが `&lt;=` 1.2.7 の場合は、オプション `sink.properties.columns` を `id,name,__op` に設定して、更新する必要があるカラムを Flink コネクタに指示する必要もあります。`__op` フィールドを末尾に追加する必要があることに注意してください。`__op` フィールドは、データロードが UPSERT または DELETE 操作であることを示し、その値はコネクタによって自動的に設定されます。

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
        -- only for Flink connector version <= 1.2.7
        'sink.properties.columns' = 'id,name,__op'
    ); 
    ```

3. 2つのデータ行を Flink テーブルに挿入します。データ行の主キーは、 StarRocks テーブルの行の主キーと同じですが、`name` 列の値は変更されています。

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

`name` の値のみが変更され、`score` の値は変更されないことがわかります。

#### 条件付き更新

この例では、カラム `score` の値に応じて条件付き更新を行う方法を示します。`id` の更新は、`score` の新しい値が古い値以上の場合にのみ有効になります。

1. MySQL クライアントで、2 つのデータ行を StarRocks テーブルに挿入します。

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

2. 次の方法でFlinkテーブル `score_board` を作成します。

    - すべてのカラムを含むDDLを定義します。
    - オプション `sink.properties.merge_condition` を `score` に設定して、カラム `score` を条件として使用するようにコネクタに指示します。
    - オプション `sink.version` を `V1` に設定して、 Stream Load を使用するようにコネクタに指示します。

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

3. 2つのデータ行を Flink テーブルに挿入します。データ行の主キーは、 StarRocks テーブルの行の主キーと同じです。最初のデータ行の `score` 列の値は小さく、2番目のデータ行の `score` 列の値は大きくなっています。

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

2番目のデータ行の値のみが変更され、最初のデータ行の値は変更されないことがわかります。

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

### BITMAP 型の列へのデータロード

[`BITMAP`](../sql-reference/data-types/other-data-types/BITMAP.md) は、UV のカウントなど、重複排除カウントを高速化するためによく使用されます。詳細については、[Bitmap を使用した正確な重複排除カウント](../using_starrocks/distinct_values/Using_bitmap.md) を参照してください。
ここでは、UV のカウントを例として、`BITMAP` 型の列にデータをロードする方法を示します。

1. MySQL クライアントで StarRocks 集計テーブルを作成します。

   データベース `test` に、`visit_users` 列が `BITMAP` 型として定義され、集計関数 `BITMAP_UNION` が設定された集計テーブル `page_uv` を作成します。

```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Flink SQL client で Flink テーブルを作成します。

    Flink テーブルの `visit_user_id` 列は `BIGINT` 型ですが、この列を StarRocks テーブルの `BITMAP` 型の列 `visit_users` にロードしたいとします。したがって、Flink テーブルの DDL を定義する際には、以下の点に注意してください。
    - Flink は `BITMAP` をサポートしていないため、StarRocks テーブルの `BITMAP` 型の列 `visit_users` を表すために、`BIGINT` 型として列 `visit_user_id` を定義する必要があります。
    - オプション `sink.properties.columns` を `page_id,visit_date,user_id,visit_users=to_bitmap(visit_user_id)` に設定する必要があります。これにより、Flink テーブルと StarRocks テーブルの間の列のマッピングがコネクタに伝えられます。また、[`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) 関数を使用して、`BIGINT` 型のデータを `BITMAP` 型に変換するようにコネクタに指示する必要があります。

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

Flink SQL client で Flink テーブルにデータをロードします。

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

### `HLL` 型の列へのデータロード

[`HLL`](../sql-reference/data-types/other-data-types/HLL.md) は、近似重複排除カウントに使用できます。詳細については、[HLL を使用した近似重複排除カウント](../using_starrocks/distinct_values/Using_HLL.md) を参照してください。

ここでは、UV のカウントを例として、`HLL` 型の列にデータをロードする方法を示します。

1. StarRocks 集計テーブルを作成する

   データベース `test` で、列 `visit_users` が `HLL` 型として定義され、集計関数 `HLL_UNION` で構成された集計テーブル `hll_uv` を作成します。

```SQL
    CREATE TABLE `hll_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Flink SQL client で Flink テーブルを作成します。

   Flink テーブルの `visit_user_id` 列は `BIGINT` 型ですが、この列を StarRocks テーブルの `HLL` 型の `visit_users` 列にロードしたいとします。したがって、Flink テーブルの DDL を定義する際には、以下の点に注意してください。
    - Flink は `BITMAP` をサポートしていないため、StarRocks テーブルの `HLL` 型の `visit_users` 列を表すために、`BIGINT` 型として `visit_user_id` 列を定義する必要があります。
    - オプション `sink.properties.columns` を `page_id,visit_date,user_id,visit_users=hll_hash(visit_user_id)` に設定して、Flink テーブルと StarRocks テーブルの間の列のマッピングをコネクタに指示する必要があります。また、[`hll_hash`](../sql-reference/sql-functions/scalar-functions/hll_hash.md) 関数を使用して、`BIGINT` 型のデータを `HLL` 型に変換するようにコネクタに指示する必要があります。

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

Flink SQL client で Flink テーブルにデータをロードします。

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