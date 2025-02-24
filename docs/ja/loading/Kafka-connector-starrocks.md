---
displayed_sidebar: docs
---

# Kafka コネクタを使用したデータのロード

StarRocks は、Apache Kafka® コネクタ（StarRocks Connector for Apache Kafka®、以下 Kafka コネクタと略します）という独自開発のコネクタを提供しています。このコネクタはシンクコネクタとして、Kafka からメッセージを継続的に消費し、StarRocks にロードします。Kafka コネクタは少なくとも一度のセマンティクスを保証します。

Kafka コネクタは Kafka Connect とシームレスに統合でき、StarRocks が Kafka エコシステムとより良く統合されることを可能にします。リアルタイムデータを StarRocks にロードしたい場合には賢明な選択です。Routine Load と比較して、以下のシナリオでは Kafka コネクタの使用が推奨されます：

- Routine Load は CSV、JSON、Avro フォーマットでのデータロードのみをサポートしていますが、Kafka コネクタは Protobuf など、より多くのフォーマットでデータをロードできます。Kafka Connect のコンバータを使用してデータを JSON や CSV フォーマットに変換できる限り、Kafka コネクタを介して StarRocks にデータをロードできます。
- Debezium フォーマットの CDC データなど、データ変換をカスタマイズします。
- 複数の Kafka トピックからデータをロードします。
- Confluent Cloud からデータをロードします。
- ロードバッチサイズ、並行性、その他のパラメータを細かく制御して、ロード速度とリソース使用率のバランスを取る必要があります。

## 準備

### バージョン要件

| コネクタ | Kafka                    | StarRocks | Java |
|-----------|--------------------------|-----------| ---- |
| 1.0.4     | 3.4                      | 2.5 and later | 8    |
| 1.0.3     | 3.4                      | 2.5 and later | 8    |

### Kafka 環境のセットアップ

自己管理の Apache Kafka クラスターと Confluent Cloud の両方がサポートされています。

- 自己管理の Apache Kafka クラスターの場合、[Apache Kafka クイックスタート](https://kafka.apache.org/quickstart)を参照して、Kafka クラスターを迅速にデプロイできます。Kafka Connect はすでに Kafka に統合されています。
- Confluent Cloud の場合、Confluent アカウントを持ち、クラスターを作成していることを確認してください。

### Kafka コネクタのダウンロード

Kafka コネクタを Kafka Connect に提出します：

- 自己管理の Kafka クラスター：

  [starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases) をダウンロードして解凍します。

- Confluent Cloud：

  現在、Kafka コネクタは Confluent Hub にアップロードされていません。[starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases) をダウンロードして解凍し、ZIP ファイルにパッケージして Confluent Cloud にアップロードする必要があります。

### ネットワーク構成

Kafka が配置されているマシンが StarRocks クラスターの FE ノードに [`http_port`](../administration/management/FE_configuration.md#http_port)（デフォルト：`8030`）および [`query_port`](../administration/management/FE_configuration.md#query_port)（デフォルト：`9030`）を介してアクセスでき、BE ノードに [`be_http_port`](../administration/management/BE_configuration.md#be_http_port)（デフォルト：`8040`）を介してアクセスできることを確認してください。

## 使用方法

このセクションでは、自己管理の Kafka クラスターを例にとり、Kafka コネクタと Kafka Connect を設定し、Kafka Connect を実行して StarRocks にデータをロードする方法を説明します。

### データセットの準備

Kafka クラスターのトピック `test` に JSON フォーマットのデータが存在すると仮定します。

```JSON
{"id":1,"city":"New York"}
{"id":2,"city":"Los Angeles"}
{"id":3,"city":"Chicago"}
```

### テーブルの作成

StarRocks クラスターのデータベース `example_db` に JSON フォーマットデータのキーに基づいてテーブル `test_tbl` を作成します。

```SQL
CREATE DATABASE example_db;
USE example_db;
CREATE TABLE test_tbl (id INT, city STRING);
```

### Kafka コネクタと Kafka Connect の設定、そして Kafka Connect を実行してデータをロード

#### スタンドアロンモードで Kafka Connect を実行

1. Kafka コネクタを設定します。Kafka インストールディレクトリの **config** ディレクトリに、Kafka コネクタ用の設定ファイル **connect-StarRocks-sink.properties** を作成し、以下のパラメータを設定します。詳細なパラメータと説明については、[Parameters](#Parameters) を参照してください。

    :::info

    - この例では、StarRocks が提供する Kafka コネクタは、Kafka からデータを継続的に消費し、StarRocks にデータをロードできるシンクコネクタです。
    - ソースデータが CDC データ（例えば、Debezium フォーマットのデータ）であり、StarRocks テーブルが主キーテーブルである場合、StarRocks が提供する Kafka コネクタ用の設定ファイル **connect-StarRocks-sink.properties** で [`transform`](#load-debezium-formatted-cdc-data) を設定し、ソースデータの変更を主キーテーブルに同期する必要があります。

    :::

    ```yaml
    name=starrocks-kafka-connector
    connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
    topics=test
    key.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable=true
    value.converter.schemas.enable=false
    # StarRocks クラスター内の FE の HTTP URL。デフォルトポートは 8030。
    starrocks.http.url=192.168.xxx.xxx:8030
    # Kafka トピック名が StarRocks テーブル名と異なる場合、それらの間のマッピング関係を設定する必要があります。
    starrocks.topic2table.map=test:test_tbl
    # StarRocks のユーザー名を入力します。
    starrocks.username=user1
    # StarRocks のパスワードを入力します。
    starrocks.password=123456
    starrocks.database.name=example_db
    sink.properties.strip_outer_array=true
    ```

2. Kafka Connect を設定して実行します。

   1. Kafka Connect を設定します。**config** ディレクトリの設定ファイル **config/connect-standalone.properties** で、以下のパラメータを設定します。詳細なパラメータと説明については、[Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running) を参照してください。

        ```yaml
        # Kafka ブローカーのアドレス。複数の Kafka ブローカーのアドレスはカンマ（,）で区切る必要があります。
        # この例では、Kafka クラスターにアクセスするためのセキュリティプロトコルとして PLAINTEXT を使用しています。他のセキュリティプロトコルを使用して Kafka クラスターにアクセスする場合は、このファイルに関連情報を設定する必要があります。
        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # 解凍後の Kafka コネクタの絶対パス。例：
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```

   2. Kafka Connect を実行します。

        ```Bash
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-standalone.sh config/connect-standalone.properties config/connect-starrocks-sink.properties
        ```

#### 分散モードで Kafka Connect を実行

1. Kafka Connect を設定して実行します。

    1. Kafka Connect を設定します。**config** ディレクトリの設定ファイル `config/connect-distributed.properties` で、以下のパラメータを設定します。詳細なパラメータと説明については、[Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running) を参照してください。

        ```yaml
        # Kafka ブローカーのアドレス。複数の Kafka ブローカーのアドレスはカンマ（,）で区切る必要があります。
        # この例では、Kafka クラスターにアクセスするためのセキュリティプロトコルとして PLAINTEXT を使用しています。他のセキュリティプロトコルを使用して Kafka クラスターにアクセスする場合は、このファイルに関連情報を設定する必要があります。
        bootstrap.servers=<kafka_broker_ip>:9092
        offset.storage.file.filename=/tmp/connect.offsets
        offset.flush.interval.ms=10000
        key.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        key.converter.schemas.enable=true
        value.converter.schemas.enable=false
        # 解凍後の Kafka コネクタの絶対パス。例：
        plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3
        ```

    2. Kafka Connect を実行します。

        ```BASH
        CLASSPATH=/home/kafka-connect/starrocks-kafka-connector-1.0.3/* bin/connect-distributed.sh config/connect-distributed.properties
        ```

2. Kafka コネクタを設定して作成します。分散モードでは、REST API を通じて Kafka コネクタを設定して作成する必要があります。パラメータと説明については、[Parameters](#Parameters) を参照してください。

    :::info

    - この例では、StarRocks が提供する Kafka コネクタは、Kafka からデータを継続的に消費し、StarRocks にデータをロードできるシンクコネクタです。
    - ソースデータが CDC データ（例えば、Debezium フォーマットのデータ）であり、StarRocks テーブルが主キーテーブルである場合、StarRocks が提供する Kafka コネクタ用の設定ファイル **connect-StarRocks-sink.properties** で [`transform`](#load-debezium-formatted-cdc-data) を設定し、ソースデータの変更を主キーテーブルに同期する必要があります。

    :::

      ```Shell
      curl -i http://127.0.0.1:8083/connectors -H "Content-Type: application/json" -X POST -d '{
        "name":"starrocks-kafka-connector",
        "config":{
          "connector.class":"com.starrocks.connector.kafka.StarRocksSinkConnector",
          "topics":"test",
          "key.converter":"org.apache.kafka.connect.json.JsonConverter",
          "value.converter":"org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable":"true",
          "value.converter.schemas.enable":"false",
          "starrocks.http.url":"192.168.xxx.xxx:8030",
          "starrocks.topic2table.map":"test:test_tbl",
          "starrocks.username":"user1",
          "starrocks.password":"123456",
          "starrocks.database.name":"example_db",
          "sink.properties.strip_outer_array":"true"
        }
      }'
      ```

#### StarRocks テーブルをクエリ

ターゲット StarRocks テーブル `test_tbl` をクエリします。

```mysql
MySQL [example_db]> select * from test_tbl;

+------+-------------+
| id   | city        |
+------+-------------+
|    1 | New York    |
|    2 | Los Angeles |
|    3 | Chicago     |
+------+-------------+
3 rows in set (0.01 sec)
```

上記の結果が返された場合、データは正常にロードされています。

## パラメータ

### name

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: この Kafka コネクタの名前。Kafka Connect クラスター内のすべての Kafka コネクタの中でグローバルにユニークである必要があります。例：starrocks-kafka-connector。

### connector.class

**必須**: YES<br/>
**デフォルト値**: <br/>
**説明**: この Kafka コネクタのシンクで使用されるクラス。値を `com.starrocks.connector.kafka.StarRocksSinkConnector` に設定します。

### topics

**必須**:<br/>
**デフォルト値**:<br/>
**説明**: 購読する1つ以上のトピックで、各トピックは StarRocks テーブルに対応します。デフォルトでは、StarRocks はトピック名が StarRocks テーブル名と一致すると仮定します。そのため、StarRocks はトピック名を使用してターゲットの StarRocks テーブルを決定します。`topics` または `topics.regex`（下記）のいずれかを記入してください。ただし、StarRocks テーブル名がトピック名と異なる場合は、オプションの `starrocks.topic2table.map` パラメータ（下記）を使用してトピック名からテーブル名へのマッピングを指定します。

### topics.regex

**必須**:<br/>
**デフォルト値**:
**説明**: 購読する1つ以上のトピックを一致させる正規表現。詳細については `topics` を参照してください。`topics.regex` または `topics`（上記）のいずれかを記入してください。<br/>

### starrocks.topic2table.map

**必須**: NO<br/>
**デフォルト値**:<br/>
**説明**: トピック名が StarRocks テーブル名と異なる場合の StarRocks テーブル名とトピック名のマッピング。フォーマットは `<topic-1>:<table-1>,<topic-2>:<table-2>,...` です。

### starrocks.http.url

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks クラスター内の FE の HTTP URL。フォーマットは `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>,...` です。複数のアドレスはカンマ（,）で区切ります。例：`192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`。

### starrocks.database.name

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks データベースの名前。

### starrocks.username

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks クラスターアカウントのユーザー名。ユーザーは StarRocks テーブルに対する [INSERT](../sql-reference/sql-statements/account-management/GRANT.md) 権限を持つ必要があります。

### starrocks.password

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks クラスターアカウントのパスワード。

### key.converter

**必須**: NO<br/>
**デフォルト値**: Kafka Connect クラスターで使用されるキーコンバータ<br/>
**説明**: このパラメータはシンクコネクタ（Kafka-connector-starrocks）用のキーコンバータを指定し、Kafka データのキーをデシリアライズするために使用されます。デフォルトのキーコンバータは Kafka Connect クラスターで使用されるものです。

### value.converter

**必須**: NO<br/>
**デフォルト値**: Kafka Connect クラスターで使用される値コンバータ<br/>
**説明**: このパラメータはシンクコネクタ（Kafka-connector-starrocks）用の値コンバータを指定し、Kafka データの値をデシリアライズするために使用されます。デフォルトの値コンバータは Kafka Connect クラスターで使用されるものです。

### key.converter.schema.registry.url

**必須**: NO<br/>
**デフォルト値**:<br/>
**説明**: キーコンバータ用のスキーマレジストリ URL。

### value.converter.schema.registry.url

**必須**: NO<br/>
**デフォルト値**:<br/>
**説明**: 値コンバータ用のスキーマレジストリ URL。

### tasks.max

**必須**: NO<br/>
**デフォルト値**: 1<br/>
**説明**: Kafka コネクタが作成できるタスクスレッドの上限で、通常は Kafka Connect クラスターのワーカーノードの CPU コア数と同じです。このパラメータを調整してロードパフォーマンスを制御できます。

### bufferflush.maxbytes

**必須**: NO<br/>
**デフォルト値**: 94371840(90M)<br/>
**説明**: 一度に StarRocks に送信される前にメモリに蓄積できるデータの最大サイズ。最大値は 64 MB から 10 GB の範囲です。Stream Load SDK バッファはデータをバッファリングするために複数の Stream Load ジョブを作成することがあります。したがって、ここで言及されている閾値は総データサイズを指します。

### bufferflush.intervalms

**必須**: NO<br/>
**デフォルト値**: 1000<br/>
**説明**: データのバッチを送信する間隔で、ロードの遅延を制御します。範囲: [1000, 3600000]。

### connect.timeoutms

**必須**: NO<br/>
**デフォルト値**: 1000<br/>
**説明**: HTTP URL への接続のタイムアウト。範囲: [100, 60000]。

### sink.properties.*

**必須**:<br/>
**デフォルト値**:<br/>
**説明**: ロード動作を制御するための Stream Load パラメータ。例えば、パラメータ `sink.properties.format` は Stream Load に使用されるフォーマットを指定し、CSV または JSON などがあります。サポートされているパラメータとその説明のリストについては、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### sink.properties.format

**必須**: NO<br/>
**デフォルト値**: json<br/>
**説明**: Stream Load に使用されるフォーマット。Kafka コネクタは、データの各バッチを StarRocks に送信する前にフォーマットに変換します。有効な値: `csv` と `json`。詳細については、[CSV パラメータ](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#csv-parameters) および [JSON パラメータ](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#json-parameters) を参照してください。

### sink.properties.partial_update

**必須**:  NO<br/>
**デフォルト値**: `FALSE`<br/>
**説明**: 部分更新を使用するかどうか。有効な値: `TRUE` と `FALSE`。デフォルト値: `FALSE`、この機能を無効にすることを示します。

### sink.properties.partial_update_mode

**必須**:  NO<br/>
**デフォルト値**: `row`<br/>
**説明**: 部分更新のモードを指定します。有効な値: `row` と `column`。<ul><li>値 `row`（デフォルト）は行モードでの部分更新を意味し、多くの列と小さなバッチでのリアルタイム更新に適しています。</li><li>値 `column` は列モードでの部分更新を意味し、少ない列と多くの行でのバッチ更新に適しています。このようなシナリオでは、列モードを有効にすると更新速度が速くなります。例えば、100 列のテーブルで、すべての行に対して 10 列（全体の 10%）のみが更新される場合、列モードの更新速度は 10 倍速くなります。</li></ul>

## 使用上の注意

### フラッシュポリシー

Kafka コネクタはデータをメモリにバッファし、Stream Load を介して StarRocks にバッチでフラッシュします。以下の条件のいずれかが満たされた場合、フラッシュがトリガーされます：

- バッファされた行のバイト数が `bufferflush.maxbytes` の制限に達したとき。
- 最後のフラッシュからの経過時間が `bufferflush.intervalms` の制限に達したとき。
- コネクタがタスクのオフセットをコミットしようとする間隔に達したとき。この間隔は Kafka Connect の設定 [`offset.flush.interval.ms`](https://docs.confluent.io/platform/current/connect/references/allconfigs.html) によって制御され、デフォルト値は `60000` です。

データの遅延を低くするために、これらの設定を Kafka コネクタの設定で調整します。ただし、フラッシュの頻度が増えると CPU と I/O の使用量が増加します。

### 制限事項

- Kafka トピックからの単一メッセージを複数のデータ行にフラット化して StarRocks にロードすることはサポートされていません。
- StarRocks が提供する Kafka コネクタのシンクは少なくとも一度のセマンティクスを保証します。

## ベストプラクティス

### Debezium フォーマットの CDC データのロード

Debezium は、さまざまなデータベースシステムでのデータ変更を監視し、これらの変更を Kafka にストリーミングすることをサポートする人気のある Change Data Capture (CDC) ツールです。以下の例では、PostgreSQL の変更を StarRocks の **主キーテーブル** に書き込むために Kafka コネクタを設定して使用する方法を示します。

#### ステップ 1: Kafka のインストールと開始

> **注意**
>
> 独自の Kafka 環境を持っている場合、このステップをスキップできます。

1. 公式サイトから最新の Kafka リリースを[ダウンロード](https://dlcdn.apache.org/kafka/)し、パッケージを解凍します。

   ```Bash
   tar -xzf kafka_2.13-3.7.0.tgz
   cd kafka_2.13-3.7.0
   ```

2. Kafka 環境を開始します。

   Kafka クラスター UUID を生成します。

   ```Bash
   KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
   ```

   ログディレクトリをフォーマットします。

   ```Bash
   bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
   ```

   Kafka サーバーを開始します。

   ```Bash
   bin/kafka-server-start.sh config/kraft/server.properties
   ```

#### ステップ 2: PostgreSQL の設定

1. PostgreSQL ユーザーに `REPLICATION` 権限が付与されていることを確認します。

2. PostgreSQL の設定を調整します。

   **postgresql.conf** で `wal_level` を `logical` に設定します。

   ```Properties
   wal_level = logical
   ```

   PostgreSQL サーバーを再起動して変更を適用します。

   ```Bash
   pg_ctl restart
   ```

3. データセットを準備します。

   テーブルを作成し、テストデータを挿入します。

   ```SQL
   CREATE TABLE customers (
     id int primary key ,
     first_name varchar(65533) NULL,
     last_name varchar(65533) NULL ,
     email varchar(65533) NULL 
   );

   INSERT INTO customers VALUES (1,'a','a','a@a.com');
   ```

4. Kafka で CDC ログメッセージを確認します。

    ```Json
    {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "int32",
                            "optional": false,
                            "field": "id"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "first_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "last_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "email"
                        }
                    ],
                    "optional": true,
                    "name": "test.public.customers.Value",
                    "field": "before"
                },
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "int32",
                            "optional": false,
                            "field": "id"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "first_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "last_name"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "email"
                        }
                    ],
                    "optional": true,
                    "name": "test.public.customers.Value",
                    "field": "after"
                },
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "string",
                            "optional": false,
                            "field": "version"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "connector"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "name"
                        },
                        {
                            "type": "int64",
                            "optional": false,
                            "field": "ts_ms"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "name": "io.debezium.data.Enum",
                            "version": 1,
                            "parameters": {
                                "allowed": "true,last,false,incremental"
                            },
                            "default": "false",
                            "field": "snapshot"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "db"
                        },
                        {
                            "type": "string",
                            "optional": true,
                            "field": "sequence"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "schema"
                        },
                        {
                            "type": "string",
                            "optional": false,
                            "field": "table"
                        },
                        {
                            "type": "int64",
                            "optional": true,
                            "field": "txId"
                        },
                        {
                            "type": "int64",
                            "optional": true,
                            "field": "lsn"
                        },
                        {
                            "type": "int64",
                            "optional": true,
                            "field": "xmin"
                        }
                    ],
                    "optional": false,
                    "name": "io.debezium.connector.postgresql.Source",
                    "field": "source"
                },
                {
                    "type": "string",
                    "optional": false,
                    "field": "op"
                },
                {
                    "type": "int64",
                    "optional": true,
                    "field": "ts_ms"
                },
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "string",
                            "optional": false,
                            "field": "id"
                        },
                        {
                            "type": "int64",
                            "optional": false,
                            "field": "total_order"
                        },
                        {
                            "type": "int64",
                            "optional": false,
                            "field": "data_collection_order"
                        }
                    ],
                    "optional": true,
                    "name": "event.block",
                    "version": 1,
                    "field": "transaction"
                }
            ],
            "optional": false,
            "name": "test.public.customers.Envelope",
            "version": 1
        },
        "payload": {
            "before": null,
            "after": {
                "id": 1,
                "first_name": "a",
                "last_name": "a",
                "email": "a@a.com"
            },
            "source": {
                "version": "2.5.3.Final",
                "connector": "postgresql",
                "name": "test",
                "ts_ms": 1714283798721,
                "snapshot": "false",
                "db": "postgres",
                "sequence": "[\"22910216\",\"22910504\"]",
                "schema": "public",
                "table": "customers",
                "txId": 756,
                "lsn": 22910504,
                "xmin": null
            },
            "op": "c",
            "ts_ms": 1714283798790,
            "transaction": null
        }
    }
    ```

#### ステップ 3: StarRocks の設定

PostgreSQL のソーステーブルと同じスキーマを持つ主キーテーブルを StarRocks に作成します。

```SQL
CREATE TABLE `customers` (
  `id` int(11) COMMENT "",
  `first_name` varchar(65533) NULL COMMENT "",
  `last_name` varchar(65533) NULL COMMENT "",
  `email` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id`) 
DISTRIBUTED BY hash(id) buckets 1
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);
```

#### ステップ 4: コネクタのインストール

1. コネクタをダウンロードし、**plugins** ディレクトリにパッケージを解凍します。

   ```Bash
   mkdir plugins
   tar -zxvf debezium-debezium-connector-postgresql-2.5.3.zip -C plugins
   tar -zxvf starrocks-kafka-connector-1.0.3.tar.gz -C plugins
   ```

   このディレクトリは、**config/connect-standalone.properties** の設定項目 `plugin.path` の値です。

   ```Properties
   plugin.path=/path/to/kafka_2.13-3.7.0/plugins
   ```

2. **pg-source.properties** で PostgreSQL ソースコネクタを設定します。

   ```Json
   {
     "name": "inventory-connector",
     "config": {
       "connector.class": "io.debezium.connector.postgresql.PostgresConnector", 
       "plugin.name": "pgoutput",
       "database.hostname": "localhost", 
       "database.port": "5432", 
       "database.user": "postgres", 
       "database.password": "", 
       "database.dbname" : "postgres", 
       "topic.prefix": "test"
     }
   }
   ```

3. **sr-sink.properties** で StarRocks シンクコネクタを設定します。

   ```Json
   {
       "name": "starrocks-kafka-connector",
       "config": {
           "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
           "tasks.max": "1",
           "topics": "test.public.customers",
           "starrocks.http.url": "172.26.195.69:28030",
           "starrocks.database.name": "test",
           "starrocks.username": "root",
           "starrocks.password": "StarRocks@123",
           "sink.properties.strip_outer_array": "true",
           "connect.timeoutms": "3000",
           "starrocks.topic2table.map": "test.public.customers:customers",
           "transforms": "addfield,unwrap",
           "transforms.addfield.type": "com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord",
           "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
           "transforms.unwrap.drop.tombstones": "true",
           "transforms.unwrap.delete.handling.mode": "rewrite"
       }
   }
   ```

   > **注意**
   >
   > - StarRocks テーブルが主キーテーブルでない場合、`addfield` トランスフォームを指定する必要はありません。
   > - unwrap トランスフォームは Debezium によって提供され、操作タイプに基づいて Debezium の複雑なデータ構造をアンラップするために使用されます。詳細については、[New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html) を参照してください。

4. Kafka Connect を設定します。

   Kafka Connect 設定ファイル **config/connect-standalone.properties** で以下の設定項目を設定します。

   ```Properties
   # Kafka ブローカーのアドレス。複数の Kafka ブローカーのアドレスはカンマ（,）で区切る必要があります。
   # この例では、Kafka クラスターにアクセスするためのセキュリティプロトコルとして PLAINTEXT を使用しています。
   # 他のセキュリティプロトコルを使用して Kafka クラスターにアクセスする場合は、この部分に関連情報を設定します。

   bootstrap.servers=<kafka_broker_ip>:9092
   offset.storage.file.filename=/tmp/connect.offsets
   key.converter=org.apache.kafka.connect.json.JsonConverter
   value.converter=org.apache.kafka.connect.json.JsonConverter
   key.converter.schemas.enable=true
   value.converter.schemas.enable=false

   # 解凍後の starrocks-kafka-connector の絶対パス。例：
   plugin.path=/home/kafka-connect/starrocks-kafka-connector-1.0.3

   # フラッシュポリシーを制御するパラメータ。詳細については、使用上の注意セクションを参照してください。
   offset.flush.interval.ms=10000
   bufferflush.maxbytes = xxx
   bufferflush.intervalms = xxx
   ```

   詳細なパラメータの説明については、[Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running) を参照してください。

#### ステップ 5: スタンドアロンモードで Kafka Connect を開始

スタンドアロンモードで Kafka Connect を実行してコネクタを起動します。

```Bash
bin/connect-standalone.sh config/connect-standalone.properties config/pg-source.properties config/sr-sink.properties 
```

#### ステップ 6: データ取り込みの確認

以下の操作をテストし、データが正しく StarRocks に取り込まれていることを確認します。

##### INSERT

- PostgreSQL で:

```Plain
postgres=# insert into customers values (2,'b','b','b@b.com');
INSERT 0 1
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  1 | a          | a         | a@a.com
  2 | b          | b         | b@b.com
(2 rows)
```

- StarRocks で:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    1 | a          | a         | a@a.com |
|    2 | b          | b         | b@b.com |
+------+------------+-----------+---------+
2 rows in set (0.01 sec)
```

##### UPDATE

- PostgreSQL で:

```Plain
postgres=# update customers set email='c@c.com';
UPDATE 2
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  1 | a          | a         | c@c.com
  2 | b          | b         | c@c.com
(2 rows)
```

- StarRocks で:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    1 | a          | a         | c@c.com |
|    2 | b          | b         | c@c.com |
+------+------------+-----------+---------+
2 rows in set (0.00 sec)
```

##### DELETE

- PostgreSQL で:

```Plain
postgres=# delete from customers where id=1;
DELETE 1
postgres=# select * from customers;
 id | first_name | last_name |  email  
----+------------+-----------+---------
  2 | b          | b         | c@c.com
(1 row)
```

- StarRocks で:

```Plain
MySQL [test]> select * from customers;
+------+------------+-----------+---------+
| id   | first_name | last_name | email   |
+------+------------+-----------+---------+
|    2 | b          | b         | c@c.com |
+------+------------+-----------+---------+
1 row in set (0.00 sec)
```