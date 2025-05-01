---
displayed_sidebar: docs
---

# Kafka コネクタを使用してデータをロードする

StarRocks は、Apache Kafka® コネクタ（StarRocks Connector for Apache Kafka®、以下 Kafka コネクタ）という自社開発のコネクタを提供しています。このコネクタはシンクコネクタとして、Kafka からメッセージを継続的に消費し、それを StarRocks にロードします。Kafka コネクタは少なくとも一度のセマンティクスを保証します。

Kafka コネクタは Kafka Connect とシームレスに統合でき、StarRocks が Kafka エコシステムとより良く統合されることを可能にします。リアルタイムデータを StarRocks にロードしたい場合には賢明な選択です。Routine Load と比較して、以下のシナリオでは Kafka コネクタの使用が推奨されます：

- Routine Load は CSV、JSON、Avro フォーマットのデータのみをサポートしていますが、Kafka コネクタは Protobuf など、より多くのフォーマットのデータをロードできます。Kafka Connect のコンバータを使用してデータを JSON や CSV フォーマットに変換できる限り、Kafka コネクタを通じて StarRocks にデータをロードできます。
- Debezium フォーマットの CDC データなど、データ変換をカスタマイズする。
- 複数の Kafka トピックからデータをロードする。
- Confluent Cloud からデータをロードする。
- ロードバッチサイズ、並行性、その他のパラメータを細かく制御して、ロード速度とリソース利用のバランスを取る必要がある。

## 準備

### Kafka 環境のセットアップ

自己管理型の Apache Kafka クラスターと Confluent Cloud の両方がサポートされています。

- 自己管理型の Apache Kafka クラスターの場合、[Apache Kafka クイックスタート](https://kafka.apache.org/quickstart) を参照して、Kafka クラスターを迅速にデプロイできます。Kafka Connect はすでに Kafka に統合されています。
- Confluent Cloud の場合、Confluent アカウントを持ち、クラスターを作成していることを確認してください。

### Kafka コネクタのダウンロード

Kafka コネクタを Kafka Connect に提出します：

- 自己管理型 Kafka クラスター：

  [starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases) をダウンロードして解凍します。

- Confluent Cloud：

  現在、Kafka コネクタは Confluent Hub にアップロードされていません。[starrocks-kafka-connector-xxx.tar.gz](https://github.com/StarRocks/starrocks-connector-for-kafka/releases) をダウンロードして解凍し、ZIP ファイルにパッケージして Confluent Cloud にアップロードする必要があります。

## 使用方法

このセクションでは、自己管理型 Kafka クラスターを例にとり、Kafka コネクタと Kafka Connect の設定方法を説明し、その後 Kafka Connect を実行して StarRocks にデータをロードします。

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

    - この例では、StarRocks が提供する Kafka コネクタはシンクコネクタであり、Kafka からデータを継続的に消費し、StarRocks にデータをロードできます。
    - ソースデータが CDC データ（例えば Debezium フォーマットのデータ）であり、StarRocks テーブルが主キーテーブルの場合、StarRocks が提供する Kafka コネクタ用の設定ファイル **connect-StarRocks-sink.properties** で [transform を設定](#load-debezium-formatted-cdc-data) し、ソースデータの変更を主キーテーブルに同期する必要があります。

    :::

    ```yaml
    name=starrocks-kafka-connector
    connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
    topics=test
    key.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable=true
    value.converter.schemas.enable=false
    # StarRocks クラスター内の FE の HTTP URL。デフォルトのポートは 8030 です。
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

   1. Kafka Connect を設定します。**config** ディレクトリ内の設定ファイル **config/connect-standalone.properties** で以下のパラメータを設定します。詳細なパラメータと説明については、[Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running) を参照してください。

        ```yaml
        # Kafka ブローカーのアドレス。複数の Kafka ブローカーのアドレスはカンマ (,) で区切る必要があります。
        # この例では、Kafka クラスターにアクセスするためのセキュリティプロトコルとして PLAINTEXT を使用しています。他のセキュリティプロトコルを使用して Kafka クラスターにアクセスする場合、このファイルに関連情報を設定する必要があります。
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
    
    1. Kafka Connect を設定します。**config** ディレクトリ内の設定ファイル `config/connect-distributed.properties` で以下のパラメータを設定します。詳細なパラメータと説明については、[Running Kafka Connect](https://kafka.apache.org/documentation.html#connect_running) を参照してください。

        ```yaml
        # Kafka ブローカーのアドレス。複数の Kafka ブローカーのアドレスはカンマ (,) で区切る必要があります。
        # この例では、Kafka クラスターにアクセスするためのセキュリティプロトコルとして PLAINTEXT を使用しています。他のセキュリティプロトコルを使用して Kafka クラスターにアクセスする場合、このファイルに関連情報を設定する必要があります。
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

    - この例では、StarRocks が提供する Kafka コネクタはシンクコネクタであり、Kafka からデータを継続的に消費し、StarRocks にデータをロードできます。
    - ソースデータが CDC データ（例えば Debezium フォーマットのデータ）であり、StarRocks テーブルが主キーテーブルの場合、StarRocks が提供する Kafka コネクタ用の設定ファイル **connect-StarRocks-sink.properties** で [transform を設定](#load-debezium-formatted-cdc-data) し、ソースデータの変更を主キーテーブルに同期する必要があります。

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
**説明**: この Kafka コネクタの名前。Kafka Connect クラスター内のすべての Kafka コネクタ間でグローバルに一意である必要があります。例：starrocks-kafka-connector。

### connector.class                     

**必須**: YES<br/>
**デフォルト値**: <br/>
**説明**: この Kafka コネクタのシンクで使用されるクラス。値を `com.starrocks.connector.kafka.StarRocksSinkConnector` に設定します。
### topics                              

**必須**:<br/>
**デフォルト値**:<br/>
**説明**: 購読する1つ以上のトピック。各トピックは StarRocks テーブルに対応します。デフォルトでは、StarRocks はトピック名が StarRocks テーブル名と一致すると仮定します。そのため、StarRocks はトピック名を使用してターゲットの StarRocks テーブルを決定します。`topics` または `topics.regex`（下記）のいずれかを選択して記入してください。ただし、StarRocks テーブル名がトピック名と異なる場合は、オプションの `starrocks.topic2table.map` パラメータ（下記）を使用してトピック名からテーブル名へのマッピングを指定してください。

### topics.regex                        

**必須**:<br/>
**デフォルト値**:
**説明**: 購読する1つ以上のトピックに一致する正規表現。詳細については `topics` を参照してください。`topics.regex` または `topics`（上記）のいずれかを選択して記入してください。<br/>

### starrocks.topic2table.map           

**必須**: NO<br/>
**デフォルト値**:<br/>
**説明**: トピック名が StarRocks テーブル名と異なる場合の StarRocks テーブル名とトピック名のマッピング。フォーマットは `<topic-1>:<table-1>,<topic-2>:<table-2>,...`。

### starrocks.http.url                  

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks クラスター内の FE の HTTP URL。フォーマットは `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>,...`。複数のアドレスはカンマ (,) で区切ります。例：`192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`。

### starrocks.database.name             

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks データベースの名前。

### starrocks.username                  

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks クラスターアカウントのユーザー名。ユーザーは StarRocks テーブルに対する [INSERT](../sql-reference/sql-statements/account-management/GRANT.md) 権限を持っている必要があります。

### starrocks.password                  

**必須**: YES<br/>
**デフォルト値**:<br/>
**説明**: StarRocks クラスターアカウントのパスワード。

### key.converter                       

**必須**: NO<br/>
**デフォルト値**: Kafka Connect クラスターで使用されるキーコンバータ<br/>
**説明**: このパラメータは、シンクコネクタ（Kafka-connector-starrocks）用のキーコンバータを指定します。これは Kafka データのキーをデシリアライズするために使用されます。デフォルトのキーコンバータは Kafka Connect クラスターで使用されるものです。

### value.converter                     

**必須**: NO<br/>
**デフォルト値**: Kafka Connect クラスターで使用される値コンバータ<br/>
**説明**: このパラメータは、シンクコネクタ（Kafka-connector-starrocks）用の値コンバータを指定します。これは Kafka データの値をデシリアライズするために使用されます。デフォルトの値コンバータは Kafka Connect クラスターで使用されるものです。

### key.converter.schema.registry.url   

**必須**: NO<br/>
**デフォルト値**:<br/>
**説明**: キーコンバータのスキーマレジストリ URL。

### value.converter.schema.registry.url 

**必須**: NO<br/>
**デフォルト値**:<br/>
**説明**: 値コンバータのスキーマレジストリ URL。

### tasks.max                           

**必須**: NO<br/>
**デフォルト値**: 1<br/>
**説明**: Kafka コネクタが作成できるタスクスレッドの上限で、通常は Kafka Connect クラスター内のワーカーノードの CPU コア数と同じです。このパラメータを調整してロードパフォーマンスを制御できます。

### bufferflush.maxbytes                

**必須**: NO<br/>
**デフォルト値**: 94371840(90M)<br/>
**説明**: 一度に StarRocks に送信される前にメモリに蓄積できるデータの最大サイズ。最大値は 64 MB から 10 GB の範囲です。Stream Load SDK バッファはデータをバッファするために複数の Stream Load ジョブを作成する可能性があることを覚えておいてください。したがって、ここで言及されているしきい値は、総データサイズを指します。

### bufferflush.intervalms

**必須**: NO<br/>
**デフォルト値**: 300000<br/>
**説明**: データのバッチを送信する間隔で、ロードの遅延を制御します。範囲：[1000, 3600000]。

### connect.timeoutms

**必須**: NO<br/>
**デフォルト値**: 1000<br/>
**説明**: HTTP URL への接続のタイムアウト。範囲：[100, 60000]。

### sink.properties.*

**必須**:<br/>
**デフォルト値**:<br/>
**説明**: ロード動作を制御するための Stream Load パラメータ。例えば、パラメータ `sink.properties.format` は Stream Load に使用されるフォーマットを指定し、CSV や JSON などがあります。サポートされているパラメータとその説明のリストについては、[STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM LOAD.md) を参照してください。

### sink.properties.format

**必須**: NO<br/>
**デフォルト値**: json<br/>
**説明**: Stream Load に使用されるフォーマット。Kafka コネクタは、各バッチのデータを StarRocks に送信する前にこのフォーマットに変換します。有効な値は `csv` と `json` です。詳細については、[CSV parameters](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#csv-parameters) および [JSON parameters](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#json-parameters) を参照してください。

## 制限事項

- Kafka トピックからの単一メッセージを複数のデータ行にフラット化して StarRocks にロードすることはサポートされていません。
- StarRocks が提供する Kafka コネクタのシンクは、少なくとも一度のセマンティクスを保証します。

## ベストプラクティス

### Debezium フォーマットの CDC データをロードする

Kafka データが Debezium CDC フォーマットであり、StarRocks テーブルが主キーテーブルの場合、StarRocks が提供する Kafka コネクタ用の設定ファイル **connect-StarRocks-sink.properties** で `transforms` パラメータとその他の関連パラメータを設定する必要があります。

:::info

この例では、StarRocks が提供する Kafka コネクタはシンクコネクタであり、Kafka からデータを継続的に消費し、StarRocks にデータをロードできます。

:::

```Properties
transforms=addfield,unwrap
transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=true
transforms.unwrap.delete.handling.mode
```

上記の設定では、`transforms=addfield,unwrap` を指定しています。

- Debezium フォーマットの CDC データの `op` フィールドは、上流データベースからの各データ行に対する SQL 操作を記録します。値 `c`、`u`、`d` はそれぞれ作成、更新、削除を表します。StarRocks テーブルが主キーテーブルの場合、addfield トランスフォームを指定する必要があります。addfield トランスフォームは、各データ行に `__op` フィールドを追加し、各データ行に対する SQL 操作をマークします。完全なデータ行を形成するために、addfield トランスフォームは、Debezium フォーマットの CDC データの `op` フィールドの値に基づいて、`before` または `after` フィールドから他の列の値を取得します。最終的に、データは JSON または CSV フォーマットに変換され、StarRocks に書き込まれます。addfield トランスフォームクラスは `com.Starrocks.Kafka.Transforms.AddOpFieldForDebeziumRecord` です。これは Kafka コネクタの JAR ファイルに含まれているため、手動でインストールする必要はありません。

    StarRocks テーブルが主キーテーブルでない場合、addfield トランスフォームを指定する必要はありません。

- unwrap トランスフォームは Debezium によって提供され、操作タイプに基づいて Debezium の複雑なデータ構造をアンラップするために使用されます。詳細については、[New Record State Extraction](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html) を参照してください。