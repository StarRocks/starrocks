---
description: Kafka Routine Load と共有データストレージ
displayed_sidebar: docs
---

# Kafkaから共有データクラスタにデータをロードする

import Clients from '../_assets/quick-start/_clientsCompose.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'

## Routine Loadについて

Routine Loadは、Apache Kafka、またはこのラボではRedpandaを使用して、データをStarRocksに継続的にストリーミングする方法です。データはKafkaトピックにストリームされ、Routine LoadジョブがそのデータをStarRocksに取り込みます。Routine Loadの詳細は、ラボの最後に提供されています。

## 共有データモードについて

ストレージと計算を分離するアーキテクチャは、データはAmazon S3、Google Cloud Storage、Azure Blob Storage、その他のS3互換ストレージ（MinIOなど）のような低コストで信頼性のある遠端ストレージシステムに保存されます。ホットデータはローカルディスクでキャッシュされ、キャッシュがヒットすると、クエリパフォーマンスはストレージと計算が結合されたアーキテクチャと同等になります。計算节点（Compute Node, CN）は、数秒以内にオンデマンドで追加または削除できます。このアーキテクチャは、ストレージコストを削減し、リソースの分離を改善し、弾力性と拡張性を提供します。

このチュートリアルでは以下をカバーします：

- Docker Composeを使用してStarRocks、Redpanda、MinIOを実行する
- MinIOをStarRocksのストレージ層として使用する
- StarRocksを共有データモード用に設定する
- Redpandaからデータを取り込むためのRoutine Loadジョブを追加する

使用されるデータは合成データです。

このドキュメントには多くの情報が含まれており、最初にステップバイステップの内容が提示され、技術的な詳細が最後に示されています。これは次の目的を順に果たすためです：

1. Routine Loadを設定する。
2. 共有データクラスタにデータをロードし、そのデータを分析する。
3. 共有データデプロイメントの設定詳細を提供する。

---

## 前提条件

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- Dockerに割り当てられた 4 GB のRAM
- Dockerに割り当てられた 10 GB の空きディスクスペース

### SQLクライアント

Docker環境で提供されるSQLクライアントを使用するか、システム上のものを使用できます。多くのMySQL互換クライアントが動作し、このガイドではDBeaverとMySQL Workbenchの設定をカバーしています。

### curl

`curl`はComposeファイルとデータを生成するスクリプトをダウンロードするために使用されます。OSプロンプトで`curl`または`curl.exe`を実行してインストールされているか確認してください。curlがインストールされていない場合は、[こちらからcurlを入手してください](https://curl.se/dlwiz/?type=bin)。

### Python

Python 3 とApache Kafka用のPythonクライアント`kafka-python`が必要です。

- [Python](https://www.python.org/)
- [`kafka-python`](https://pypi.org/project/kafka-python/)

---

## StarRocks 用語

### FE

フロントエンドノードは、メタデータ管理、クライアント接続管理、クエリプランニング、およびクエリスケジューリングを担当します。各FEはメモリ内にメタデータの完全なコピーを保存および維持し、FE間での無差別なサービスを保証します。

### CN

計算节点は、共有データデプロイメントでクエリプランを実行する役割を担います。

### BE

バックエンドノードは、共有なしデプロイメントでデータストレージとクエリプランの実行の両方を担当します。

:::note
このガイドではBEを使用しませんが、BEとCNの違いを理解するためにこの情報を含めています。
:::

---

## StarRocksを立ち上げる

オブジェクトストレージを使用して共有データでStarRocksを実行するには、以下が必要です：

- フロントエンドエンジン（FE）
- 計算节点（CN）
- オブジェクトストレージ

このガイドでは、S3互換のオブジェクトストレージプロバイダーであるMinIOを使用します。MinIOはGNU Affero General Public Licenseの下で提供されています。

### ラボファイルをダウンロード

#### `docker-compose.yml`

```bash
mkdir routineload
cd routineload
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/routine-load-shared-data/docker-compose.yml
```

#### `gen.py`

`gen.py`は、Apache Kafka用のPythonクライアントを使用してデータをKafkaトピックに公開（生成）するスクリプトです。このスクリプトはRedpandaコンテナのアドレスとポートで記述されています。

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/routine-load-shared-data/gen.py
```

## StarRocks、MinIO、Redpandaをスタート

```bash
docker compose up --detach --wait --wait-timeout 120
```

サービスの進行状況を確認します。コンテナが正常になるまで30秒以上かかる場合があります。`routineload-minio_mc-1`コンテナは健康指標を表示せず、StarRocksが使用するアクセスキーでMinIOを設定し終わると終了します。`routineload-minio_mc-1`が`0`コードで終了し、他のサービスが`Healthy`になるのを待ちます。

サービスが正常になるまで`docker compose ps`を実行します：

```bash
docker compose ps
```

```plaintext
WARN[0000] /Users/droscign/routineload/docker-compose.yml: `version` is obsolete
[+] Running 6/7
 ✔ Network routineload_default       Crea...          0.0s
 ✔ Container minio                   Healthy          5.6s
 ✔ Container redpanda                Healthy          3.6s
 ✔ Container redpanda-console        Healt...         1.1s
 ⠧ Container routineload-minio_mc-1  Waiting          23.1s
 ✔ Container starrocks-fe            Healthy          11.1s
 ✔ Container starrocks-cn            Healthy          23.0s
container routineload-minio_mc-1 exited (0)
```

---

## MinIOのクレデンシャルを確認

StarRocksでオブジェクトストレージとしてMinIOを使用するためには、StarRocksがMinIOアクセスキーを必要とします。アクセスキーはDockerサービスの起動時に生成されました。StarRocksがMinIOに接続する方法をよりよく理解するために、キーが存在することを確認してください。

### MinIOのWeb UIを起動

http://localhost:9001/access-keys にアクセスします。ユーザー名とパスワードはDocker composeファイルに指定されており、`miniouser`と`miniopassword`です。1つのアクセスキーがあることが確認できるはずです。キーは`AAAAAAAAAAAAAAAAAAAA`で、MinIOコンソールではシークレットは表示されませんが、Docker composeファイルにあり、`BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB`です：

![View the MinIO access key](../_assets/quick-start/MinIO-view-key.png)

---

## SQLクライアント

<Clients />
---

## 共有データクラスタの設定

この時点で、StarRocks、Redpanda、およびMinIOが実行されています。MinIOアクセスキーはStarRocksとMinioを接続するために使用されます。StarRocksが起動すると、MinIOとの接続が確立され、MinIOにデフォルトのストレージボリュームが作成されました。

これは、MinIOを使用するためにデフォルトのストレージボリュームを設定するために使用される設定です（これもDocker composeファイルにあります）。設定はこのガイドの最後で詳細に説明されますが、今は`aws_s3_access_key`がMinIOコンソールで見た文字列に設定されていることと、`run_mode`が`shared_data`に設定されていることに注意してください。

```plaintext
#highlight-start
# 共有データ、ストレージタイプ、エンドポイントを設定
run_mode = shared_data
#highlight-end
cloud_native_storage_type = S3
aws_s3_endpoint = minio:9000

# minIOのパスを設定
aws_s3_path = starrocks

#highlight-start
# MinIOオブジェクトの読み書きに関するクレデンシャル
aws_s3_access_key = AAAAAAAAAAAAAAAAAAAA
aws_s3_secret_key = BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
#highlight-end
aws_s3_use_instance_profile = false
aws_s3_use_aws_sdk_default_behavior = false

# オブジェクト=ストレージにデフォルトストレージを作成したくない場合は、falseに設定する。
enable_load_volume_from_conf = true
```

:::tip

完全な設定ファイルを表示するには、このコマンドを実行できます：

```bash
docker compose exec starrocks-fe cat fe/conf/fe.conf
```

すべての`docker compose`コマンドは`docker-compose.yml`ファイルを含むディレクトリから実行してください。

:::

### SQLクライアントで StarRocks に接続

:::tip

このコマンドは`docker-compose.yml`ファイルを含むディレクトリから実行してください。

mysql CLI以外のクライアントを使用している場合は、今すぐ開いてください。
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

#### ストレージボリュームを確認

```sql
SHOW STORAGE VOLUMES;
```

```plaintext
+------------------------+
| Storage Volume         |
+------------------------+
| builtin_storage_volume |
+------------------------+
1 row in set (0.00 sec)
```

```sql
DESC STORAGE VOLUME builtin_storage_volume\G
```

:::tip
このドキュメントの一部のSQL、およびStarRocksドキュメントの多くの他のドキュメントでは、`;`の代わりに`\G`で終わります。`\G`はmysql CLIにクエリ結果を縦に表示させます。

多くのSQLクライアントは縦のフォーマット出力を解釈しないため、`\G`を`;`に置き換える必要があります。
:::

```plaintext
*************************** 1. row ***************************
     Name: builtin_storage_volume
     Type: S3
IsDefault: true
#highlight-start
 Location: s3://starrocks
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"","aws.s3.use_instance_profile":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
#highlight-end
  Enabled: true
  Comment:
1 row in set (0.03 sec)
```

パラメータが設定と一致していることを確認してください。

:::note
フォルダ`builtin_storage_volume`は、バケットにデータが書き込まれるまでMinIOオブジェクトリストに表示されません。
:::

---

## テーブルを作る

これらのSQLコマンドはSQLクライアントで実行されます。

```SQL
CREATE DATABASE quickstart;
```

```SQL
USE quickstart;
```

```SQL
CREATE TABLE site_clicks (
    `uid` bigint NOT NULL COMMENT "uid",
    `site` string NOT NULL COMMENT "site url",
    `vtime` bigint NOT NULL COMMENT "vtime"
)
DISTRIBUTED BY HASH(`uid`)
PROPERTIES("replication_num"="1");
```

---

### Redpandaコンソールを開く

まだトピックはありませんが、次のステップでトピックが作成されます。

http://localhost:8080/overview

### Redpandaトピックにデータを公開

`routineload/`フォルダのコマンドシェルからこのコマンドを実行してデータを生成します：

```python
python gen.py 5
```

:::tip

システムによっては、コマンドで`python`の代わりに`python3`を使用する必要があるかもしれません。

`kafka-python`がない場合は、次を試してください：

```
pip install kafka-python
```
または

```
pip3 install kafka-python
```

:::

```plaintext
b'{ "uid": 6926, "site": "https://docs.starrocks.io/", "vtime": 1718034793 } '
b'{ "uid": 3303, "site": "https://www.starrocks.io/product/community", "vtime": 1718034793 } '
b'{ "uid": 227, "site": "https://docs.starrocks.io/", "vtime": 1718034243 } '
b'{ "uid": 7273, "site": "https://docs.starrocks.io/", "vtime": 1718034794 } '
b'{ "uid": 4666, "site": "https://www.starrocks.io/", "vtime": 1718034794 } '
```

### Redpandaコンソールで確認

Redpandaコンソールで http://localhost:8080/topics に移動すると、`test2`という名前のトピックが1つ表示されます。そのトピックを選択し、**Messages**タブを選択すると、`gen.py`の出力に一致する5つのメッセージが表示されます。

## メッセージを消費

StarRocksでは、Routine Loadジョブを作成して以下を行います：

1. Redpandaトピック`test2`からメッセージを取り込む
2. そのメッセージをテーブル`site_clicks`にロードする

StarRocksはMinIOをストレージとして使用するように設定されているため、`site_clicks`テーブルに挿入されたデータはMinIOに保存されます。

### Routine Loadジョブを作る

SQLクライアントでこのコマンドを実行してRoutine Loadジョブを作成します。このコマンドはラボの最後で詳細に説明されます。

```SQL
CREATE ROUTINE LOAD quickstart.clicks ON site_clicks
PROPERTIES
(
    "format" = "JSON",
    "jsonpaths" ="[\"$.uid\",\"$.site\",\"$.vtime\"]"
)
FROM KAFKA
(     
    "kafka_broker_list" = "redpanda:29092",
    "kafka_topic" = "test2",
    "kafka_partitions" = "0",
    "kafka_offsets" = "OFFSET_BEGINNING"
);
```

### Routine Loadジョブを確認

```SQL
SHOW ROUTINE LOAD\G
```

3つのハイライトされた行を確認してください：

1. 状態が`RUNNING`であること
2. トピックが`test2`であり、ブローカーが`redpanda:2092`であること
3. 統計が、`SHOW ROUTINE LOAD`コマンドを実行したタイミングに応じて、0または5のロードされた行を示していること。0行の場合は再度実行してください。

```SQL
*************************** 1. row ***************************
                  Id: 10078
                Name: clicks
          CreateTime: 2024-06-12 15:51:12
           PauseTime: NULL
             EndTime: NULL
              DbName: quickstart
           TableName: site_clicks
           -- highlight-next-line
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","partial_update":"false","columnToColumnExpr":"*","maxBatchIntervalS":"10","partial_update_mode":"null","whereExpr":"*","dataFormat":"json","timezone":"Etc/UTC","format":"json","log_rejected_record_num":"0","taskTimeoutSecond":"60","json_root":"","maxFilterRatio":"1.0","strict_mode":"false","jsonpaths":"[\"$.uid\",\"$.site\",\"$.vtime\"]","taskConsumeSecond":"15","desireTaskConcurrentNum":"5","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
       -- highlight-next-line
DataSourceProperties: {"topic":"test2","currentKafkaPartitions":"0","brokerList":"redpanda:29092"}
    CustomProperties: {"group.id":"clicks_ea38a713-5a0f-4abe-9b11-ff4a241ccbbd"}
    -- highlight-next-line
           Statistic: {"receivedBytes":0,"errorRows":0,"committedTaskNum":0,"loadedRows":0,"loadRowsRate":0,"abortedTaskNum":0,"totalRows":0,"unselectedRows":0,"receivedBytesRate":0,"taskExecuteTimeMs":1}
            Progress: {"0":"OFFSET_ZERO"}
   TimestampProgress: {}
ReasonOfStateChanged:
        ErrorLogUrls:
         TrackingSQL:
            OtherMsg:
LatestSourcePosition: {}
1 row in set (0.00 sec)
```

```SQL
SHOW ROUTINE LOAD\G
```

```SQL
*************************** 1. row ***************************
                  Id: 10076
                Name: clicks
          CreateTime: 2024-06-12 18:40:53
           PauseTime: NULL
             EndTime: NULL
              DbName: quickstart
           TableName: site_clicks
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","partial_update":"false","columnToColumnExpr":"*","maxBatchIntervalS":"10","partial_update_mode":"null","whereExpr":"*","dataFormat":"json","timezone":"Etc/UTC","format":"json","log_rejected_record_num":"0","taskTimeoutSecond":"60","json_root":"","maxFilterRatio":"1.0","strict_mode":"false","jsonpaths":"[\"$.uid\",\"$.site\",\"$.vtime\"]","taskConsumeSecond":"15","desireTaskConcurrentNum":"5","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","maxBatchRows":"200000"}
DataSourceProperties: {"topic":"test2","currentKafkaPartitions":"0","brokerList":"redpanda:29092"}
    CustomProperties: {"group.id":"clicks_a9426fee-45bb-403a-a1a3-b3bc6c7aa685"}
               -- highlight-next-line
           Statistic: {"receivedBytes":372,"errorRows":0,"committedTaskNum":1,"loadedRows":5,"loadRowsRate":0,"abortedTaskNum":0,"totalRows":5,"unselectedRows":0,"receivedBytesRate":0,"taskExecuteTimeMs":519}
            Progress: {"0":"4"}
   TimestampProgress: {"0":"1718217035111"}
ReasonOfStateChanged:
        ErrorLogUrls:
         TrackingSQL:
            OtherMsg:
                       -- highlight-next-line
LatestSourcePosition: {"0":"5"}
1 row in set (0.00 sec)
```

---

## データがMinIOに保存されていることを確認

MinIOを開き、`starrocks`の下にオブジェクトが保存されていることを確認します。

---

## StarRocksからデータをクエリ

```SQL
USE quickstart;
SELECT * FROM site_clicks;
```

```SQL
+------+--------------------------------------------+------------+
| uid  | site                                       | vtime      |
+------+--------------------------------------------+------------+
| 4607 | https://www.starrocks.io/blog              | 1718031441 |
| 1575 | https://www.starrocks.io/                  | 1718031523 |
| 2398 | https://docs.starrocks.io/                 | 1718033630 |
| 3741 | https://www.starrocks.io/product/community | 1718030845 |
| 4792 | https://www.starrocks.io/                  | 1718033413 |
+------+--------------------------------------------+------------+
5 rows in set (0.07 sec)
```

## 追加データを公開

`gen.py`を再度実行すると、Redpandaにさらに5つのレコードが公開されます。

```bash
python gen.py 5
```

### データが追加されていることを確認

Routine Loadジョブはスケジュールに基づいて実行されるため（デフォルトでは10秒ごと）、データは数秒以内にロードされます。

```SQL
SELECT * FROM site_clicks;
```

```Plaintext
+------+--------------------------------------------+------------+
| uid  | site                                       | vtime      |
+------+--------------------------------------------+------------+
| 6648 | https://www.starrocks.io/blog              | 1718205970 |
| 7914 | https://www.starrocks.io/                  | 1718206760 |
| 9854 | https://www.starrocks.io/blog              | 1718205676 |
| 1186 | https://www.starrocks.io/                  | 1718209083 |
| 3305 | https://docs.starrocks.io/                 | 1718209083 |
| 2288 | https://www.starrocks.io/blog              | 1718206759 |
| 7879 | https://www.starrocks.io/product/community | 1718204280 |
| 2666 | https://www.starrocks.io/                  | 1718208842 |
| 5801 | https://www.starrocks.io/                  | 1718208783 |
| 8409 | https://www.starrocks.io/                  | 1718206889 |
+------+--------------------------------------------+------------+
10 rows in set (0.02 sec)
```

---

## 設定の詳細

StarRocksを共有データモードで使用する経験を積んだ今、設定を理解することが重要です。

### CNの設定

ここで使用されているCNの設定はデフォルトです。CNは共有データモードの使用を目的として設計されています。デフォルトの設定は以下の通りです。変更を加える必要はありません。

```bash
sys_log_level = INFO

# 管理、ウェブ、ハートビートサービス用ポート
be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
starlet_port = 9070
```

### FEの設定

FEの設定は、デフォルトとは少し異なります。FEはデータがBEノードの本地ディスクではなく、オブジェクトストレージに保存されることを期待するように設定されなければなりません。

`docker-compose.yml`ファイルは、`starrocks-fe`サービスの`command`セクションでFEの設定を生成します。

```plaintext
# 共有データ、ストレージタイプ、エンドポイントを設定
run_mode = shared_data
cloud_native_storage_type = S3
aws_s3_endpoint = minio:9000

# minIOのパスを設定
aws_s3_path = starrocks

# MinIOオブジェクトの読み書きに関するクレデンシャル
aws_s3_access_key = AAAAAAAAAAAAAAAAAAAA
aws_s3_secret_key = BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
aws_s3_use_instance_profile = false
aws_s3_use_aws_sdk_default_behavior = false

# オブジェクト=ストレージにデフォルトストレージを作成したくない場合は、falseに設定する。
enable_load_volume_from_conf = true
```

:::note
この設定ファイルにはFEのデフォルトエントリは含まれておらず、共有データモードの設定のみが示されています。
:::

デフォルトではないFEの設定：

:::note
多くの設定パラメータは`s3_`で始まります。このプレフィックスは、すべてのAmazon S3互換ストレージタイプ（例：S3、GCS、MinIO）に使用されます。Azure Blob Storageを使用する場合、プレフィックスは`azure_`です。
:::

#### `run_mode=shared_data`

これは共有データモードの使用を有効にします。

#### `cloud_native_storage_type=S3`

これは、S3互換ストレージまたはAzure Blob Storageが使用されるかどうかを指定します。MinIOの場合、常にS3です。

#### `aws_s3_endpoint=minio:9000`

MinIOのエンドポイント（ポート番号を含む）。

#### `aws_s3_path=starrocks`

バケット名。

#### `aws_s3_access_key=AAAAAAAAAAAAAAAAAAAA`

MinIOのアクセスキー。

#### `aws_s3_secret_key=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB`

MinIOのアクセスキーシークレット。

#### `aws_s3_use_instance_profile=false`

MinIOを使用する場合、アクセスキーが使用されるため、インスタンスプロファイルはMinIOでは使用されません。

#### `aws_s3_use_aws_sdk_default_behavior=false`

MinIOを使用する場合、このパラメータは常にfalseに設定されます。

#### `enable_load_volume_from_conf=true`

これがtrueに設定されている場合、MinIOオブジェクトストレージを使用して`builtin_storage_volume`という名前のStarRocksストレージボリュームが作成され、作成するテーブルのデフォルトのストレージボリュームとして設定されます。

---

## Routine Loadコマンドに関する注意事項

StarRocks Routine Loadは多くの引数を取ります。このチュートリアルで使用されているものだけがここで説明され、残りは詳細情報セクションでリンクされます。

```SQL
CREATE ROUTINE LOAD quickstart.clicks ON site_clicks
PROPERTIES
(
    "format" = "JSON",
    "jsonpaths" ="[\"$.uid\",\"$.site\",\"$.vtime\"]"
)
FROM KAFKA
(     
    "kafka_broker_list" = "redpanda:29092",
    "kafka_topic" = "test2",
    "kafka_partitions" = "0",
    "kafka_offsets" = "OFFSET_BEGINNING"
);
```

### パラメータ

```
CREATE ROUTINE LOAD quickstart.clicks ON site_clicks
```

`CREATE ROUTINE LOAD ON`のパラメータは：
- database_name.job_name
- table_name

`database_name.`はオプションです。このラボでは`quickstart`で指定されています。

`job_name`は必須で、`clicks`です。

`table_name`は必須で、`site_clicks`です。

### ジョブのプロパティ

#### `format`

```
"format" = "JSON",
```

この場合、データはJSON形式であるため、プロパティは`JSON`に設定されています。他の有効な形式は：`CSV`、`JSON`、および`Avro`です。`CSV`がデフォルトです。

#### `jsonpaths`

```
"jsonpaths" ="[\"$.uid\",\"$.site\",\"$.vtime\"]"
```

JSON形式のデータからロードしたいフィールドの名前。このプロパティの値は有効なJsonPath式です。詳細情報はこのページの最後にあります。

### Data source properties

#### `kafka_broker_list`

```
"kafka_broker_list" = "redpanda:29092",
```

Kafkaのブローカー接続情報。形式は`<kafka_broker_name_or_ip>:<broker_ port>`です。複数のブローカーはカンマで区切られます。

#### `kafka_topic`

```
"kafka_topic" = "test2",
```

消費するKafkaトピック。

#### `kafka_partitions` and `kafka_offsets`

```
"kafka_partitions" = "0",
"kafka_offsets" = "OFFSET_BEGINNING"
```

これらのプロパティは、`kafka_partitions`エントリごとに1つの`kafka_offset`が必要であるため、一緒に提示されます。

`kafka_partitions`は消費する1つ以上のパーティションのリストです。このプロパティが設定されていない場合、すべてのパーティションが消費されます。

`kafka_offsets`は、`kafka_partitions`にリストされている各パーティションに対するオフセットのリストです。この場合、値は`OFFSET_BEGINNING`であり、すべてのデータが消費されます。デフォルトは新しいデータのみを消費することです。

---

## 要約

このチュートリアルでは：

- DockerでStarRocks、Reedpanda、およびMinioをデプロイしました
- Kafkaトピックからデータを取り込むRoutine Loadジョブを作成しました
- MinIOを使用するStarRocksストレージボリュームの設定方法を学びました

## 詳細情報

[StarRocks アーキテクチャ](../introduction/Architecture.md)

このラボで使用されたサンプルは非常にシンプルです。Routine Loadには多くのオプションと機能があります。[詳細はこちら](../loading/RoutineLoad.md)。

[JSONPath](https://goessner.net/articles/JsonPath/)
