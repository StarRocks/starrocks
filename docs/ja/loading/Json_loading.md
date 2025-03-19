---
displayed_sidebar: docs
---

# はじめに

半構造化データ（例えば、JSON）を stream load または routine load を使用してインポートできます。

## 使用シナリオ

* Stream Load: テキストファイルに保存された JSON データをインポートするには、stream load を使用します。
* Routine Load: Kafka 内の JSON データをインポートするには、routine load を使用します。

### Stream Load インポート

サンプルデータ:

~~~json
{ "id": 123, "city" : "beijing"},
{ "id": 456, "city" : "shanghai"},
    ...
~~~

例:

~~~shell
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "jsonpaths: [\"$.id\", \"$.city\"]" \
    -T example.json \
    http://FE_HOST:HTTP_PORT/api/DATABASE/TABLE/_stream_load
~~~

`format: json` パラメータは、インポートするデータの形式を実行することを可能にします。`jsonpaths` は、対応するデータインポートパスを実行するために使用されます。

関連パラメータ:

* jsonpaths: 各列の JSON パスを選択
* json\_root: JSON の解析を開始する列を選択
* strip\_outer\_array: 最外部の配列フィールドを切り取る
* strict\_mode: インポート中の列型変換を厳密にフィルタリング

JSON データスキーマと StarRocks データスキーマが完全に一致しない場合、`Jsonpath` を修正します。

サンプルデータ:

~~~json
{"k1": 1, "k2": 2}
~~~

インポート例:

~~~bash
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" \
    -H "columns: k2, tmp_k1, k1 = tmp_k1 * 100" \
    -T example.json \
    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
~~~

インポート中に k1 を 100 倍する ETL 操作が実行され、列は `Jsonpath` によって元のデータと一致します。

インポート結果は以下の通りです:

~~~plain text
+------+------+
| k1   | k2   |
+------+------+
|  100 |    2 |
+------+------+
~~~

欠落している列については、列の定義が nullable であれば `NULL` が追加され、または `ifnull` によってデフォルト値を追加できます。

サンプルデータ:

~~~json
[
    {"k1": 1, "k2": "a"},
    {"k1": 2},
    {"k1": 3, "k2": "c"},
]
~~~

インポート例-1:

~~~shell
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "strip_outer_array: true" \
    -T example.json \
    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
~~~

インポート結果は以下の通りです:

~~~plain text
+------+------+
| k1   | k2   |
+------+------+
|    1 | a    |
+------+------+
|    2 | NULL |
+------+------+
|    3 | c    |
+------+------+
~~~

インポート例-2:

~~~shell
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.k1\", \"$.k2\"]" \
    -H "columns: k1, tmp_k2, k2 = ifnull(tmp_k2, 'x')" \
    -T example.json \
    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
~~~

インポート結果は以下の通りです:

~~~plain text
+------+------+
| k1   | k2   |
+------+------+
|    1 | a    |
+------+------+
|    2 | x    |
+------+------+
|    3 | c    |
+------+------+
~~~

### Routine Load インポート

stream load と同様に、Kafka データソースのメッセージ内容は完全な JSON データとして扱われます。

1. メッセージが配列形式で複数行のデータを含む場合、すべての行がインポートされ、Kafka のオフセットは 1 だけ増加します。
2. 配列形式の JSON が複数行のデータを表すが、JSON 形式のエラーにより JSON の解析に失敗した場合、エラー行は 1 だけ増加します（解析に失敗したため、StarRocks は実際に何行のデータを含むかを判断できず、エラーデータを 1 行として記録することしかできません）。

### Canal を使用して MySQL から StarRocks へ増分同期 binlogs をインポート

[Canal](https://github.com/alibaba/canal) は Alibaba のオープンソース MySQL binlog 同期ツールで、これを通じて MySQL データを Kafka に同期できます。データは Kafka で JSON 形式で生成されます。ここでは、routine load を使用して Kafka 内のデータを MySQL と増分データ同期する方法を示します。

* MySQL には、次のテーブル作成文を持つデータテーブルがあります。

~~~sql
CREATE TABLE `query_record` (
  `query_id` varchar(64) NOT NULL,
  `conn_id` int(11) DEFAULT NULL,
  `fe_host` varchar(32) DEFAULT NULL,
  `user` varchar(32) DEFAULT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime DEFAULT NULL,
  `time_used` double DEFAULT NULL,
  `state` varchar(16) NOT NULL,
  `error_message` text,
  `sql` text NOT NULL,
  `database` varchar(128) NOT NULL,
  `profile` longtext,
  `plan` longtext,
  PRIMARY KEY (`query_id`),
  KEY `idx_start_time` (`start_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8
~~~

* 前提条件: MySQL に binlog が有効であり、形式が ROW であることを確認してください。

~~~bash
[mysqld]
log-bin=mysql-bin # binlog を有効にする
binlog-format=ROW # ROW モードを選択
server_id=1 # MySQL レプリケーションには定義が必要で、canal の slaveId と重複しないようにする
~~~

* アカウントを作成し、セカンダリ MySQL サーバーに権限を付与します。

~~~sql
CREATE USER canal IDENTIFIED BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
~~~

* 次に Canal をダウンロードしてインストールします。

~~~bash
wget https://github.com/alibaba/canal/releases/download/canal-1.0.17/canal.deployer-1.0.17.tar.gz

mkdir /tmp/canal
tar zxvf canal.deployer-$version.tar.gz -C /tmp/canal
~~~

* 設定を変更します（MySQL 関連）。

`$ vi conf/example/instance.properties`

~~~bash
## mysql serverId
canal.instance.mysql.slaveId = 1234
#位置情報、自分のデータベース情報に変更する必要があります
canal.instance.master.address = 127.0.0.1:3306
canal.instance.master.journal.name =
canal.instance.master.position =
canal.instance.master.timestamp =
#canal.instance.standby.address =
#canal.instance.standby.journal.name =
#canal.instance.standby.position =
#canal.instance.standby.timestamp =
#ユーザー名/パスワード、自分のデータベース情報に変更する必要があります
canal.instance.dbUsername = canal  
canal.instance.dbPassword = canal
canal.instance.defaultDatabaseName =
canal.instance.connectionCharset = UTF-8
#テーブルの正規表現
canal.instance.filter.regex = .\*\\\\..\*
# 同期するテーブルの名前と kafka ターゲットのパーティション名を選択します。
canal.mq.dynamicTopic=databasename.query_record
canal.mq.partitionHash= databasename.query_record:query_id
~~~

* 設定を変更します（Kafka 関連）。

`$ vi /usr/local/canal/conf/canal.properties`

~~~bash
# 利用可能なオプション: tcp(デフォルト), kafka, RocketMQ
canal.serverMode = kafka
# ...
# kafka/rocketmq クラスター構成: 192.168.1.117:9092,192.168.1.118:9092,192.168.1.119:9092
canal.mq.servers = 127.0.0.1:6667
canal.mq.retries = 0
# この値は flagMessage モードで増やすことができますが、MQ メッセージの最大サイズを超えないようにしてください。
canal.mq.batchSize = 16384
canal.mq.maxRequestSize = 1048576
# flatMessage モードでは、この値を大きく変更してください。50-200 が推奨されます。
canal.mq.lingerMs = 1
canal.mq.bufferMemory = 33554432
# Canal のバッチサイズはデフォルトで 50K です。Kafka の最大メッセージサイズ制限（900K 未満）により、1M を超えないようにしてください。
canal.mq.canalBatchSize = 50
# `Canal get` のタイムアウト（ミリ秒）。空の場合は無制限のタイムアウトを示します。
canal.mq.canalGetTimeout = 100
# オブジェクトが flat json 形式かどうか
canal.mq.flatMessage = false
canal.mq.compressionType = none
canal.mq.acks = all
# Kafka メッセージ配信がトランザクションを使用するかどうか
canal.mq.transaction = false
~~~

* 起動

`bin/startup.sh`

対応する同期ログは `logs/example/example.log` に表示され、Kafka では以下の形式で表示されます:

~~~json
{
    "data": [{
        "query_id": "3c7ebee321e94773-b4d79cc3f08ca2ac",
        "conn_id": "34434",
        "fe_host": "172.26.34.139",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.578",
        "end_time": "2020-10-19 20:40:10",
        "time_used": "1.0",
        "state": "FINISHED",
        "error_message": "",
        "sql": "COMMIT",
        "database": "",
        "profile": "",
        "plan": ""
    }, {
        "query_id": "7ff2df7551d64f8e-804004341bfa63ad",
        "conn_id": "34432",
        "fe_host": "172.26.34.139",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.566",
        "end_time": "2020-10-19 20:40:10",
        "time_used": "0.0",
        "state": "FINISHED",
        "error_message": "",
        "sql": "COMMIT",
        "database": "",
        "profile": "",
        "plan": ""
    }, {
        "query_id": "3a4b35d1c1914748-be385f5067759134",
        "conn_id": "34440",
        "fe_host": "172.26.34.139",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.601",
        "end_time": "1970-01-01 08:00:00",
        "time_used": "-1.0",
        "state": "RUNNING",
        "error_message": "",
        "sql": " SELECT SUM(length(lo_custkey)), SUM(length(c_custkey)) FROM lineorder_str INNER JOIN customer_str ON lo_custkey=c_custkey;",
        "database": "ssb",
        "profile": "",
        "plan": ""
    }],
    "database": "center_service_lihailei",
    "es": 1603111211000,
    "id": 122,
    "isDdl": false,
    "mysqlType": {
        "query_id": "varchar(64)",
        "conn_id": "int(11)",
        "fe_host": "varchar(32)",
        "user": "varchar(32)",
        "start_time": "datetime(3)",
        "end_time": "datetime",
        "time_used": "double",
        "state": "varchar(16)",
        "error_message": "text",
        "sql": "text",
        "database": "varchar(128)",
        "profile": "longtext",
        "plan": "longtext"
    },
    "old": null,
    "pkNames": ["query_id"],
    "sql": "",
    "sqlType": {
        "query_id": 12,
        "conn_id": 4,
        "fe_host": 12,
        "user": 12,
        "start_time": 93,
        "end_time": 93,
        "time_used": 8,
        "state": 12,
        "error_message": 2005,
        "sql": 2005,
        "database": 12,
        "profile": 2005,
        "plan": 2005
    },
    "table": "query_record",
    "ts": 1603111212015,
    "type": "INSERT"
}
~~~

`json_root` と `strip_outer_array = true` を追加して `data` からデータをインポートします。

~~~sql
create routine load manual.query_job on query_record   
columns (query_id,conn_id,fe_host,user,start_time,end_time,time_used,state,error_message,`sql`,`database`,profile,plan)  
PROPERTIES (  
    "format"="json",  
    "json_root"="$.data",
    "desired_concurrent_number"="1",  
    "strip_outer_array" ="true",    
    "max_error_number"="1000" 
) 
FROM KAFKA (     
    "kafka_broker_list"= "172.26.92.141:9092",     
    "kafka_topic" = "databasename.query_record" 
);
~~~

これで、MySQL から StarRocks へのデータのほぼリアルタイムの同期が完了します。

`show routine load` によってインポートジョブのステータスとエラーメッセージを表示します。