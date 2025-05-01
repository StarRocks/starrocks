---
displayed_sidebar: docs
---

# INSERT を使用したデータのロード

このトピックでは、SQL ステートメント - INSERT を使用して StarRocks にデータをロードする方法について説明します。

MySQL や他の多くのデータベース管理システムと同様に、StarRocks は INSERT を使用して内部テーブルにデータをロードすることをサポートしています。VALUES 句を使用して 1 行または複数行を直接挿入し、関数やデモをテストすることができます。また、[外部テーブル](../data_source/External_table.md)からクエリの結果として定義されたデータを内部テーブルに挿入することもできます。

StarRocks v2.4 では、INSERT OVERWRITE を使用してテーブルにデータを上書きすることもサポートしています。INSERT OVERWRITE ステートメントは、上書き機能を実装するために次の操作を統合しています。

1. 元のデータを格納するパーティションに基づいて一時パーティションを作成します。
2. 一時パーティションにデータを挿入します。
3. 元のパーティションを一時パーティションと入れ替えます。

> **注意**
>
> データを上書きする前に検証が必要な場合は、INSERT OVERWRITE を使用する代わりに、上記の手順に従ってデータを上書きし、パーティションを入れ替える前に検証することができます。

## 注意事項

- INSERT トランザクションは同期的であるため、MySQL クライアントから **Ctrl** と **C** キーを押すことでのみキャンセルできます。
- 現在の StarRocks のバージョンでは、テーブルのスキーマに準拠していない行がある場合、INSERT トランザクションはデフォルトで失敗します。たとえば、テーブル内のフィールドの長さ制限を超える行がある場合、INSERT トランザクションは失敗します。セッション変数 `enable_insert_strict` を `false` に設定すると、テーブルと一致しない行をフィルタリングしてトランザクションを続行できます。
- 小さなデータバッチを頻繁に INSERT ステートメントで StarRocks にロードすると、過剰なデータバージョンが生成されます。これはクエリパフォーマンスに大きな影響を与えます。したがって、プロダクション環境では、INSERT コマンドを頻繁に使用してデータをロードしたり、日常的なデータロードの手段として使用したりしないことをお勧めします。アプリケーションや分析シナリオがストリーミングデータや小さなデータバッチを個別にロードするソリューションを必要とする場合、Apache Kafka® をデータソースとして使用し、[Routine Load](../loading/RoutineLoad.md) を介してデータをロードすることをお勧めします。
- INSERT OVERWRITE ステートメントを実行すると、StarRocks は元のデータを格納するパーティションに対して一時パーティションを作成し、新しいデータを一時パーティションに挿入し、[元のパーティションを一時パーティションと入れ替えます](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md#use-a-temporary-partition-to-replace-current-partition)。これらの操作はすべて FE Leader ノードで実行されます。したがって、INSERT OVERWRITE コマンドを実行中に FE Leader ノードがクラッシュすると、ロードトランザクション全体が失敗し、一時パーティションが切り捨てられます。

## 準備

`load_test` という名前のデータベースを作成し、宛先テーブルとして `insert_wiki_edit` テーブルを、ソーステーブルとして `source_wiki_edit` テーブルを作成します。

> **注意**
>
> このトピックで示されている例は、`insert_wiki_edit` テーブルと `source_wiki_edit` テーブルに基づいています。独自のテーブルとデータを使用したい場合は、準備をスキップして次のステップに進むことができます。

```SQL
CREATE DATABASE IF NOT EXISTS load_test;
USE load_test;
CREATE TABLE insert_wiki_edit
(
    event_time DATETIME,
    channel VARCHAR(32) DEFAULT '',
    user VARCHAR(128) DEFAULT '',
    is_anonymous TINYINT DEFAULT '0',
    is_minor TINYINT DEFAULT '0',
    is_new TINYINT DEFAULT '0',
    is_robot TINYINT DEFAULT '0',
    is_unpatrolled TINYINT DEFAULT '0',
    delta INT DEFAULT '0',
    added INT DEFAULT '0',
    deleted INT DEFAULT '0'
)
DUPLICATE KEY
(
    event_time,
    channel,
    user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)
(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user) BUCKETS 3;

CREATE TABLE source_wiki_edit
(
    event_time DATETIME,
    channel VARCHAR(32) DEFAULT '',
    user VARCHAR(128) DEFAULT '',
    is_anonymous TINYINT DEFAULT '0',
    is_minor TINYINT DEFAULT '0',
    is_new TINYINT DEFAULT '0',
    is_robot TINYINT DEFAULT '0',
    is_unpatrolled TINYINT DEFAULT '0',
    delta INT DEFAULT '0',
    added INT DEFAULT '0',
    deleted INT DEFAULT '0'
)
DUPLICATE KEY
(
    event_time,
    channel,
    user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)
(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user) BUCKETS 3;
```

## INSERT INTO VALUES を使用したデータの挿入

INSERT INTO VALUES コマンドを使用して、特定のテーブルに 1 行または複数行を追加できます。複数行はカンマ (,) で区切られます。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) を参照してください。

> **注意**
>
> INSERT INTO VALUES を使用したデータの挿入は、小規模なデータセットでデモを検証する必要がある場合にのみ適用されます。大規模なテストやプロダクション環境には推奨されません。StarRocks に大量のデータをロードするには、[Ingestion Overview](../loading/Loading_intro.md) を参照して、シナリオに適した他のオプションを確認してください。

次の例では、`source_wiki_edit` データソーステーブルに `insert_load_wikipedia` というラベルで 2 行を挿入します。ラベルは、データベース内の各データロードトランザクションの一意の識別ラベルです。

```SQL
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

## INSERT INTO SELECT を使用したデータの挿入

INSERT INTO SELECT コマンドを使用して、データソーステーブルのクエリ結果をターゲットテーブルにロードできます。INSERT INTO SELECT コマンドは、データソーステーブルからのデータに対して ETL 操作を実行し、StarRocks の内部テーブルにデータをロードします。データソースは、1 つ以上の内部または外部テーブルである可能性があります。ターゲットテーブルは、StarRocks の内部テーブルでなければなりません。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) を参照してください。

> **注意**
>
> 外部テーブルからのデータの挿入は、内部テーブルからのデータの挿入と同じです。簡単のため、以下の例では内部テーブルからのデータの挿入方法のみを示します。

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` にデータを挿入します。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_1
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` の `p06` および `p12` パーティションにデータを挿入します。パーティションが指定されていない場合、データはすべてのパーティションに挿入されます。指定されたパーティションがある場合、データは指定されたパーティションにのみ挿入されます。

```SQL
INSERT INTO insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_2
SELECT * FROM source_wiki_edit;
```

ターゲットテーブルにデータがあることを確認するためにクエリを実行します。

```Plain text
MySQL > select * from insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.00 sec)
```

`p06` および `p12` パーティションを切り捨てると、データはクエリで返されません。

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` に `event_time` および `channel` 列を挿入します。ここで指定されていない列にはデフォルト値が使用されます。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

## INSERT OVERWRITE VALUES を使用したデータの上書き

INSERT OVERWRITE VALUES コマンドを使用して、特定のテーブルを 1 行または複数行で上書きできます。複数行はカンマ (,) で区切られます。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) を参照してください。

> **注意**
>
> INSERT OVERWRITE VALUES を使用したデータの上書きは、小規模なデータセットでデモを検証する必要がある場合にのみ適用されます。大規模なテストやプロダクション環境には推奨されません。StarRocks に大量のデータをロードするには、[Ingestion Overview](../loading/Loading_intro.md) を参照して、シナリオに適した他のオプションを確認してください。

ソーステーブルとターゲットテーブルにデータがあることを確認するためにクエリを実行します。

```Plain
MySQL > SELECT * FROM source_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.02 sec)
 
MySQL > SELECT * FROM insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)
```

次の例では、ソーステーブル `source_wiki_edit` を 2 つの新しい行で上書きします。

```SQL
INSERT OVERWRITE source_wiki_edit
WITH LABEL insert_load_wikipedia_ow
VALUES
    ("2015-09-12 00:00:00","#cn.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),
    ("2015-09-12 00:00:00","#fr.wikipedia","PereBot",0,1,0,1,0,17,17,0);
```

## INSERT OVERWRITE SELECT を使用したデータの上書き

INSERT OVERWRITE SELECT コマンドを使用して、データソーステーブルのクエリ結果でテーブルを上書きできます。INSERT OVERWRITE SELECT ステートメントは、1 つ以上の内部または外部テーブルからのデータに対して ETL 操作を実行し、内部テーブルをデータで上書きします。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) を参照してください。

> **注意**
>
> 外部テーブルからのデータのロードは、内部テーブルからのデータのロードと同じです。簡単のため、以下の例では内部テーブルからのデータでターゲットテーブルを上書きする方法のみを示します。

ソーステーブルとターゲットテーブルに異なる行のデータがあることを確認するためにクエリを実行します。

```Plain
MySQL > SELECT * FROM source_wiki_edit;
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user         | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #cn.wikipedia | GELongstreet |            0 |        0 |      0 |        0 |              0 |    36 |    36 |       0 |
| 2015-09-12 00:00:00 | #fr.wikipedia | PereBot      |            0 |        1 |      0 |        1 |              0 |    17 |    17 |       0 |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.02 sec)
 
MySQL > SELECT * FROM insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)
```

- 次の例では、ソーステーブルからのデータで `insert_wiki_edit` テーブルを上書きします。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_1
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルからのデータで `insert_wiki_edit` テーブルの `p06` および `p12` パーティションを上書きします。

```SQL
INSERT OVERWRITE insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_ow_2
SELECT * FROM source_wiki_edit;
```

ターゲットテーブルにデータがあることを確認するためにクエリを実行します。

```plain text
MySQL > select * from insert_wiki_edit;
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user         | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #fr.wikipedia | PereBot      |            0 |        1 |      0 |        1 |              0 |    17 |    17 |       0 |
| 2015-09-12 00:00:00 | #cn.wikipedia | GELongstreet |            0 |        0 |      0 |        0 |              0 |    36 |    36 |       0 |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)
```

`p06` および `p12` パーティションを切り捨てると、データはクエリで返されません。

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 次の例では、ソーステーブルからの `event_time` および `channel` 列でターゲットテーブル `insert_wiki_edit` を上書きします。上書きされない列にはデフォルト値が割り当てられます。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

## INSERT トランザクションのステータスを確認する

### 結果による確認

INSERT トランザクションは、トランザクションの結果に応じて異なるステータスを返します。

- **トランザクションが成功した場合**

トランザクションが成功した場合、StarRocks は次のように返します。

```Plain
Query OK, 2 rows affected (0.05 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'1006'}
```

- **トランザクションが失敗した場合**

すべてのデータ行がターゲットテーブルにロードされなかった場合、INSERT トランザクションは失敗します。トランザクションが失敗した場合、StarRocks は次のように返します。

```Plain
ERROR 1064 (HY000): Insert has filtered data in strict mode, tracking_url=http://x.x.x.x:yyyy/api/_load_error_log?file=error_log_9f0a4fd0b64e11ec_906bbede076e9d08
```

`tracking_url` を使用してログを確認することで問題を特定できます。

### SHOW LOAD を使用した確認

[SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) コマンドを使用して、INSERT トランザクションのステータスを確認できます。

次の例では、`insert_load_wikipedia` というラベルのトランザクションのステータスを確認します。

```SQL
SHOW LOAD WHERE label="insert_load_wikipedia"\G
```

返される結果は次のとおりです。

```Plain
*************************** 1. row ***************************
         JobId: 10278
         Label: insert_load_wikipedia
         State: FINISHED
      Progress: ETL:100%; LOAD:100%
          Type: INSERT
      Priority: NORMAL
      ScanRows: 0
  FilteredRows: 0
UnselectedRows: 0
      SinkRows: 2
       EtlInfo: NULL
      TaskInfo: resource:N/A; timeout(s):300; max_filter_ratio:0.0
      ErrorMsg: NULL
    CreateTime: 2023-06-12 18:31:07
  EtlStartTime: 2023-06-12 18:31:07
 EtlFinishTime: 2023-06-12 18:31:07
 LoadStartTime: 2023-06-12 18:31:07
LoadFinishTime: 2023-06-12 18:31:08
   TrackingSQL: 
    JobDetails: {"All backends":{"3d96e21a-090c-11ee-9083-00163e0e2cf9":[10142]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":175,"InternalTableLoadRows":2,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"3d96e21a-090c-11ee-9083-00163e0e2cf9":[]}}
1 row in set (0.00 sec)
```

### curl コマンドを使用した確認

curl コマンドを使用して、INSERT トランザクションのステータスを確認できます。

ターミナルを開き、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
  http://<fe_address>:<fe_http_port>/api/<db_name>/_load_info?label=<label_name>
```

次の例では、`insert_load_wikipedia` というラベルのトランザクションのステータスを確認します。

```Bash
curl --location-trusted -u <username>:<password> \
  http://x.x.x.x:8030/api/load_test/_load_info?label=insert_load_wikipedia
```

> **注意**
>
> パスワードが設定されていないアカウントを使用する場合は、`<username>:` のみを入力する必要があります。

返される結果は次のとおりです。

```Plain
{
   "jobInfo":{
      "dbName":"load_test",
      "tblNames":[
         "source_wiki_edit"
      ],
      "label":"insert_load_wikipedia",
      "state":"FINISHED",
      "failMsg":"",
      "trackingUrl":""
   },
   "status":"OK",
   "msg":"Success"
}
```

## 設定

INSERT トランザクションのために次の設定項目を設定できます。

- **FE 設定**

| FE 設定                           | 説明                                                                 |
| --------------------------------- | ------------------------------------------------------------------- |
| insert_load_default_timeout_second | INSERT トランザクションのデフォルトタイムアウト。単位: 秒。このパラメータで設定された時間内に現在の INSERT トランザクションが完了しない場合、システムによってキャンセルされ、ステータスは CANCELLED になります。StarRocks の現在のバージョンでは、このパラメータを使用してすべての INSERT トランザクションに対して一律のタイムアウトを指定することしかできず、特定の INSERT トランザクションに対して異なるタイムアウトを設定することはできません。デフォルトは 3600 秒 (1 時間) です。指定された時間内に INSERT トランザクションを完了できない場合は、このパラメータを調整してタイムアウトを延長できます。 |

- **セッション変数**

| セッション変数       | 説明                                                                 |
| -------------------- | ------------------------------------------------------------------- |
| enable_insert_strict | INSERT トランザクションが無効なデータ行に対して寛容であるかどうかを制御するスイッチ値です。これが `true` に設定されている場合、データ行のいずれかが無効であるとトランザクションは失敗します。これが `false` に設定されている場合、少なくとも 1 行のデータが正しくロードされた場合にトランザクションは成功し、ラベルが返されます。デフォルトは `true` です。この変数は `SET enable_insert_strict = {true or false};` コマンドで設定できます。 |
| query_timeout        | SQL コマンドのタイムアウト。単位: 秒。INSERT は SQL コマンドとして、このセッション変数によっても制限されます。この変数は `SET query_timeout = xxx;` コマンドで設定できます。 |
```