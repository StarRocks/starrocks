---
displayed_sidebar: docs
---

# INSERTを使用したデータロード

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.mdx'

このトピックでは、SQLステートメント - INSERT を使用してStarRocksにデータをロードする方法について説明します。

MySQLや他の多くのデータベース管理システムと同様に、StarRocksはINSERTを使用して内部テーブルにデータをロードすることをサポートしています。VALUES句を使用して1行または複数行を直接挿入し、関数やデモをテストすることができます。また、[外部テーブル](../data_source/External_table.md)からクエリの結果として定義されたデータを内部テーブルに挿入することもできます。StarRocks v3.1以降では、INSERTコマンドとテーブル関数 [FILES()](../sql-reference/sql-functions/table-functions/files.md) を使用して、クラウドストレージ上のファイルから直接データをロードできます。

StarRocks v2.4では、INSERT OVERWRITEを使用してテーブルにデータを上書きすることもサポートしています。INSERT OVERWRITEステートメントは、上書き機能を実現するために以下の操作を統合しています：

1. 元のデータを格納するパーティションに基づいて一時パーティションを作成します。
2. 一時パーティションにデータを挿入します。
3. 元のパーティションと一時パーティションを入れ替えます。

> **注意**
>
> データを上書きする前に検証が必要な場合は、INSERT OVERWRITEを使用する代わりに、上記の手順に従ってデータを上書きし、パーティションを入れ替える前に検証することができます。

## 注意事項

- 同期INSERTトランザクションをキャンセルするには、MySQLクライアントから**Ctrl**と**C**キーを押す必要があります。
- 非同期INSERTタスクを[SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md)を使用して送信できます。
- 現在のStarRocksのバージョンでは、テーブルのスキーマに準拠していない行のデータがある場合、INSERTトランザクションはデフォルトで失敗します。例えば、テーブル内のマッピングフィールドの長さ制限を超えるフィールドがある場合、INSERTトランザクションは失敗します。セッション変数`enable_insert_strict`を`false`に設定することで、テーブルと一致しない行をフィルタリングしてトランザクションを続行することができます。
- 小さなデータバッチをStarRocksに頻繁にINSERTステートメントでロードすると、過剰なデータバージョンが生成されます。これによりクエリパフォーマンスが大きく影響を受けます。生産環境では、INSERTコマンドを頻繁に使用してデータをロードしたり、日常的なデータロードのルーチンとして使用したりしないことをお勧めします。アプリケーションや分析シナリオがストリーミングデータや小さなデータバッチのロードを別々に求める場合、Apache Kafka®をデータソースとして使用し、Routine Loadを介してデータをロードすることをお勧めします。
- INSERT OVERWRITEステートメントを実行すると、StarRocksは元のデータを格納するパーティションに対して一時パーティションを作成し、新しいデータを一時パーティションに挿入し、[元のパーティションと一時パーティションを入れ替えます](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#use-a-temporary-partition-to-replace-the-current-partition)。これらの操作はすべてFE Leaderノードで実行されます。したがって、INSERT OVERWRITEコマンドを実行中にFE Leaderノードがクラッシュすると、ロードトランザクション全体が失敗し、一時パーティションは切り捨てられます。

## 準備

### 権限を確認する

<InsertPrivNote />

### オブジェクトを作成する

`load_test`という名前のデータベースを作成し、宛先テーブルとして`insert_wiki_edit`テーブルを、ソーステーブルとして`source_wiki_edit`テーブルを作成します。

> **注意**
>
> このトピックで示されている例は、`insert_wiki_edit`テーブルと`source_wiki_edit`テーブルに基づいています。独自のテーブルとデータで作業することを希望する場合は、準備をスキップして次のステップに進むことができます。

```SQL
CREATE DATABASE IF NOT EXISTS load_test;
USE load_test;
CREATE TABLE insert_wiki_edit
(
    event_time      DATETIME,
    channel         VARCHAR(32)      DEFAULT '',
    user            VARCHAR(128)     DEFAULT '',
    is_anonymous    TINYINT          DEFAULT '0',
    is_minor        TINYINT          DEFAULT '0',
    is_new          TINYINT          DEFAULT '0',
    is_robot        TINYINT          DEFAULT '0',
    is_unpatrolled  TINYINT          DEFAULT '0',
    delta           INT              DEFAULT '0',
    added           INT              DEFAULT '0',
    deleted         INT              DEFAULT '0'
)
DUPLICATE KEY(
    event_time,
    channel,
    user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user);

CREATE TABLE source_wiki_edit
(
    event_time      DATETIME,
    channel         VARCHAR(32)      DEFAULT '',
    user            VARCHAR(128)     DEFAULT '',
    is_anonymous    TINYINT          DEFAULT '0',
    is_minor        TINYINT          DEFAULT '0',
    is_new          TINYINT          DEFAULT '0',
    is_robot        TINYINT          DEFAULT '0',
    is_unpatrolled  TINYINT          DEFAULT '0',
    delta           INT              DEFAULT '0',
    added           INT              DEFAULT '0',
    deleted         INT              DEFAULT '0'
)
DUPLICATE KEY(
    event_time,
    channel,user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user);
```

> **注意**
>
> v2.5.7以降、StarRocksはテーブルを作成する際やパーティションを追加する際にバケット数（BUCKETS）を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

## INSERT INTO VALUESを使用したデータ挿入

INSERT INTO VALUESコマンドを使用して、特定のテーブルに1行または複数行を追加できます。複数行はカンマ（,）で区切られます。詳細な手順とパラメータの参照については、[SQLリファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

> **注意**
>
> INSERT INTO VALUESを使用したデータ挿入は、小さなデータセットでデモを検証する場合にのみ適用されます。大規模なテストや本番環境には推奨されません。StarRocksに大量のデータをロードするには、[ロードオプション](Loading_intro.md)を参照し、シナリオに適した他のオプションを検討してください。

以下の例では、`source_wiki_edit`データソーステーブルに`insert_load_wikipedia`ラベルを付けて2行を挿入します。ラベルは、データベース内の各データロードトランザクションの一意の識別ラベルです。

```SQL
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

## INSERT INTO SELECTを使用したデータ挿入

データソーステーブルのクエリ結果をターゲットテーブルにロードするには、INSERT INTO SELECTコマンドを使用します。INSERT INTO SELECTコマンドは、データソーステーブルからのデータに対してETL操作を行い、StarRocksの内部テーブルにデータをロードします。データソースは、1つ以上の内部または外部テーブル、またはクラウドストレージ上のデータファイルである可能性があります。ターゲットテーブルはStarRocksの内部テーブルでなければなりません。詳細な手順とパラメータの参照については、[SQLリファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

### 内部または外部テーブルから内部テーブルへのデータ挿入

> **注意**
>
> 外部テーブルからのデータ挿入は、内部テーブルからのデータ挿入と同じです。簡単のため、以下の例では内部テーブルからのデータ挿入のみを示します。

- 以下の例では、ソーステーブルからターゲットテーブル`insert_wiki_edit`にデータを挿入します。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_1
SELECT * FROM source_wiki_edit;
```

- 以下の例では、ソーステーブルからターゲットテーブル`insert_wiki_edit`の`p06`および`p12`パーティションにデータを挿入します。パーティションが指定されていない場合、データはすべてのパーティションに挿入されます。指定されたパーティションにのみデータが挿入されます。

```SQL
INSERT INTO insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_2
SELECT * FROM source_wiki_edit;
```

ターゲットテーブルをクエリして、データが存在することを確認します。

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

`p06`および`p12`パーティションを切り捨てると、クエリでデータは返されません。

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 以下の例では、ソーステーブルからターゲットテーブル`insert_wiki_edit`に`event_time`および`channel`列を挿入します。ここで指定されていない列にはデフォルト値が使用されます。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

:::note
v3.3.1以降、主キーテーブルでINSERT INTOステートメントに列リストを指定すると、部分更新（以前のバージョンでは完全アップサート）を実行します。列リストが指定されていない場合、システムは完全アップサートを実行します。
:::

### FILES()を使用して外部ソースのファイルから直接データを挿入する

v3.1以降、StarRocksはINSERTコマンドと[FILES()](../sql-reference/sql-functions/table-functions/files.md)関数を使用してクラウドストレージ上のファイルから直接データをロードすることをサポートしています。これにより、external catalogやファイル外部テーブルを最初に作成する必要がありません。さらに、FILES()はファイルのテーブルスキーマを自動的に推論でき、データロードのプロセスを大幅に簡素化します。

以下の例では、AWS S3バケット`inserttest`内のParquetファイル**parquet/insert_wiki_edit_append.parquet**からテーブル`insert_wiki_edit`にデータ行を挿入します：

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
```

## INSERT OVERWRITE VALUESを使用したデータの上書き

INSERT OVERWRITE VALUESコマンドを使用して、特定のテーブルを1行または複数行で上書きできます。複数行はカンマ（,）で区切られます。詳細な手順とパラメータの参照については、[SQLリファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

> **注意**
>
> INSERT OVERWRITE VALUESを使用したデータの上書きは、小さなデータセットでデモを検証する場合にのみ適用されます。大規模なテストや本番環境には推奨されません。StarRocksに大量のデータをロードするには、[ロードオプション](Loading_intro.md)を参照し、シナリオに適した他のオプションを検討してください。

ソーステーブルとターゲットテーブルをクエリして、データが存在することを確認します。

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

以下の例では、ソーステーブル`source_wiki_edit`を2つの新しい行で上書きします。

```SQL
INSERT OVERWRITE source_wiki_edit
WITH LABEL insert_load_wikipedia_ow
VALUES
    ("2015-09-12 00:00:00","#cn.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),
    ("2015-09-12 00:00:00","#fr.wikipedia","PereBot",0,1,0,1,0,17,17,0);
```

## INSERT OVERWRITE SELECTを使用したデータの上書き

データソーステーブルのクエリ結果でテーブルを上書きするには、INSERT OVERWRITE SELECTコマンドを使用します。INSERT OVERWRITE SELECTステートメントは、1つ以上の内部または外部テーブルからのデータに対してETL操作を行い、内部テーブルをデータで上書きします。詳細な手順とパラメータの参照については、[SQLリファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

> **注意**
>
> 外部テーブルからのデータロードは、内部テーブルからのデータロードと同じです。簡単のため、以下の例では内部テーブルからのデータでターゲットテーブルを上書きする方法のみを示します。

ソーステーブルとターゲットテーブルをクエリして、異なるデータ行を保持していることを確認します。

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

- 以下の例では、ソーステーブルのデータでテーブル`insert_wiki_edit`を上書きします。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_1
SELECT * FROM source_wiki_edit;
```

- 以下の例では、ソーステーブルのデータでテーブル`insert_wiki_edit`の`p06`および`p12`パーティションを上書きします。

```SQL
INSERT OVERWRITE insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_ow_2
SELECT * FROM source_wiki_edit;
```

ターゲットテーブルをクエリして、データが存在することを確認します。

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

`p06`および`p12`パーティションを切り捨てると、クエリでデータは返されません。

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

:::note
`PARTITION BY column`戦略を使用するテーブルの場合、INSERT OVERWRITEはパーティションキーの値を指定することで、宛先テーブルに新しいパーティションを作成することをサポートします。既存のパーティションは通常通り上書きされます。

以下の例では、パーティション化されたテーブル`activity`を作成し、データを挿入しながらテーブルに新しいパーティションを作成します：

```SQL
CREATE TABLE activity (
id INT          NOT NULL,
dt VARCHAR(10)  NOT NULL
) ENGINE=OLAP 
DUPLICATE KEY(`id`)
PARTITION BY (`id`, `dt`)
DISTRIBUTED BY HASH(`id`);

INSERT OVERWRITE activity
PARTITION(id='4', dt='2022-01-01')
WITH LABEL insert_activity_auto_partition
VALUES ('4', '2022-01-01');
```

:::

- 以下の例では、ソーステーブルの`event_time`および`channel`列でターゲットテーブル`insert_wiki_edit`を上書きします。上書きされない列にはデフォルト値が割り当てられます。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

## 生成列を持つテーブルへのデータ挿入

生成列は、他の列に基づいて事前定義された式や評価から値が導出される特別な列です。生成列は、クエリ要求が高価な式の評価を含む場合に特に便利です。例えば、JSON値から特定のフィールドをクエリしたり、ARRAYデータを計算したりする場合です。StarRocksはデータがテーブルにロードされる際に式を評価し、生成列に結果を格納することで、クエリ中の式評価を回避し、クエリパフォーマンスを向上させます。

INSERTを使用して生成列を持つテーブルにデータをロードできます。

以下の例では、テーブル`insert_generated_columns`を作成し、1行を挿入します。このテーブルには2つの生成列があります：`avg_array`と`get_string`。`avg_array`は`data_array`内のARRAYデータの平均値を計算し、`get_string`はJSONパス`a`から文字列を抽出します。

```SQL
CREATE TABLE insert_generated_columns (
  id           INT(11)           NOT NULL    COMMENT "ID",
  data_array   ARRAY<INT(11)>    NOT NULL    COMMENT "ARRAY",
  data_json    JSON              NOT NULL    COMMENT "JSON",
  avg_array    DOUBLE            NULL 
      AS array_avg(data_array)               COMMENT "Get the average of ARRAY",
  get_string   VARCHAR(65533)    NULL 
      AS get_json_string(json_string(data_json), '$.a') COMMENT "Extract JSON string"
) ENGINE=OLAP 
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id);

INSERT INTO insert_generated_columns 
VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
```

> **注意**
>
> 生成列に直接データをロードすることはサポートされていません。

テーブルをクエリして、データが含まれていることを確認できます。

```Plain
mysql> SELECT * FROM insert_generated_columns;
+------+------------+------------------+-----------+------------+
| id   | data_array | data_json        | avg_array | get_string |
+------+------------+------------------+-----------+------------+
|    1 | [1,2]      | {"a": 1, "b": 2} |       1.5 | 1          |
+------+------------+------------------+-----------+------------+
1 row in set (0.02 sec)
```

## INSERTを使用した非同期データロード

INSERTを使用したデータロードは同期トランザクションを送信しますが、セッションの中断やタイムアウトにより失敗する可能性があります。[SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md)を使用して非同期INSERTトランザクションを送信できます。この機能はStarRocks v2.5以降でサポートされています。

- 以下の例では、ソーステーブルからターゲットテーブル`insert_wiki_edit`にデータを非同期で挿入します。

```SQL
SUBMIT TASK AS INSERT INTO insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- 以下の例では、ソーステーブルのデータでテーブル`insert_wiki_edit`を非同期で上書きします。

```SQL
SUBMIT TASK AS INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- 以下の例では、ソーステーブルのデータでテーブル`insert_wiki_edit`を非同期で上書きし、ヒントを使用してクエリタイムアウトを`100000`秒に延長します。

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- 以下の例では、ソーステーブルのデータでテーブル`insert_wiki_edit`を非同期で上書きし、タスク名を`async`として指定します。

```SQL
SUBMIT TASK async
AS INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

Information Schemaのメタデータビュー`task_runs`をクエリして、非同期INSERTタスクのステータスを確認できます。

以下の例では、INSERTタスク`async`のステータスを確認します。

```SQL
SELECT * FROM information_schema.task_runs WHERE task_name = 'async';
```

## INSERTジョブのステータスを確認する

### 結果による確認

同期INSERTトランザクションは、トランザクションの結果に応じて異なるステータスを返します。

- **トランザクション成功**

トランザクションが成功した場合、StarRocksは以下を返します：

```Plain
Query OK, 2 rows affected (0.05 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'1006'}
```

- **トランザクション失敗**

ターゲットテーブルにデータが全くロードされなかった場合、INSERTトランザクションは失敗します。トランザクションが失敗した場合、StarRocksは以下を返します：

```Plain
ERROR 1064 (HY000): Insert has filtered data in strict mode, tracking_url=http://x.x.x.x:yyyy/api/_load_error_log?file=error_log_9f0a4fd0b64e11ec_906bbede076e9d08
```

`tracking_url`を使用してログを確認することで問題を特定できます。

### Information Schemaによる確認

[SELECT](../sql-reference/sql-statements/table_bucket_part_index/SELECT.md)ステートメントを使用して、`information_schema`データベースの`loads`テーブルから1つ以上のロードジョブの結果をクエリできます。この機能はv3.1以降でサポートされています。

例1：`load_test`データベースで実行されたロードジョブの結果をクエリし、作成時間（`CREATE_TIME`）で降順にソートし、トップの結果のみを返します。

```SQL
SELECT * FROM information_schema.loads
WHERE database_name = 'load_test'
ORDER BY create_time DESC
LIMIT 1\G
```

例2：`load_test`データベースで実行されたロードジョブ（ラベルが`insert_load_wikipedia`）の結果をクエリします：

```SQL
SELECT * FROM information_schema.loads
WHERE database_name = 'load_test' and label = 'insert_load_wikipedia'\G
```

返される結果は以下の通りです：

```Plain
*************************** 1. row ***************************
              JOB_ID: 21319
               LABEL: insert_load_wikipedia
       DATABASE_NAME: load_test
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: INSERT
            PRIORITY: NORMAL
           SCAN_ROWS: 0
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 2
            ETL_INFO: 
           TASK_INFO: resource:N/A; timeout(s):300; max_filter_ratio:0.0
         CREATE_TIME: 2023-08-09 10:42:23
      ETL_START_TIME: 2023-08-09 10:42:23
     ETL_FINISH_TIME: 2023-08-09 10:42:23
     LOAD_START_TIME: 2023-08-09 10:42:23
    LOAD_FINISH_TIME: 2023-08-09 10:42:24
         JOB_DETAILS: {"All backends":{"5ebf11b5-365e-11ee-9e4a-7a563fb695da":[10006]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":175,"InternalTableLoadRows":2,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"5ebf11b5-365e-11ee-9e4a-7a563fb695da":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
1 row in set (0.01 sec)
```

返される結果のフィールドに関する情報については、[Information Schema > loads](../sql-reference/information_schema/loads.md)を参照してください。

### curlコマンドによる確認

curlコマンドを使用してINSERTトランザクションのステータスを確認できます。

ターミナルを起動し、以下のコマンドを実行します：

```Bash
curl --location-trusted -u <username>:<password> \
  http://<fe_address>:<fe_http_port>/api/<db_name>/_load_info?label=<label_name>
```

以下の例では、ラベル`insert_load_wikipedia`のトランザクションのステータスを確認します。

```Bash
curl --location-trusted -u <username>:<password> \
  http://x.x.x.x:8030/api/load_test/_load_info?label=insert_load_wikipedia
```

> **注意**
>
> パスワードが設定されていないアカウントを使用する場合、`<username>:`のみを入力する必要があります。

返される結果は以下の通りです：

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

INSERTトランザクションのために以下の設定項目を設定できます：

- **FE設定**

| FE設定                           | 説明                                                                 |
| -------------------------------- | ------------------------------------------------------------------- |
| insert_load_default_timeout_second | INSERTトランザクションのデフォルトタイムアウト。単位：秒。このパラメータで設定された時間内に現在のINSERTトランザクションが完了しない場合、システムによってキャンセルされ、ステータスはCANCELLEDになります。現在のStarRocksのバージョンでは、このパラメータを使用してすべてのINSERTトランザクションに対して統一されたタイムアウトを指定することしかできず、特定のINSERTトランザクションに対して異なるタイムアウトを設定することはできません。デフォルトは3600秒（1時間）です。指定された時間内にINSERTトランザクションが完了しない場合、このパラメータを調整してタイムアウトを延長できます。 |

- **セッション変数**

| セッション変数         | 説明                                                                 |
| -------------------- | ------------------------------------------------------------------- |
| enable_insert_strict | INSERTトランザクションが無効なデータ行に対して寛容であるかどうかを制御するスイッチ値です。`true`に設定されている場合、データ行のいずれかが無効であるとトランザクションは失敗します。`false`に設定されている場合、少なくとも1行のデータが正しくロードされた場合にトランザクションは成功し、ラベルが返されます。デフォルトは`true`です。この変数は`SET enable_insert_strict = {true or false};`コマンドで設定できます。 |
| query_timeout        | SQLコマンドのタイムアウト。単位：秒。INSERTはSQLコマンドとして、このセッション変数によっても制限されます。この変数は`SET query_timeout = xxx;`コマンドで設定できます。 |
