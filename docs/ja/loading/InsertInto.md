---
displayed_sidebar: docs
---

# INSERT を使用したデータのロード

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

このトピックでは、SQL ステートメント - INSERT を使用して StarRocks にデータをロードする方法について説明します。

MySQL や他の多くのデータベース管理システムと同様に、StarRocks は INSERT を使用して内部テーブルにデータをロードすることをサポートしています。VALUES 句を使用して 1 行または複数行を直接挿入し、関数やデモをテストできます。また、クエリの結果で定義されたデータを [外部テーブル](../data_source/External_table.md)から内部テーブルに挿入することもできます。StarRocks v3.1 以降、INSERT コマンドとテーブル関数 [FILES()](../sql-reference/sql-functions/table-functions/files.md) を使用して、クラウドストレージ上のファイルから直接データをロードできます。

StarRocks v2.4 では、INSERT OVERWRITE を使用してテーブルにデータを上書きすることもサポートしています。INSERT OVERWRITE ステートメントは、上書き機能を実装するために次の操作を統合します。

1. 元のデータを格納するパーティションに従って一時パーティションを作成します。
2. データを一時パーティションに挿入します。
3. 元のパーティションを一時パーティションと交換します。

> **注意**
>
> データを上書きする前に検証する必要がある場合は、INSERT OVERWRITE を使用する代わりに、上記の手順に従ってデータを上書きし、パーティションを交換する前に検証できます。

v3.4.0 以降、StarRocks はパーティション化されたテーブルに対する INSERT OVERWRITE の新しいセマンティクス - Dynamic Overwrite をサポートしています。詳細については、[Dynamic Overwrite](#dynamic-overwrite) を参照してください。

## 注意事項

- 同期 INSERT トランザクションをキャンセルするには、MySQL クライアントから **Ctrl** と **C** キーを押す必要があります。
- [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) を使用して非同期 INSERT タスクを送信できます。
- 現在の StarRocks のバージョンでは、任意の行のデータがテーブルのスキーマに準拠していない場合、INSERT トランザクションはデフォルトで失敗します。たとえば、任意の行のフィールドの長さがテーブルのマッピングフィールドの長さ制限を超える場合、INSERT トランザクションは失敗します。セッション変数 `enable_insert_strict` を `false` に設定すると、テーブルと一致しない行をフィルタリングしてトランザクションを続行できます。
- StarRocks に小さなデータバッチをロードするために INSERT ステートメントを頻繁に実行すると、過剰なデータバージョンが生成されます。これはクエリパフォーマンスに深刻な影響を与えます。運用環境では、INSERT コマンドを使用してデータを頻繁にロードしたり、日常的なデータロードのルーチンとして使用したりしないことをお勧めします。アプリケーションや分析シナリオがストリーミングデータや小さなデータバッチを個別にロードするソリューションを必要とする場合は、Apache Kafka® をデータソースとして使用し、Routine Load を介してデータをロードすることをお勧めします。
- INSERT OVERWRITE ステートメントを実行すると、StarRocks は元のデータを格納するパーティションに対して一時パーティションを作成し、新しいデータを一時パーティションに挿入し、[元のパーティションを一時パーティションと交換します](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#use-a-temporary-partition-to-replace-the-current-partition)。これらの操作はすべて FE Leader ノードで実行されます。したがって、FE Leader ノードが INSERT OVERWRITE コマンドの実行中にクラッシュすると、ロードトランザクション全体が失敗し、一時パーティションが切り捨てられます。

## 準備

### 権限の確認

<InsertPrivNote />

### オブジェクトの作成

`load_test` という名前のデータベースを作成し、宛先テーブルとして `insert_wiki_edit` テーブルを、ソーステーブルとして `source_wiki_edit` テーブルを作成します。

> **注意**
>
> このトピックで示されている例は、`insert_wiki_edit` テーブルと `source_wiki_edit` テーブルに基づいています。独自のテーブルとデータを使用することを希望する場合は、準備をスキップして次のステップに進むことができます。

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
> v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

## INSERT INTO VALUES を使用したデータの挿入

INSERT INTO VALUES コマンドを使用して、特定のテーブルに 1 行または複数行を追加できます。複数行はカンマ (,) で区切られます。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

> **注意**
>
> INSERT INTO VALUES を使用したデータの挿入は、小さなデータセットでデモを検証する必要がある場合にのみ適用されます。大規模なテストや運用環境には推奨されません。StarRocks に大量のデータをロードするには、[ロードオプション](Loading_intro.md)を参照して、シナリオに適した他のオプションを確認してください。

次の例では、ラベル `insert_load_wikipedia` を使用して、データソーステーブル `source_wiki_edit` に 2 行を挿入します。ラベルは、データベース内の各データロードトランザクションの一意の識別ラベルです。

```SQL
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

## INSERT INTO SELECT を使用したデータの挿入

INSERT INTO SELECT コマンドを使用して、データソーステーブルのクエリ結果をターゲットテーブルにロードできます。INSERT INTO SELECT コマンドは、データソーステーブルのデータに対して ETL 操作を行い、StarRocks の内部テーブルにデータをロードします。データソースは、1 つまたは複数の内部または外部テーブル、さらにはクラウドストレージ上のデータファイルである可能性があります。ターゲットテーブルは、StarRocks の内部テーブルである必要があります。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

### 内部または外部テーブルから内部テーブルへのデータの挿入

> **注意**
>
> 外部テーブルからのデータの挿入は、内部テーブルからのデータの挿入と同じです。簡単のため、以下の例では内部テーブルからのデータの挿入方法のみを示します。

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` にデータを挿入します。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_1
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` の `p06` および `p12` パーティションにデータを挿入します。パーティションが指定されていない場合、データはすべてのパーティションに挿入されます。指定されたパーティションにのみデータが挿入されます。

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

`p06` および `p12` パーティションを切り捨てると、クエリでデータは返されません。

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` に `event_time` および `channel` 列を挿入します。指定されていない列にはデフォルト値が使用されます。

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
v3.3.1 以降、主キーテーブルに対する INSERT INTO ステートメントで列リストを指定すると、部分更新が実行されます（以前のバージョンでは完全アップサート）。列リストが指定されていない場合、システムは完全アップサートを実行します。
:::

### FILES() を使用して外部ソースのファイルから直接データを挿入

v3.1 以降、StarRocks は INSERT コマンドと [FILES()](../sql-reference/sql-functions/table-functions/files.md) 関数を使用して、クラウドストレージ上のファイルから直接データをロードすることをサポートしています。これにより、外部カタログやファイル外部テーブルを最初に作成する必要がなくなります。さらに、FILES() はファイルのテーブルスキーマを自動的に推測できるため、データロードのプロセスが大幅に簡素化されます。

次の例では、AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入します。

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

## INSERT OVERWRITE VALUES を使用したデータの上書き

INSERT OVERWRITE VALUES コマンドを使用して、特定のテーブルを 1 行または複数行で上書きできます。複数行はカンマ (,) で区切られます。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

> **注意**
>
> INSERT OVERWRITE VALUES を使用したデータの上書きは、小さなデータセットでデモを検証する必要がある場合にのみ適用されます。大規模なテストや運用環境には推奨されません。StarRocks に大量のデータをロードするには、[ロードオプション](Loading_intro.md)を参照して、シナリオに適した他のオプションを確認してください。

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

次の例では、ソーステーブル `source_wiki_edit` を 2 つの新しい行で上書きします。

```SQL
INSERT OVERWRITE source_wiki_edit
WITH LABEL insert_load_wikipedia_ow
VALUES
    ("2015-09-12 00:00:00","#cn.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),
    ("2015-09-12 00:00:00","#fr.wikipedia","PereBot",0,1,0,1,0,17,17,0);
```

## INSERT OVERWRITE SELECT を使用したデータの上書き

INSERT OVERWRITE SELECT コマンドを使用して、データソーステーブルのクエリ結果でテーブルを上書きできます。INSERT OVERWRITE SELECT ステートメントは、1 つまたは複数の内部または外部テーブルからのデータに対して ETL 操作を行い、内部テーブルをデータで上書きします。詳細な手順とパラメータの参照については、[SQL リファレンス - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を参照してください。

> **注意**
>
> 外部テーブルからのデータのロードは、内部テーブルからのデータのロードと同じです。簡単のため、以下の例では内部テーブルからのデータでターゲットテーブルを上書きする方法のみを示します。

ソーステーブルとターゲットテーブルをクエリして、異なる行のデータを保持していることを確認します。

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

- 次の例では、ソーステーブルのデータでテーブル `insert_wiki_edit` を上書きします。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_1
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルのデータでテーブル `insert_wiki_edit` の `p06` および `p12` パーティションを上書きします。

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

`p06` および `p12` パーティションを切り捨てると、クエリでデータは返されません。

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

:::note
`PARTITION BY column` 戦略を使用するテーブルの場合、INSERT OVERWRITE はパーティションキーの値を指定することで、宛先テーブルに新しいパーティションを作成することをサポートします。既存のパーティションは通常どおり上書きされます。

次の例では、パーティション化されたテーブル `activity` を作成し、データを挿入しながらテーブルに新しいパーティションを作成します。

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

- 次の例では、ソーステーブルの `event_time` および `channel` 列でターゲットテーブル `insert_wiki_edit` を上書きします。デフォルト値は、データが上書きされない列に割り当てられます。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

### Dynamic Overwrite

v3.4.0 以降、StarRocks はパーティション化されたテーブルに対する INSERT OVERWRITE の新しいセマンティクス - Dynamic Overwrite をサポートしています。

現在、INSERT OVERWRITE のデフォルトの動作は次のとおりです。

- パーティション化されたテーブル全体を上書きする場合（つまり、PARTITION 句を指定せずに）、新しいデータレコードは対応するパーティションのデータを置き換えます。関与していないパーティションがある場合、それらは上書きされる一方で切り捨てられます。
- 空のパーティション化されたテーブルを上書きする場合（つまり、パーティションがない状態で）PARTITION 句を指定すると、システムはエラー `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'` を返します。
- パーティション化されたテーブルを上書きし、PARTITION 句で存在しないパーティションを指定すると、システムはエラー `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'` を返します。
- PARTITION 句で指定されたパーティションと一致しないデータレコードでパーティション化されたテーブルを上書きする場合、システムはエラー `ERROR 1064 (HY000): Insert has filtered data in strict mode`（ストリクトモードが有効な場合）を返すか、資格のないデータレコードをフィルタリングします（ストリクトモードが無効な場合）。

新しい Dynamic Overwrite セマンティクスの動作は大きく異なります。

パーティション化されたテーブル全体を上書きする場合、新しいデータレコードは対応するパーティションのデータを置き換えます。関与していないパーティションがある場合、それらはそのまま残され、切り捨てられたり削除されたりしません。また、新しいデータレコードが存在しないパーティションに対応する場合、システムはそのパーティションを作成します。

Dynamic Overwrite セマンティクスはデフォルトでは無効です。有効にするには、システム変数 `dynamic_overwrite` を `true` に設定する必要があります。

現在のセッションで Dynamic Overwrite を有効にする:

```SQL
SET dynamic_overwrite = true;
```

INSERT OVERWRITE ステートメントのヒントに設定して、ステートメントに対してのみ有効にすることもできます。

例:

```SQL
INSERT OVERWRITE /*+set_var(set dynamic_overwrite = false)*/ insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

## 生成列を持つテーブルへのデータの挿入

生成列は、他の列に基づいて事前定義された式または評価から値が導出される特別な列です。生成列は、クエリ要求が高価な式の評価を含む場合に特に便利です。たとえば、JSON 値から特定のフィールドをクエリしたり、ARRAY データを計算したりする場合です。StarRocks は、データがテーブルにロードされる際に式を評価し、生成列に結果を格納することで、クエリ中の式評価を回避し、クエリパフォーマンスを向上させます。

INSERT を使用して、生成列を持つテーブルにデータをロードできます。

次の例では、テーブル `insert_generated_columns` を作成し、1 行を挿入します。このテーブルには、2 つの生成列 `avg_array` と `get_string` が含まれています。`avg_array` は `data_array` の ARRAY データの平均値を計算し、`get_string` は `data_json` の JSON パス `a` から文字列を抽出します。

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

## PROPERTIES を使用したデータの挿入

v3.4.0 以降、INSERT ステートメントは PROPERTIES の設定をサポートしており、さまざまな目的に使用できます。PROPERTIES は対応する変数をオーバーライドします。

### ストリクトモードの有効化

v3.4.0 以降、FILES() からの INSERT に対してストリクトモードを有効にし、`max_filter_ratio` を設定できます。FILES() からの INSERT のストリクトモードは、他のロード方法と同じ動作を持ちます。

資格のない行を含むデータセットをロードしたい場合、これらの資格のない行をフィルタリングするか、資格のない列に NULL 値を割り当ててロードすることができます。これを実現するには、プロパティ `strict_mode` と `max_filter_ratio` を使用します。

- 資格のない行をフィルタリングするには: `strict_mode` を `true` に設定し、`max_filter_ratio` を希望の値に設定します。
- 資格のない行をすべて NULL 値でロードするには: `strict_mode` を `false` に設定します。

次の例では、AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入し、ストリクトモードを有効にして資格のないデータレコードをフィルタリングし、最大 10% のエラーデータを許容します。

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "strict_mode" = "true",
    "max_filter_ratio" = "0.1"
)
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "XXXXXXXXXX",
    "aws.s3.secret_key" = "YYYYYYYYYY",
    "aws.s3.region" = "us-west-2"
);
```

:::note

`strict_mode` と `max_filter_ratio` は、FILES() からの INSERT のみをサポートしています。テーブルからの INSERT はこれらのプロパティをサポートしていません。

:::

### タイムアウト期間の設定

v3.4.0 以降、INSERT ステートメントのタイムアウト期間を設定できます。v3.4.0 より前のバージョンでは、INSERT ステートメントのタイムアウト期間はシステム変数 `query_timeout` によって制御されていました。

次の例では、ソーステーブル `source_wiki_edit` からターゲットテーブル `insert_wiki_edit` にデータを挿入し、タイムアウト期間を `2` 秒に設定します。

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "timeout" = "2"
)
SELECT * FROM source_wiki_edit;
```

:::note

v3.4.0 以降、システム変数 `insert_timeout` は INSERT に関与する操作（たとえば、UPDATE、DELETE、CTAS、マテリアライズドビューのリフレッシュ、統計収集、PIPE）に適用され、`query_timeout` に置き換わります。

:::

### 名前で列を一致させる

デフォルトでは、INSERT はソーステーブルとターゲットテーブルの列をその位置で一致させます。つまり、ステートメント内の列のマッピングです。

次の例では、ソーステーブルとターゲットテーブルの各列をその位置で明示的に一致させます。

```SQL
INSERT INTO insert_wiki_edit (
    event_time,
    channel,
    user
)
SELECT event_time, channel, user FROM source_wiki_edit;
```

`channel` と `user` の順序を列リストまたは SELECT ステートメントのいずれかで変更すると、列のマッピングが変更されます。

```SQL
INSERT INTO insert_wiki_edit (
    event_time,
    channel,
    user
)
SELECT event_time, user, channel FROM source_wiki_edit;
```

この場合、ターゲットテーブル `insert_wiki_edit` の `channel` は、ソーステーブル `source_wiki_edit` の `user` からのデータで埋められるため、取り込まれたデータはおそらく望んでいるものではありません。

INSERT ステートメントに `BY NAME` 句を追加することで、システムはソーステーブルとターゲットテーブルの列名を検出し、同じ名前の列を一致させます。

:::note

- `BY NAME` が指定されている場合、列リストを指定することはできません。
- `BY NAME` が指定されていない場合、システムは列リストと SELECT ステートメント内の列の位置で列を一致させます。

:::

次の例では、ソーステーブルとターゲットテーブルの各列をその名前で一致させます。

```SQL
INSERT INTO insert_wiki_edit BY NAME
SELECT event_time, user, channel FROM source_wiki_edit;
```

この場合、`channel` と `user` の順序を変更しても、列のマッピングは変更されません。

## INSERT を使用して非同期でデータをロード

INSERT を使用したデータのロードは同期トランザクションを送信しますが、セッションの中断やタイムアウトにより失敗する可能性があります。[SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) を使用して非同期 INSERT トランザクションを送信できます。この機能は StarRocks v2.5 以降でサポートされています。

- 次の例では、ソーステーブルからターゲットテーブル `insert_wiki_edit` にデータを非同期で挿入します。

```SQL
SUBMIT TASK AS INSERT INTO insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルのデータでテーブル `insert_wiki_edit` を非同期で上書きします。

```SQL
SUBMIT TASK AS INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルのデータでテーブル `insert_wiki_edit` を非同期で上書きし、ヒントを使用してクエリのタイムアウトを `100000` 秒に延長します。

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- 次の例では、ソーステーブルのデータでテーブル `insert_wiki_edit` を非同期で上書きし、タスク名を `async` と指定します。

```SQL
SUBMIT TASK async
AS INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

Information Schema のメタデータビュー `task_runs` をクエリして、非同期 INSERT タスクのステータスを確認できます。

次の例では、INSERT タスク `async` のステータスを確認します。

```SQL
SELECT * FROM information_schema.task_runs WHERE task_name = 'async';
```

## INSERT ジョブのステータスを確認

### 結果による確認

同期 INSERT トランザクションは、トランザクションの結果に応じて異なるステータスを返します。

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

`tracking_url` を使用してログを確認し、問題を特定できます。

### Information Schema による確認

[SELECT](../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) ステートメントを使用して、`information_schema` データベースの `loads` テーブルから 1 つ以上のロードジョブの結果をクエリできます。この機能は v3.1 以降でサポートされています。

例 1: `load_test` データベースで実行されたロードジョブの結果をクエリし、作成時間 (`CREATE_TIME`) で降順にソートし、トップの結果のみを返します。

```SQL
SELECT * FROM information_schema.loads
WHERE database_name = 'load_test'
ORDER BY create_time DESC
LIMIT 1\G
```

例 2: `load_test` データベースで実行されたロードジョブ（ラベルが `insert_load_wikipedia`）の結果をクエリします。

```SQL
SELECT * FROM information_schema.loads
WHERE database_name = 'load_test' and label = 'insert_load_wikipedia'\G
```

返り値は次のとおりです。

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

返り値のフィールドに関する情報は、[Information Schema > loads](../sql-reference/information_schema/loads.md) を参照してください。

### curl コマンドによる確認

curl コマンドを使用して INSERT トランザクションのステータスを確認できます。

ターミナルを起動し、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
  http://<fe_address>:<fe_http_port>/api/<db_name>/_load_info?label=<label_name>
```

次の例では、ラベル `insert_load_wikipedia` のトランザクションのステータスを確認します。

```Bash
curl --location-trusted -u <username>:<password> \
  http://x.x.x.x:8030/api/load_test/_load_info?label=insert_load_wikipedia
```

> **注意**
>
> パスワードが設定されていないアカウントを使用する場合は、`<username>:` のみを入力する必要があります。

返り値は次のとおりです。

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

INSERT トランザクションに対して次の設定項目を設定できます。

- **FE 設定**

| FE 設定                           | 説明                                                                 |
| ---------------------------------- | ------------------------------------------------------------------- |
| insert_load_default_timeout_second | INSERT トランザクションのデフォルトのタイムアウト。単位: 秒。このパラメータで設定された時間内に現在の INSERT トランザクションが完了しない場合、システムによってキャンセルされ、ステータスは CANCELLED になります。StarRocks の現在のバージョンでは、このパラメータを使用してすべての INSERT トランザクションに対して一様なタイムアウトを指定することしかできず、特定の INSERT トランザクションに対して異なるタイムアウトを設定することはできません。デフォルトは 3600 秒（1 時間）です。指定された時間内に INSERT トランザクションが完了しない場合、このパラメータを調整してタイムアウトを延長できます。 |

- **セッション変数**

| セッション変数         | 説明                                                                 |
| -------------------- | ------------------------------------------------------------------- |
| enable_insert_strict | INSERT トランザクションが無効なデータ行を許容するかどうかを制御するスイッチ値です。`true` に設定されている場合、データ行のいずれかが無効な場合、トランザクションは失敗します。`false` に設定されている場合、少なくとも 1 行のデータが正しくロードされた場合、トランザクションは成功し、ラベルが返されます。デフォルトは `true` です。この変数は `SET enable_insert_strict = {true or false};` コマンドで設定できます。 |
| query_timeout        | SQL コマンドのタイムアウト。単位: 秒。INSERT は SQL コマンドとして、このセッション変数によっても制約されます。この変数は `SET query_timeout = xxx;` コマンドで設定できます。 |
```