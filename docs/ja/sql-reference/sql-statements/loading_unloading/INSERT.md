---
displayed_sidebar: docs
---

# INSERT

## Description

特定のテーブルにデータを挿入するか、特定のテーブルをデータで上書きします。v3.2.0以降、INSERTはリモートストレージ内のファイルにデータを書き込むことをサポートします。INSERT INTO FILES() を使用して、StarRocksからリモートストレージにデータをアンロードできます。

[SUBMIT TASK](ETL/SUBMIT_TASK.md) を使用して非同期のINSERTタスクを送信できます。

## Syntax

- **データロード**:

  ```sql
  INSERT { INTO | OVERWRITE } [db_name.]<table_name>
  [ PARTITION (<partition_name> [, ...] ) ]
  [ TEMPORARY PARTITION (<temporary_partition_name> [, ...] ) ]
  [ WITH LABEL <label>]
  [ (<column_name>[, ...]) | BY NAME ]
  [ PROPERTIES ("key"="value", ...) ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

- **データアンロード**:

  ```sql
  INSERT INTO FILES()
  [ WITH LABEL <label> ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

## Parameters

| Parameter     | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| INTO          | テーブルにデータを追加します。                                 |
| OVERWRITE     | テーブルをデータで上書きします。                            |
| table_name    | データをロードしたいテーブルの名前。テーブルが存在するデータベースと共に `db_name.table_name` として指定できます。 |
| PARTITION    |  データをロードしたいパーティション。複数のパーティションを指定でき、カンマ (,) で区切る必要があります。宛先テーブルに存在するパーティションに設定する必要があります。このパラメータを指定すると、データは指定されたパーティションにのみ挿入されます。このパラメータを指定しない場合、データはすべてのパーティションに挿入されます。 |
| TEMPORARY PARTITION |データをロードしたい[temporary partition](../../../table_design/data_distribution/Temporary_partition.md) の名前。複数の一時パーティションを指定でき、カンマ (,) で区切る必要があります。|
| label         | データロードトランザクションごとにデータベース内で一意の識別ラベル。指定しない場合、システムが自動的にトランザクションのラベルを生成します。トランザクションのラベルを指定することをお勧めします。そうしないと、接続エラーが発生して結果が返されない場合にトランザクションの状態を確認できません。`SHOW LOAD WHERE label="label"` ステートメントを使用してトランザクションの状態を確認できます。ラベルの命名規則については、[System Limits](../../System_limit.md) を参照してください。 |
| column_name   | データをロードする宛先カラムの名前。宛先テーブルに存在するカラムとして設定する必要があります。**`column_name` と `BY NAME` を両方指定することはできません。**<ul><li>`BY NAME` が指定されていない場合、宛先カラムは宛先カラム名に関係なく、ソースカラムに順番に1対1でマッピングされます。</li><li>`BY NAME` が指定されている場合、宛先カラムは同じ名前のソースカラムにマッピングされ、宛先とソーステーブルのカラム順に関係なくマッピングされます。</li><li>宛先カラムが指定されていない場合、デフォルト値は宛先テーブルのすべてのカラムです。</li><li>ソーステーブルで指定されたカラムが宛先カラムに存在しない場合、デフォルト値がこのカラムに書き込まれます。</li><li>指定されたカラムにデフォルト値がない場合、トランザクションは失敗します。</li><li>ソーステーブルのカラムタイプが宛先テーブルのカラムタイプと一致しない場合、システムは不一致のカラムに対して暗黙の変換を行います。</li><li>変換が失敗した場合、構文解析エラーが返されます。</li></ul>**NOTE**<br />v3.3.1以降、主キーテーブルでのINSERT INTOステートメントでカラムリストを指定すると、部分更新が実行されます（以前のバージョンでは完全アップサート）。カラムリストが指定されていない場合、システムは完全アップサートを実行します。 |
| BY NAME       | ソースと宛先のカラムを名前で一致させます。**`column_name` と `BY NAME` を両方指定することはできません。** 指定しない場合、宛先カラムは宛先カラム名に関係なく、ソースカラムに順番に1対1でマッピングされます。  |
| PROPERTIES    | INSERTジョブのプロパティ。各プロパティはキーと値のペアでなければなりません。サポートされているプロパティについては、[PROPERTIES](#properties) を参照してください。 |
| expression    | カラムに値を割り当てる式。                |
| DEFAULT       | カラムにデフォルト値を割り当てます。                         |
| query         | 結果が宛先テーブルにロードされるクエリステートメント。StarRocksがサポートする任意のSQLステートメントを使用できます。 |
| FILES()       | テーブル関数 [FILES()](../../sql-functions/table-functions/files.md)。この関数を使用して、リモートストレージにデータをアンロードできます。 |

### PROPERTIES

INSERTステートメントはv3.4.0以降、PROPERTIESの設定をサポートします。

| Property         | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| timeout          | INSERTジョブのタイムアウト時間。単位: 秒。INSERTのタイムアウト時間はセッション内またはグローバルにセッション変数 `insert_timeout` を使用して設定することもできます。 |
| strict_mode      | INSERT from FILES() を使用してデータをロードする際にストリクトモードを有効にするかどうか。 有効な値: `true` (デフォルト) および `false`。ストリクトモードが有効な場合、システムは適格な行のみをロードします。不適格な行をフィルタリングし、不適格な行の詳細を返します。詳細については、[Strict mode](../../../loading/load_concept/strict_mode.md) を参照してください。INSERT from FILES() のストリクトモードは、セッション内またはグローバルにセッション変数 `enable_insert_strict` を使用して有効にすることもできます。 |
| max_filter_ratio | INSERT from FILES() の最大エラー許容率。データ品質が不十分なためにフィルタリングされるデータレコードの最大比率です。不適格なデータレコードの比率がこの閾値に達すると、ジョブは失敗します。デフォルト値: `0`。範囲: [0, 1]。INSERT from FILES() の最大エラー許容率は、セッション内またはグローバルにセッション変数 `insert_max_filter_ratio` を使用して設定することもできます。 |

:::note

- `strict_mode` と `max_filter_ratio` は、INSERT from FILES() のみでサポートされます。テーブルからのINSERTはこれらのプロパティをサポートしていません。
- v3.4.0以降、`enable_insert_strict` が `true` に設定されている場合、システムは適格な行のみをロードします。不適格な行をフィルタリングし、不適格な行の詳細を返します。代わりに、v3.4.0以前のバージョンでは、`enable_insert_strict` が `true` に設定されている場合、不適格な行があるとINSERTジョブは失敗します。

:::

## Return

```Plain
Query OK, 5 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_test', 'status':'VISIBLE', 'txnId':'1008'}
```

| Return        | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| rows affected | ロードされた行数を示します。`warnings` はフィルタリングされた行を示します。 |
| label         | データロードトランザクションごとにデータベース内で一意の識別ラベル。ユーザーが割り当てることも、システムが自動的に割り当てることもできます。 |
| status        | ロードされたデータが可視であるかどうかを示します。`VISIBLE`: データが正常にロードされ、可視です。`COMMITTED`: データが正常にロードされましたが、現在は不可視です。 |
| txnId         | 各INSERTトランザクションに対応するID番号。      |

## Usage notes

- 現在のバージョンでは、StarRocksがINSERT INTOステートメントを実行する際、データのいずれかの行が宛先テーブルのフォーマットと一致しない場合（例えば、文字列が長すぎる場合）、デフォルトでINSERTトランザクションは失敗します。セッション変数 `enable_insert_strict` を `false` に設定すると、システムは宛先テーブルのフォーマットと一致しないデータをフィルタリングし、トランザクションの実行を続行します。

- INSERT OVERWRITEステートメントが実行されると、StarRocksは元のデータを格納するパーティションのために一時パーティションを作成し、一時パーティションにデータを挿入し、元のパーティションと一時パーティションを入れ替えます。これらの操作はすべてLeader FEノードで実行されます。したがって、Leader FEノードがINSERT OVERWRITEステートメントを実行中にクラッシュすると、ロードトランザクション全体が失敗し、一時パーティションが削除されます。

### Dynamic Overwrite

v3.4.0以降、StarRocksはパーティションテーブルに対するINSERT OVERWRITEの新しいセマンティクス - Dynamic Overwriteをサポートします。

現在、INSERT OVERWRITEのデフォルトの動作は次のとおりです:

- パーティションテーブル全体を上書きする場合（つまり、PARTITION句を指定しない場合）、新しいデータレコードは対応するパーティションのデータを置き換えます。関与していないパーティションがある場合、それらは切り捨てられ、他のものは上書きされます。
- 空のパーティションテーブルを上書きする場合（つまり、パーティションがない場合）でPARTITION句を指定すると、システムはエラー `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'` を返します。
- パーティションテーブルを上書きし、PARTITION句で存在しないパーティションを指定すると、システムはエラー `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'` を返します。
- パーティションテーブルを上書きし、PARTITION句で指定されたパーティションと一致しないデータレコードがある場合、システムはエラー `ERROR 1064 (HY000): Insert has filtered data in strict mode`（ストリクトモードが有効な場合）を返すか、不適格なデータレコードをフィルタリングします（ストリクトモードが無効な場合）。

新しいDynamic Overwriteセマンティクスの動作は大きく異なります:

パーティションテーブル全体を上書きする場合、新しいデータレコードは対応するパーティションのデータを置き換えます。関与していないパーティションがある場合、それらはそのまま残され、切り捨てられたり削除されたりしません。そして、新しいデータレコードが存在しないパーティションに対応する場合、システムはそのパーティションを作成します。

Dynamic Overwriteセマンティクスはデフォルトでは無効です。有効にするには、システム変数 `dynamic_overwrite` を `true` に設定する必要があります。

現在のセッションでDynamic Overwriteを有効にする:

```SQL
SET dynamic_overwrite = true;
```

INSERT OVERWRITEステートメントのヒントに設定して、ステートメントにのみ適用させることもできます。

例:

```SQL
INSERT OVERWRITE /*+set_var(set dynamic_overwrite = false)*/ insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

## Example

### Example 1: 一般的な使用法

以下の例は、`test` テーブルを基にしており、`c1` と `c2` の2つのカラムを含んでいます。`c2` カラムはデフォルト値としてDEFAULTを持っています。

- `test` テーブルに1行のデータをインポートします。

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

宛先カラムが指定されていない場合、カラムはデフォルトで宛先テーブルに順番にロードされます。したがって、上記の例では、最初と2番目のSQLステートメントの結果は同じです。

宛先カラム（データが挿入されるかどうかに関係なく）がDEFAULTを値として使用する場合、カラムはデフォルト値をロードされたデータとして使用します。したがって、上記の例では、3番目と4番目のステートメントの出力は同じです。

- `test` テーブルに一度に複数の行のデータをロードします。

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
INSERT INTO test (c1) VALUES (1), (3);
```

式の結果が同等であるため、最初と2番目のステートメントの結果は同じです。3番目と4番目のステートメントの結果も同じで、どちらもデフォルト値を使用しています。

- クエリステートメントの結果を `test` テーブルにインポートします。

```SQL
INSERT INTO test SELECT * FROM test2;
INSERT INTO test (c1, c2) SELECT * from test2;
```

- クエリ結果を `test` テーブルにインポートし、パーティションとラベルを指定します。

```SQL
INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
```

- クエリ結果で `test` テーブルを上書きし、パーティションとラベルを指定します。

```SQL
INSERT OVERWRITE test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test3;
INSERT OVERWRITE test WITH LABEL `label1` (c1, c2) SELECT * from test3;
```

### Example 2: AWS S3からParquetファイルをINSERT from FILES() でロードする

以下の例では、AWS S3バケット `inserttest` 内のParquetファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入します:

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "ap-southeast-1"
);
```

### Example 3: INSERTタイムアウト

以下の例では、ソーステーブル `source_wiki_edit` からターゲットテーブル `insert_wiki_edit` にデータを挿入し、タイムアウト時間を `2` 秒に設定します。

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "timeout" = "2"
)
SELECT * FROM source_wiki_edit;
```

大規模なデータセットを取り込む場合、`timeout` またはセッション変数 `insert_timeout` に対してより大きな値を設定できます。

### Example 4: INSERTストリクトモードと最大フィルタ比率

以下の例では、AWS S3バケット `inserttest` 内のParquetファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入し、ストリクトモードを有効にして不適格なデータレコードをフィルタリングし、最大10%のエラーデータを許容します:

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

### Example 5: INSERTでカラムを名前で一致させる

以下の例では、ソーステーブルとターゲットテーブルの各カラムを名前で一致させます:

```SQL
INSERT INTO insert_wiki_edit BY NAME
SELECT event_time, user, channel FROM source_wiki_edit;
```

この場合、`channel` と `user` の順序を変更してもカラムのマッピングは変わりません。