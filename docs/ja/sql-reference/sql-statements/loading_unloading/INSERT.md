---
displayed_sidebar: docs
---

# INSERT

## Description

特定のテーブルにデータを挿入するか、特定のテーブルをデータで上書きします。

非同期の INSERT タスクを [SUBMIT TASK](ETL/SUBMIT_TASK.md) を使用して送信できます。

## Syntax

```Bash
INSERT { INTO | OVERWRITE } [db_name.]<table_name>
[ PARTITION (<partition_name> [, ...) ]
[ TEMPORARY PARTITION (<temporary_partition_name>[, ...) ]
[ WITH LABEL <label>]
[ (<column_name>[, ...]) ]
{ VALUES ( { <expression> | DEFAULT }[, ...] )
  | <query> }
```

## Parameters

| **Parameter** | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| INTO          | テーブルにデータを追加します。                               |
| OVERWRITE     | テーブルをデータで上書きします。                             |
| table_name    | データをロードしたいテーブルの名前です。テーブルが存在するデータベースと一緒に `db_name.table_name` として指定できます。 |
| PARTITION    | データをロードしたいパーティションです。複数のパーティションを指定でき、カンマ (,) で区切る必要があります。これは、宛先テーブルに存在するパーティションに設定する必要があります。このパラメータを指定すると、データは指定されたパーティションにのみ挿入されます。このパラメータを指定しない場合、データはすべてのパーティションに挿入されます。 |
| TEMPORARY PARTITION|データをロードしたい [temporary partition](../../../table_design/data_distribution/Temporary_partition.md) の名前です。複数の一時パーティションを指定でき、カンマ (,) で区切る必要があります。|
| label         | データベース内の各データ ロード トランザクションの一意の識別ラベルです。指定しない場合、システムが自動的にトランザクション用に生成します。トランザクションのラベルを指定することをお勧めします。そうしないと、接続エラーが発生して結果が返されない場合にトランザクションのステータスを確認できません。`SHOW LOAD WHERE label="label"` ステートメントを使用してトランザクションのステータスを確認できます。ラベルの命名に関する制限については、[System Limits](../../System_limit.md) を参照してください。 |
| column_name   | データをロードする宛先列の名前です。宛先テーブルに存在する列として設定する必要があります。指定した宛先列は、宛先列名に関係なく、ソーステーブルの列に順番に一対一でマッピングされます。宛先列が指定されていない場合、デフォルト値は宛先テーブルのすべての列です。ソーステーブルで指定された列が宛先列に存在しない場合、デフォルト値がこの列に書き込まれ、指定された列にデフォルト値がない場合、トランザクションは失敗します。ソーステーブルの列タイプが宛先テーブルの列タイプと一致しない場合、システムは不一致の列に対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。 |
| expression    | 列に値を割り当てる式です。                                   |
| DEFAULT       | 列にデフォルト値を割り当てます。                             |
| query         | 結果が宛先テーブルにロードされるクエリステートメントです。StarRocks がサポートする任意の SQL ステートメントを使用できます。 |

## Return

```Plain
Query OK, 5 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_test', 'status':'VISIBLE', 'txnId':'1008'}
```

| Return        | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| rows affected | ロードされた行数を示します。`warnings` はフィルタリングされた行を示します。 |
| label         | データベース内の各データ ロード トランザクションの一意の識別ラベルです。ユーザーが割り当てることも、システムが自動的に割り当てることもできます。 |
| status        | ロードされたデータが表示可能かどうかを示します。`VISIBLE`: データが正常にロードされ、表示可能です。`COMMITTED`: データが正常にロードされましたが、現在は表示されていません。 |
| txnId         | 各 INSERT トランザクションに対応する ID 番号です。          |

## Usage notes

- 現在のバージョンでは、StarRocks が INSERT INTO ステートメントを実行する際、データのいずれかの行が宛先テーブルの形式と一致しない場合 (例えば、文字列が長すぎる場合)、INSERT トランザクションはデフォルトで失敗します。セッション変数 `enable_insert_strict` を `false` に設定すると、システムは宛先テーブルの形式と一致しないデータをフィルタリングし、トランザクションの実行を続行します。

- INSERT OVERWRITE ステートメントが実行されると、StarRocks は元のデータを格納するパーティション用の一時パーティションを作成し、一時パーティションにデータを挿入し、元のパーティションを一時パーティションと交換します。これらの操作はすべて Leader FE ノードで実行されます。したがって、Leader FE ノードが INSERT OVERWRITE ステートメントを実行中にクラッシュした場合、ロードトランザクション全体が失敗し、一時パーティションが削除されます。

## Example

次の例は、`test` テーブルに基づいており、2 つの列 `c1` と `c2` を含んでいます。`c2` 列にはデフォルト値 DEFAULT があります。

- `test` テーブルに 1 行のデータをインポートします。

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

宛先列が指定されていない場合、列はデフォルトで宛先テーブルに順番にロードされます。したがって、上記の例では、最初と 2 番目の SQL ステートメントの結果は同じです。

宛先列 (データが挿入されているかどうかに関係なく) が DEFAULT を値として使用する場合、列はデフォルト値をロードされたデータとして使用します。したがって、上記の例では、3 番目と 4 番目のステートメントの出力は同じです。

- `test` テーブルに複数行のデータを一度にロードします。

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
INSERT INTO test (c1) VALUES (1), (3);
```

式の結果が同等であるため、最初と 2 番目のステートメントの結果は同じです。3 番目と 4 番目のステートメントの結果も同じです。なぜなら、どちらもデフォルト値を使用しているからです。

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

次の例では、AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** から `insert_wiki_edit` テーブルにデータ行を挿入します。

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