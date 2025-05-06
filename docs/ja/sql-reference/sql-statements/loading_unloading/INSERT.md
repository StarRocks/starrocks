---
displayed_sidebar: docs
---

# INSERT

## Description

特定のテーブルにデータを挿入するか、特定のテーブルをデータで上書きします。v3.2.0以降、INSERTはリモートストレージ内のファイルにデータを書き込むことをサポートしています。INSERT INTO FILES() を使用して、StarRocksからリモートストレージにデータをアンロードできます。

[SUBMIT TASK](ETL/SUBMIT_TASK.md) を使用して非同期のINSERTタスクを送信できます。

## Syntax

- **データロード**:

  ```sql
  INSERT { INTO | OVERWRITE } [db_name.]<table_name>
  [ PARTITION (<partition_name> [, ...] ) ]
  [ TEMPORARY PARTITION (<temporary_partition_name> [, ...] ) ]
  [ WITH LABEL <label>]
  [ (<column_name>[, ...]) ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

- **データアンロード**:

  ```sql
  INSERT INTO FILES()
  [ WITH LABEL <label> ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

## Parameters

| **Parameter** | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| INTO          | テーブルにデータを追加します。                               |
| OVERWRITE     | テーブルをデータで上書きします。                             |
| table_name    | データをロードしたいテーブルの名前です。テーブルが存在するデータベースと共に `db_name.table_name` として指定できます。 |
| PARTITION    | データをロードしたいパーティションです。複数のパーティションを指定でき、カンマ (,) で区切る必要があります。これを指定すると、データは指定されたパーティションにのみ挿入されます。指定しない場合、データはすべてのパーティションに挿入されます。 |
| TEMPORARY PARTITION | データをロードしたい [temporary partition](../../../table_design/data_distribution/Temporary_partition.md) の名前です。複数の一時パーティションを指定でき、カンマ (,) で区切る必要があります。|
| label         | データロードトランザクションごとにデータベース内で一意の識別ラベルです。指定しない場合、システムが自動的に生成します。トランザクションのラベルを指定することをお勧めします。そうしないと、接続エラーが発生して結果が返されない場合にトランザクションの状態を確認できません。`SHOW LOAD WHERE label="label"` ステートメントでトランザクションの状態を確認できます。ラベルの命名規則については、[System Limits](../../System_limit.md) を参照してください。 |
| column_name   | データをロードする宛先の列名です。宛先テーブルに存在する列として設定する必要があります。指定した宛先列は、宛先列名に関係なく、ソーステーブルの列と順番に一対一でマッピングされます。宛先列が指定されていない場合、デフォルト値は宛先テーブルのすべての列です。ソーステーブルで指定された列が宛先列に存在しない場合、デフォルト値がこの列に書き込まれ、指定された列にデフォルト値がない場合、トランザクションは失敗します。ソーステーブルの列タイプが宛先テーブルの列タイプと一致しない場合、システムは不一致の列に対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。<br />**NOTE**<br />v3.3.1以降、主キーテーブルでINSERT INTOステートメントに列リストを指定すると、部分更新が実行されます（以前のバージョンではフルアップサート）。列リストが指定されていない場合、システムはフルアップサートを実行します。 |
| expression    | 列に値を割り当てる式です。                                   |
| DEFAULT       | 列にデフォルト値を割り当てます。                             |
| query         | 結果が宛先テーブルにロードされるクエリステートメントです。StarRocksがサポートする任意のSQLステートメントを使用できます。 |
| FILES()       | テーブル関数 [FILES()](../../sql-functions/table-functions/files.md)。この関数を使用してデータをリモートストレージにアンロードできます。 |

## Return

```Plain
Query OK, 5 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_test', 'status':'VISIBLE', 'txnId':'1008'}
```

| Return        | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| rows affected | ロードされた行数を示します。`warnings` はフィルタリングされた行を示します。 |
| label         | データロードトランザクションごとにデータベース内で一意の識別ラベルです。ユーザーが割り当てることも、システムが自動的に割り当てることもできます。 |
| status        | ロードされたデータが可視かどうかを示します。`VISIBLE`: データが正常にロードされ、可視です。`COMMITTED`: データが正常にロードされましたが、現在は不可視です。 |
| txnId         | 各INSERTトランザクションに対応するID番号です。               |

## Usage notes

- 現在のバージョンでは、StarRocksがINSERT INTOステートメントを実行する際、データのいずれかの行が宛先テーブルの形式と一致しない場合（例えば、文字列が長すぎる場合）、デフォルトでINSERTトランザクションは失敗します。セッション変数 `enable_insert_strict` を `false` に設定すると、システムは宛先テーブルの形式と一致しないデータをフィルタリングし、トランザクションの実行を続行します。

- INSERT OVERWRITEステートメントが実行されると、StarRocksは元のデータを格納するパーティションのために一時パーティションを作成し、データを一時パーティションに挿入し、元のパーティションと一時パーティションを交換します。これらの操作はすべてLeader FEノードで実行されます。そのため、Leader FEノードがINSERT OVERWRITEステートメントの実行中にクラッシュすると、全体のロードトランザクションが失敗し、一時パーティションが削除されます。

## Example

以下の例は、`test` テーブルを基にしており、2つの列 `c1` と `c2` を含んでいます。`c2` 列にはデフォルト値が設定されています。

- `test` テーブルに1行のデータをインポートします。

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

宛先列が指定されていない場合、列はデフォルトで順番に宛先テーブルにロードされます。したがって、上記の例では、最初と2番目のSQLステートメントの結果は同じです。

宛先列（データが挿入されているかどうかに関係なく）がDEFAULTを値として使用する場合、列はデフォルト値をロードされたデータとして使用します。したがって、上記の例では、3番目と4番目のステートメントの出力は同じです。

- `test` テーブルに一度に複数行のデータをロードします。

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
INSERT INTO test (c1) VALUES (1), (3);
```

式の結果が等しいため、最初と2番目のステートメントの結果は同じです。3番目と4番目のステートメントの結果も同じで、どちらもデフォルト値を使用しています。

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

以下の例では、AWS S3バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** から `insert_wiki_edit` テーブルにデータ行を挿入します:

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