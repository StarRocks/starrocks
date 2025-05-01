---
displayed_sidebar: docs
---

# INSERT

## 説明

特定のテーブルにデータを挿入するか、特定のテーブルをデータで上書きします。アプリケーションのシナリオについての詳細は、 [Load data with INSERT](../../../loading/InsertInto.md) を参照してください。

## 構文

```Bash
INSERT {INTO | OVERWRITE} table_name
[ PARTITION (<partition1_name>[, <partition2_name>, ...]) ]
[ TEMPORARY_PARTITION (<temporary_partition1_name>[, <temporary_partition2_name>, ...]) ]
[ WITH LABEL label]
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

## パラメータ

| **パラメータ** | 説明 |
| ------------- | ------------------------------------------------------------ |
| INTO          | テーブルにデータを追加します。 |
| OVERWRITE     | テーブルをデータで上書きします。 |
| table_name    | データをロードしたいテーブルの名前です。テーブルが存在するデータベースと一緒に `db_name.table_name` として指定できます。 |
| PARTITION    | データをロードしたいパーティションです。複数のパーティションを指定でき、カンマ (,) で区切る必要があります。宛先テーブルに存在するパーティションに設定する必要があります。このパラメータを指定すると、データは指定されたパーティションにのみ挿入されます。このパラメータを指定しない場合、データはすべてのパーティションに挿入されます。 |
| TEMPORARY_PARTITION|データをロードしたい [temporary partition](../../../table_design/Temporary_partition.md) の名前です。複数の一時パーティションを指定でき、カンマ (,) で区切る必要があります。|
| label         | データロードトランザクションごとにデータベース内で一意の識別ラベルです。指定されていない場合、システムが自動的にトランザクション用のラベルを生成します。接続エラーが発生し結果が返されない場合にトランザクションの状態を確認できないため、トランザクションのラベルを指定することをお勧めします。トランザクションの状態は `SHOW LOAD WHERE label="label"` ステートメントで確認できます。ラベルの命名に関する制限については、 [System Limits](../../../reference/System_limit.md) を参照してください。 |
| column_name   | データをロードする宛先列の名前です。宛先テーブルに存在する列として設定する必要があります。指定した宛先列は、宛先列名に関係なく、ソーステーブルの列と順番に一対一でマッピングされます。宛先列が指定されていない場合、デフォルト値は宛先テーブルのすべての列です。ソーステーブルで指定された列が宛先列に存在しない場合、その列にはデフォルト値が書き込まれ、指定された列にデフォルト値がない場合、トランザクションは失敗します。ソーステーブルの列タイプが宛先テーブルの列タイプと一致しない場合、システムは不一致の列に対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。 |
| expression    | 列に値を割り当てる式です。 |
| DEFAULT       | 列にデフォルト値を割り当てます。 |
| query         | 結果が宛先テーブルにロードされるクエリステートメントです。StarRocks がサポートする任意の SQL ステートメントを使用できます。 |

## 戻り値

```Plain
Query OK, 5 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_test', 'status':'VISIBLE', 'txnId':'1008'}
```

| 戻り値        | 説明 |
| ------------- | ------------------------------------------------------------ |
| rows affected | ロードされた行数を示します。`warnings` はフィルタリングされた行を示します。 |
| label         | データロードトランザクションごとにデータベース内で一意の識別ラベルです。ユーザーが割り当てるか、システムが自動的に割り当てることができます。 |
| status        | ロードされたデータが表示可能かどうかを示します。`VISIBLE`: データが正常にロードされ表示可能です。`COMMITTED`: データが正常にロードされましたが、現在は表示されません。 |
| txnId         | 各 INSERT トランザクションに対応する ID 番号です。 |

## 使用上の注意

- 現在のバージョンでは、StarRocks が INSERT INTO ステートメントを実行する際、データのいずれかの行が宛先テーブルの形式と一致しない場合（例えば、文字列が長すぎる場合）、デフォルトで INSERT トランザクションは失敗します。セッション変数 `enable_insert_strict` を `false` に設定すると、システムは宛先テーブルの形式と一致しないデータをフィルタリングし、トランザクションの実行を続行します。

- INSERT OVERWRITE ステートメントが実行されると、StarRocks は元のデータを格納するパーティションのために一時パーティションを作成し、一時パーティションにデータを挿入し、元のパーティションを一時パーティションと交換します。これらの操作はすべて Leader FE ノードで実行されます。したがって、Leader FE ノードが INSERT OVERWRITE ステートメントを実行中にクラッシュした場合、全体のロードトランザクションは失敗し、一時パーティションは削除されます。

## 例

以下の例は、2 つの列 `c1` と `c2` を含むテーブル `test` に基づいています。`c2` 列にはデフォルト値 DEFAULT があります。

- `test` テーブルに 1 行のデータをインポートします。

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

宛先列が指定されていない場合、列はデフォルトで順番に宛先テーブルにロードされます。したがって、上記の例では、最初と 2 番目の SQL ステートメントの結果は同じです。

宛先列（データが挿入されるかどうかに関係なく）が DEFAULT を値として使用する場合、その列はデフォルト値をロードされたデータとして使用します。したがって、上記の例では、3 番目と 4 番目のステートメントの出力は同じです。

- `test` テーブルに複数の行のデータを一度にロードします。

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
INSERT INTO test (c1) VALUES (1), (3);
```

式の結果が同等であるため、最初と 2 番目のステートメントの結果は同じです。3 番目と 4 番目のステートメントの結果も同じです。どちらもデフォルト値を使用しています。

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