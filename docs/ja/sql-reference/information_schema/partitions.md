---
displayed_sidebar: docs
---

# partitions

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`partitions` はテーブルパーティションに関する情報を提供します。

`partitions` で提供されるフィールドは以下の通りです:

| **Field**                  | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| TABLE_CATALOG              | テーブルが属する catalog の名前。この値は常に `def` です。 |
| TABLE_SCHEMA               | テーブルが属するデータベースの名前。                         |
| TABLE_NAME                 | パーティションを含むテーブルの名前。                         |
| PARTITION_NAME             | パーティションの名前。                                       |
| SUBPARTITION_NAME          | `PARTITIONS` テーブルの行がサブパーティションを表す場合、サブパーティションの名前。それ以外の場合は `NULL`。`NDB` の場合: この値は常に `NULL` です。 |
| PARTITION_ORDINAL_POSITION | すべてのパーティションは定義された順序でインデックスが付けられ、`1` が最初のパーティションに割り当てられた番号です。パーティションが追加、削除、再編成されるとインデックスが変更される可能性があり、この列に表示される番号は、インデックスの変更を考慮した現在の順序を反映しています。 |
| PARTITION_METHOD           | 有効な値: `RANGE`、`LIST`、`HASH`、`LINEAR HASH`、`KEY`、または `LINEAR KEY`。 |
| SUBPARTITION_METHOD        | 有効な値: `HASH`、`LINEAR HASH`、`KEY`、または `LINEAR KEY`  |
| PARTITION_EXPRESSION       | テーブルの現在のパーティショニングスキームを作成した `CREATE TABLE` または `ALTER TABLE` ステートメントで使用されるパーティショニング関数の式。 |
| SUBPARTITION_EXPRESSION    | サブパーティションの定義に使用されるサブパーティショニング式に対して `PARTITION_EXPRESSION` がテーブルのパーティショニングを定義するために使用されるのと同様に機能します。テーブルにサブパーティションがない場合、この列は `NULL` です。 |
| PARTITION_DESCRIPTION      | この列は RANGE および LIST パーティションに使用されます。`RANGE` パーティションの場合、パーティションの `VALUES LESS THAN` 節に設定された値が含まれ、整数または `MAXVALUE` である可能性があります。`LIST` パーティションの場合、この列にはパーティションの `VALUES IN` 節で定義された値が含まれ、カンマで区切られた整数値のリストです。`PARTITION_METHOD` が `RANGE` または `LIST` 以外のパーティションの場合、この列は常に `NULL` です。 |
| TABLE_ROWS                 | パーティション内のテーブル行の数。                           |
| AVG_ROW_LENGTH             | このパーティションまたはサブパーティションに格納されている行の平均長さ（バイト単位）。これは `DATA_LENGTH` を `TABLE_ROWS` で割ったものと同じです。 |
| DATA_LENGTH                | このパーティションまたはサブパーティションに格納されているすべての行の総長（バイト単位）。つまり、パーティションまたはサブパーティションに格納されているバイトの総数です。 |
| MAX_DATA_LENGTH            | このパーティションまたはサブパーティションに格納できる最大バイト数。 |
| INDEX_LENGTH               | このパーティションまたはサブパーティションのインデックスファイルの長さ（バイト単位）。 |
| DATA_FREE                  | パーティションまたはサブパーティションに割り当てられたが使用されていないバイト数。 |
| CREATE_TIME                | パーティションまたはサブパーティションが作成された時間。     |
| UPDATE_TIME                | パーティションまたはサブパーティションが最後に変更された時間。 |
| CHECK_TIME                 | このパーティションまたはサブパーティションが属するテーブルが最後にチェックされた時間。 |
| CHECKSUM                   | チェックサム値がある場合、その値。ない場合は `NULL`。        |
| PARTITION_COMMENT          | パーティションにコメントがある場合、そのテキスト。ない場合、この値は空です。 |
| NODEGROUP                  | パーティションが属するノードグループ。                       |
| TABLESPACE_NAME            | パーティションが属するテーブルスペースの名前。               |