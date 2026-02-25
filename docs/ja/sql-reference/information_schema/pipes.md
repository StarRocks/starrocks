---
displayed_sidebar: docs
---

# pipes

`pipes` は、現在のデータベースまたは指定されたデータベースに保存されているすべてのパイプに関する情報を提供します。このビューは StarRocks v3.2 以降でサポートされています。

:::note

指定されたデータベースまたは使用中の現在のデータベースに保存されているパイプを表示するには、[SHOW PIPES](../../sql-reference/sql-statements/loading_unloading/pipe/SHOW_PIPES.md) を使用することもできます。このコマンドも v3.2 以降でサポートされています。

:::

`pipes` には以下のフィールドが提供されています:

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| DATABASE_NAME | パイプが保存されているデータベースの名前。                   |
| PIPE_ID       | パイプのユニークな ID。                                      |
| PIPE_NAME     | パイプの名前。                                               |
| TABLE_NAME    | 送信先テーブルの名前。形式: `<database_name>.<table_name>`。 |
| STATE         | パイプのステータス。有効な値: `RUNNING`, `FINISHED`, `SUSPENDED`, `ERROR`。 |
| LOAD_STATUS   | パイプを介してロードされるデータファイルの全体的なステータスで、以下のサブフィールドを含みます:<ul><li>`loadedFiles`: ロードされたデータファイルの数。</li><li>`loadedBytes`: ロードされたデータの量（バイト単位）。</li><li>`loadingFiles`: ロード中のデータファイルの数。</li></ul> |
| LAST_ERROR    | パイプ実行中に発生した最後のエラーに関する詳細。デフォルト値: `NULL`。 |
| CREATED_TIME  | パイプが作成された日時。形式: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。