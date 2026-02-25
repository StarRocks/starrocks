---
displayed_sidebar: docs
---

# pipe_files

`pipe_files` は、指定されたパイプを介してロードされるデータファイルのステータスを提供します。このビューは StarRocks v3.2 以降でサポートされています。

`pipe_files` には以下のフィールドが提供されています:

| **Field**        | **Description**                                              |
| ---------------- | ------------------------------------------------------------ |
| DATABASE_NAME    | パイプが格納されているデータベースの名前。                   |
| PIPE_ID          | パイプのユニークな ID。                                      |
| PIPE_NAME        | パイプの名前。                                               |
| FILE_NAME        | データファイルの名前。                                       |
| FILE_VERSION     | データファイルのダイジェスト。                               |
| FILE_SIZE        | データファイルのサイズ。単位: バイト。                       |
| LAST_MODIFIED    | データファイルが最後に変更された時刻。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_STATE       | データファイルのロードステータス。有効な値: `UNLOADED`、`LOADING`、`FINISHED`、`ERROR`。 |
| STAGED_TIME      | データファイルがパイプによって最初に記録された日時。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| START_LOAD_TIME  | データファイルのロードが開始された日時。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| FINISH_LOAD_TIME | データファイルのロードが完了した日時。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| ERROR_MSG        | データファイルのロードエラーに関する詳細。                   |