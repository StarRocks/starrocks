---
displayed_sidebar: docs
---

# SHOW PIPES

## 説明

指定されたデータベースまたは現在使用中のデータベースに保存されているパイプを一覧表示します。このコマンドはバージョン 3.2 以降でサポートされています。

## 構文

```SQL
SHOW PIPES [FROM <db_name>]
[
   WHERE [ NAME { = "<pipe_name>" | LIKE "pipe_matcher" } ]
         [ [AND] STATE = { "SUSPENDED" | "RUNNING" | "ERROR" } ]
]
[ ORDER BY <field_name> [ ASC | DESC ] ]
[ LIMIT { [offset, ] limit | limit OFFSET offset } ]
```

## パラメータ

### FROM `<db_name>`

クエリしたいパイプのデータベース名を指定します。このパラメータを指定しない場合、システムは現在使用中のデータベースのパイプを返します。

### WHERE

パイプをクエリするための条件です。

### ORDER BY `<field_name>`

返されるレコードをソートするフィールドです。

### LIMIT

システムが返すレコードの最大数です。

## 戻り結果

コマンドの出力は以下のフィールドで構成されます。

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| DATABASE_NAME | パイプが保存されているデータベースの名前です。               |
| PIPE_ID       | パイプのユニークな ID です。                                 |
| PIPE_NAME     | パイプの名前です。                                           |
| TABLE_NAME    | データが送られる先の StarRocks テーブルの名前です。           |
| STATE         | パイプのステータスです。有効な値は `RUNNING`、`FINISHED`、`SUSPENDED`、`ERROR` です。 |
| LOAD_STATUS   | パイプを介してロードされるデータファイルの全体的なステータスで、以下のサブフィールドを含みます:<ul><li>`loadedFiles`: ロードされたデータファイルの数。</li><li>`loadedBytes`: ロードされたデータの量（バイト単位）。</li><li>`LastLoadedTime`: 最後にデータファイルがロードされた日時。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。</li></ul> |
| LAST_ERROR    | パイプの実行中に発生した最後のエラーの詳細です。デフォルト値: `NULL`。 |
| CREATED_TIME  | パイプが作成された日時です。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |

## 例

### すべてのパイプをクエリ

`mydatabase` という名前のデータベースに切り替え、その中のすべてのパイプを表示します:

```SQL
USE mydatabase;
SHOW PIPES \G
```

### 指定されたパイプをクエリ

`mydatabase` という名前のデータベースに切り替え、その中の `user_behavior_replica` という名前のパイプを表示します:

```SQL
USE mydatabase;
SHOW PIPES WHERE NAME = 'user_behavior_replica' \G
```

## 参考

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)