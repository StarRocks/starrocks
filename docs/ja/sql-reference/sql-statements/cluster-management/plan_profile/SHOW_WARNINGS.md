---
displayed_sidebar: docs
description: "SHOW WARNINGS と SHOW ERRORS は、現在のセッションで直前に実行されたステートメントの診断情報を返します。INSERT がフィルタリングした行や NULL に置き換えた行などです。"
---

# SHOW WARNINGS

## 説明

`SHOW WARNINGS` は、現在のセッションで直前に実行されたステートメントが生成した診断情報 (note、warning、error) を表示します。`SHOW ERRORS` は同じステートメントですが、error レベルの診断情報のみを返します。

これらのステートメントは MySQL の diagnostics area を実装しています。そのため MySQL クライアント、または `Statement.getWarnings()` を通じて JDBC ドライバーが、直前のステートメントの診断情報を取得できます。バッファに書き込まれるのは次の 2 つです。`INSERT` が失敗せずに行をフィルタリングした、または範囲外の値を `NULL` に置き換えた場合と、ステートメントが失敗した場合のエラーです。

この操作を行うための特別な権限は必要ありません。

## 構文

```SQL
SHOW WARNINGS [LIMIT [offset,] row_count]
SHOW ERRORS [LIMIT [offset,] row_count]
```

## パラメータ

`LIMIT [offset,] row_count`: 最初の `offset` 件をスキップし、最大 `row_count` 件の診断情報を返します。`offset` は省略可能で、デフォルト値は `0` です。

## 戻り値

| フィールド | 説明                                               |
| ---------- | -------------------------------------------------- |
| Level      | 診断情報の重大度: `Note`、`Warning`、または `Error`。 |
| Code       | 診断情報のコード。                                 |
| Message    | 診断情報の可読な説明。                             |

## 使用上の注意

バッファは直前のステートメントの診断情報を保持し、次のステートメントが実行を開始すると置き換えられます。ステートメントが失敗した場合 (解析に失敗した場合を含む) は、それ自身のエラーで置き換えられます。次の 3 種類のステートメントは、成功したときにバッファを変更しません。`SET`、`BEGIN`/`COMMIT`/`ROLLBACK`、および `SHOW WARNINGS` と `SHOW ERRORS` 自身を含む `SHOW` ステートメントです。そのため `SHOW WARNINGS` は繰り返し発行でき、`COMMIT` の後でも直前の `INSERT` の診断情報を返します。`USE` によるデータベースの切り替えも、クライアントがステートメントとして送信するか MySQL の `COM_INIT_DB` コマンドとして送信するかにかかわらず、バッファを置き換えます。

- この方法でフィルタリングされた行を報告するのは `INSERT` だけです。Broker Load、Spark Load、Routine Load は非同期ジョブであり、送信したステートメントはジョブを登録するだけです。Stream Load には SQL セッションがありません。したがっていずれも `SHOW WARNINGS` が返せるものを残しません。これらには [SHOW LOAD](../../loading_unloading/SHOW_LOAD.md) を使用してください。
- `INSERT` は許可された場合にのみ行をフィルタリングします。デフォルトの `enable_insert_strict = true` と `insert_max_filter_ratio = 0` では、条件を満たさない行が 1 行でもあるとステートメント自体が失敗し、バッファには警告ではなくそのエラーが入ります。最初の例を参照してください。
- OK パケットの `warning_count` はフィルタリングまたは置換された行数を示しますが、`SHOW WARNINGS` はそれらをまとめた 1 行を返します。この 2 つの値は一致しません。MySQL では `warning_count` は `SHOW WARNINGS` が返す行数と等しくなります。
- 診断情報は、そのステートメントを実行したセッションに保持されます。セッションが Follower FE に接続している場合、`INSERT` は Leader に転送され、リクエストごとに作成されリクエスト終了時に破棄されるセッションで実行されます。そのため Follower 上の `SHOW WARNINGS` は空の結果を返し、Leader 側にも読み取れるセッションは残りません。Leader FE に直接接続したセッションから `INSERT` を実行するか、メッセージに含まれる tracking URL を開いてください。転送されたステートメントが失敗した場合も同様で、Follower は Leader のエラー応答をエラーコードなしで中継するため何も記録されず、`SHOW ERRORS` も空になります。
- データ読み取り中に発生する診断情報 (`CAST` がオーバーフローして `NULL` になる場合など) は、まだ記録されません。

## 例

例 1: `INSERT` がフィルタリングした行を読み取ります。デフォルトでは条件を満たさない行が 1 行あった時点でステートメントが失敗するため、2 つのセッション変数がいずれも必要です。

```Plain
mysql> SET enable_insert_strict = false;
Query OK, 0 rows affected (0.00 sec)

mysql> SET insert_max_filter_ratio = 0.5;
Query OK, 0 rows affected (0.00 sec)

mysql> INSERT INTO t_dst SELECT CAST(v AS INT) FROM t_src;
Query OK, 2 rows affected, 1 warning (0.36 sec)

mysql> SHOW WARNINGS;
+---------+------+----------------------------------------------------------------------------------------------------------------------------------------+
| Level   | Code | Message                                                                                                                                |
+---------+------+----------------------------------------------------------------------------------------------------------------------------------------+
| Warning | 1265 | 1 row(s) filtered or substituted to NULL during load; tracking_url=http://172.26.92.1:8040/api/_load_error_log?file=error_log_9a1c2b3d |
+---------+------+----------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

例 2: 診断情報を最大 1 件返します。

```SQL
SHOW WARNINGS LIMIT 1;
```

例 3: 失敗したステートメントのエラーを読み取ります。`SHOW ERRORS` は、エラー応答で送信されたものと同じコードとメッセージを報告します。

```Plain
mysql> SELECT * FROM no_such_table;
ERROR 5502 (42602): Getting analyzing error. Detail message: Unknown table 'example_db.no_such_table'.

mysql> SHOW ERRORS;
+-------+------+------------------------------------------------------------------------------------+
| Level | Code | Message                                                                            |
+-------+------+------------------------------------------------------------------------------------+
| Error | 5502 | Getting analyzing error. Detail message: Unknown table 'example_db.no_such_table'. |
+-------+------+------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
