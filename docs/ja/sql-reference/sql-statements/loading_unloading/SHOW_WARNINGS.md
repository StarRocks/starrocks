---
displayed_sidebar: docs
description: "SHOW WARNINGS と SHOW ERRORS は、現在のセッションで直前のステートメントが生成した診断情報（ロード中にフィルタリングされた行や NULL に置換された行など）を返します。"
---

# SHOW WARNINGS

`SHOW WARNINGS` は、現在のセッションで最後に実行されたステートメントが生成した診断情報（Note、Warning、Error）を表示します。`SHOW ERRORS` は同じステートメントですが、Error レベルの診断情報のみを返します。

これらのステートメントは MySQL の診断領域（Diagnostics Area）を実装しているため、MySQL クライアント、または JDBC ドライバの `Statement.getWarnings()` を通じて、直前のステートメントが報告した警告を取得できます。典型的なケースは、非 strict モードのロードや `INSERT` が行を暗黙的にフィルタリングしたり、範囲外の値を `NULL` に置換した場合です。操作は成功し、OK パケットはゼロでない `warning_count` を報告し、その詳細は `SHOW WARNINGS` を通じて確認できます。

## 構文

```SQL
SHOW WARNINGS [LIMIT [offset,] row_count]
SHOW ERRORS [LIMIT [offset,] row_count]
```

## 戻り値のフィールド

| フィールド | 説明                                                     |
| --------- | ------------------------------------------------------- |
| Level     | 診断情報のレベル：`Note`、`Warning`、または `Error`。       |
| Code      | 診断コード。                                              |
| Message   | 診断情報の可読な説明。                                     |

警告バッファは直前のステートメントの診断情報を保持し、次のステートメントが実行を開始すると置き換えられます。ステートメントが失敗した場合（解析に失敗した場合を含む）は、それ自身のエラーで置き換えられます。次の 3 種類のステートメントは、成功したときにバッファを変更しません。`SET`、`BEGIN`/`COMMIT`/`ROLLBACK`、および `SHOW WARNINGS` と `SHOW ERRORS` 自身を含む `SHOW` ステートメントです。そのため `SHOW WARNINGS` は繰り返し発行でき、`COMMIT` の後でも直前のロードの診断情報を返します。

## 例

非 strict モードで行をフィルタリングした `INSERT` が生成した警告を表示します。

```Plain
mysql> SHOW WARNINGS;
+---------+------+------------------------------------------------------------------------+
| Level   | Code | Message                                                                |
+---------+------+------------------------------------------------------------------------+
| Warning | 1265 | 3 row(s) filtered or substituted to NULL during load; tracking_url=... |
+---------+------+------------------------------------------------------------------------+
```

最大 1 行を返します。

```SQL
SHOW WARNINGS LIMIT 1;
```

Error レベルの診断情報のみを表示します。ステートメントが失敗した後、`SHOW ERRORS` はそのエラー（エラー応答で送信されたものと同じコードとメッセージ）を返します。

```Plain
mysql> SELECT * FROM no_such_table;
ERROR 5502 (42602): Getting analyzing error. Detail message: Unknown table 'no_such_table'.

mysql> SHOW ERRORS;
+-------+------+---------------------------------------------------------------------------+
| Level | Code | Message                                                                   |
+-------+------+---------------------------------------------------------------------------+
| Error | 5502 | Getting analyzing error. Detail message: Unknown table 'no_such_table'.  |
+-------+------+---------------------------------------------------------------------------+
```

## 制限事項

- ロードや `INSERT` の場合、OK パケットの `warning_count` はフィルタリングまたは置換された行数を表しますが、`SHOW WARNINGS` はそれらを要約した 1 行を返します。両者の値は一致しません。MySQL では `warning_count` は `SHOW WARNINGS` が返す行数と等しくなります。
- 診断情報は、そのステートメントを実行した FE に記録されます。セッションが Follower FE に接続している場合、`INSERT` やロードは Leader に転送され、警告は Leader 側に記録されるため、Follower 上の `SHOW WARNINGS` は空の結果を返します。読み取るには Leader FE に接続してください。
- データの読み取り中に発生する診断情報（`CAST` がオーバーフローして `NULL` になる場合など）は、まだ記録されません。`SHOW WARNINGS` が対象とするのは、ロードや `INSERT` でフィルタリングされた行と、失敗したステートメントのエラーです。
