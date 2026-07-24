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

警告バッファは直前のステートメントの診断情報を保持し、次に診断情報を生成しうるステートメントが実行を開始すると置き換えられます（解析に失敗したステートメントは、それ自身のエラーで置き換えます）。テーブルを使用せずメッセージも生成しないステートメント——`SET`、`BEGIN`/`COMMIT`/`ROLLBACK`、および `SHOW WARNINGS` と `SHOW ERRORS` 自身を含む `SHOW` ステートメント——は、MySQL と同様にバッファを変更しません。そのため `SHOW WARNINGS` は繰り返し発行でき、`COMMIT` の後でも直前のロードの診断情報を返します。

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
