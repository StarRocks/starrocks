---
displayed_sidebar: docs
---

# triggers

:::note

このビューは StarRocks の利用可能な機能には適用されません。

:::

`triggers` はトリガーに関する情報を提供します。

`triggers` には以下のフィールドが提供されています:

| **Field**                  | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| TRIGGER_CATALOG            | トリガーが属する catalog の名前。この値は常に `def` です。 |
| TRIGGER_SCHEMA             | トリガーが属するデータベースの名前。                         |
| TRIGGER_NAME               | トリガーの名前。                                             |
| EVENT_MANIPULATION         | トリガーイベント。これは、トリガーがアクティブになる関連テーブルでの操作の種類です。値は `INSERT`（行が挿入された）、`DELETE`（行が削除された）、または `UPDATE`（行が変更された）です。 |
| EVENT_OBJECT_CATALOG       | すべてのトリガーは正確に1つのテーブルに関連付けられています。このテーブルが存在する catalog。 |
| EVENT_OBJECT_SCHEMA        | すべてのトリガーは正確に1つのテーブルに関連付けられています。このテーブルが存在するデータベース。 |
| EVENT_OBJECT_TABLE         | トリガーが関連付けられているテーブルの名前。                 |
| ACTION_ORDER               | 同じテーブル上で同じ `EVENT_MANIPULATION` と `ACTION_TIMING` の値を持つトリガーのリスト内でのトリガーのアクションの順序位置。 |
| ACTION_CONDITION           | この値は常に `NULL` です。                                   |
| ACTION_STATEMENT           | トリガーがアクティブになると実行されるステートメント、つまりトリガー本体。このテキストは UTF-8 エンコーディングを使用します。 |
| ACTION_ORIENTATION         | この値は常に `ROW` です。                                    |
| ACTION_TIMING              | トリガーがトリガーイベントの前または後にアクティブになるかどうか。値は `BEFORE` または `AFTER` です。 |
| ACTION_REFERENCE_OLD_TABLE | この値は常に `NULL` です。                                   |
| ACTION_REFERENCE_NEW_TABLE | この値は常に `NULL` です。                                   |
| ACTION_REFERENCE_OLD_ROW   | 古い列の識別子。この値は常に `OLD` です。                    |
| ACTION_REFERENCE_NEW_ROW   | 新しい列の識別子。この値は常に `NEW` です。                  |
| CREATED                    | トリガーが作成された日時。これはトリガーの `DATETIME(2)` 値（百分の秒単位の小数部を含む）です。 |
| SQL_MODE                   | トリガーが作成されたときに有効だった SQL モード、およびトリガーが実行されるモード。 |
| DEFINER                    | `DEFINER` 句で指定されたユーザー（通常はトリガーを作成したユーザー）。 |
| CHARACTER_SET_CLIENT       |                                                              |
| COLLATION_CONNECTION       |                                                              |
| DATABASE_COLLATION         | トリガーが関連付けられているデータベースの照合順序。         |