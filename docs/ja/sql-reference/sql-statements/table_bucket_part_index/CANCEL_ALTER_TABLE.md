---
displayed_sidebar: docs
---

# CANCEL ALTER TABLE

## 説明

進行中の ALTER TABLE 操作の実行をキャンセルします。これには以下が含まれます:

- カラムの変更。
- テーブルスキーマの最適化 (v3.2 から)、バケット方式やバケット数の変更を含む。
- ロールアップインデックスの作成と削除。

> **注意**
>
> - このステートメントは同期操作です。
> - このステートメントを実行するには、テーブルに対する `ALTER_PRIV` 権限が必要です。
> - このステートメントは、上記のように ALTER TABLE を使用した非同期操作のキャンセルのみをサポートし、リネームなどの ALTER TABLE を使用した同期操作のキャンセルはサポートしていません。

## 構文

   ```SQL
   CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name
   ```

## パラメータ

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - `COLUMN` が指定された場合、このステートメントはカラムの変更操作をキャンセルします。
  - `OPTIMIZE` が指定された場合、このステートメントはテーブルスキーマの最適化操作をキャンセルします。
  - `ROLLUP` が指定された場合、このステートメントはロールアップインデックスの追加または削除操作をキャンセルします。

- `db_name`: 任意。テーブルが属するデータベースの名前。このパラメータが指定されていない場合、デフォルトで現在のデータベースが使用されます。
- `table_name`: 必須。テーブル名。

## 例

1. データベース `example_db` の `example_table` に対するカラム変更操作をキャンセルします。

   ```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2. データベース `example_db` の `example_table` に対するテーブルスキーマの最適化操作をキャンセルします。

   ```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. 現在のデータベースの `example_table` に対するロールアップインデックスの追加または削除操作をキャンセルします。

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table;
   ```