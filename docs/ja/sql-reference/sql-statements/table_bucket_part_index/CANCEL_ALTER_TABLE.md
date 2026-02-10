---
displayed_sidebar: docs
---

# CANCEL ALTER TABLE

CANCEL ALTER TABLE は、実行中の ALTER TABLE 操作の実行をキャンセルします。対象となる操作は以下のとおりです。

- 列の変更。
- バケッティング手法やバケット数の変更など、テーブルスキーマの最適化 (v3.2 以降)。
- ロールアップインデックスの作成と削除。

:::note
- このステートメントは同期操作です。
- このステートメントを実行するには、テーブルに対する `ALTER_PRIV` 権限が必要です。
- このステートメントは、ALTER TABLE を使用した非同期操作のキャンセル (上記参照) のみをサポートし、名前の変更など、ALTER TABLE を使用した同期操作のキャンセルはサポートしません。
:::

## 構文

```SQL
CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name [ (rollup_job_id [, rollup_job_id]) ]
```

## パラメータ

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - `COLUMN` が指定された場合、このステートメントは列の変更操作をキャンセルします。
  - `OPTIMIZE` が指定された場合、このステートメントはテーブルスキーマの最適化操作をキャンセルします。
  - `ROLLUP` が指定された場合、このステートメントはrollup index の追加または削除操作をキャンセルします。特定のロールアップジョブをキャンセルするには、`rollup_job_id` をさらに指定できます。

- `db_name`: オプション。テーブルが属するデータベースの名前。このパラメータが指定されていない場合、現在のデータベースがデフォルトで使用されます。
- `table_name`: 必須。テーブル名。
- `rollup_job_id`: オプション。ロールアップジョブの ID。ロールアップジョブの ID は[SHOW ALTER MATERIALIZED VIEW](../materialized_view/SHOW_ALTER_MATERIALIZED_VIEW.md) を使用して取得できます。

## Examples

1. データベース `example_db` 内の `example_table` に対するカラム変更のオペレーションをキャンセルします。

```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2. データベース `example_db` 内の `example_table` に対するテーブルスキーマの最適化操作をキャンセルします。

```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. 現在のデータベースで、`example_table` のロールアップインデックスの追加または削除の操作をキャンセルします。

```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table;
   ```

4. 現在のデータベースにある `example_table` の特定のロールアップ変更を、ジョブ ID を使ってキャンセルします。

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table (12345, 12346);
   ```
