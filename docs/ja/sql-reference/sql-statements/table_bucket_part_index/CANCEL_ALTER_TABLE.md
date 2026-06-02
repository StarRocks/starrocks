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
CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name [ (rollup_job_id [, rollup_job_id]) ] [ FORCE ]
```

## パラメータ

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - `COLUMN` が指定された場合、このステートメントは列の変更操作をキャンセルします。
  - `OPTIMIZE` が指定された場合、このステートメントはテーブルスキーマの最適化操作をキャンセルします。
  - `ROLLUP` が指定された場合、このステートメントはrollup index の追加または削除操作をキャンセルします。特定のロールアップジョブをキャンセルするには、`rollup_job_id` をさらに指定できます。

- `db_name`: オプション。テーブルが属するデータベースの名前。このパラメータが指定されていない場合、現在のデータベースがデフォルトで使用されます。
- `table_name`: 必須。テーブル名。
- `rollup_job_id`: オプション。ロールアップジョブの ID。ロールアップジョブの ID は[SHOW ALTER MATERIALIZED VIEW](../materialized_view/SHOW_ALTER_MATERIALIZED_VIEW.md) を使用して取得できます。
- `FORCE`: オプション。**運用担当者専用の緊急手段**で、共有データ (lake) テーブル上で `publish_version` が `FINISHED_REWRITING` 状態のまま恒久的にスタックした `COLUMN` alter ジョブ (例: `txnlog` の欠落、segment / SST ファイルの消失、永続ストレージ障害など) を強制的にキャンセルします。通常の `CANCEL ALTER TABLE` は `FINISHED_REWRITING` のジョブのキャンセルを拒否しますが、`FORCE` はそのガードをバイパスします。内部的には no-op publish (alter の変更を適用せずにパーティションの可視バージョンだけを進める) を実行してからジョブをキャンセルし、そのテーブルへの後続のロードのブロックを解除します。

  :::warning

  - `FORCE` は FE 設定 `enable_admin_skip_committed_txn` (デフォルト `false`) によって制御されます。リカバリ中のみ有効化し、終了後は直ちに無効化してください。この共有設定とトランザクションレベルの同種の緊急手段については [ADMIN SKIP COMMITTED TRANSACTION](../cluster-management/tablet_replica/ADMIN_SKIP_COMMITTED_TRANSACTION.md) を参照してください。
  - `FORCE` は共有データ (lake) テーブル上の `COLUMN` alter ジョブ (スキーマ変更および `enable_persistent_index` / `file_bundling` などのメタデータ alter) **のみ** をサポートします。`OPTIMIZE`、`ROLLUP`、マテリアライズドビューの alter には対応しておらず、これらに `FORCE` を使用すると拒否されます。
  - 強制キャンセルされた alter は破棄され、変更は反映されません。

  :::

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

5. 共有データ (lake) テーブル上で publish がスタックした `COLUMN` alter を強制的にキャンセルします (運用担当者専用、`enable_admin_skip_committed_txn=true` が必要)。

   ```SQL
   -- リカバリ中のみ緊急スイッチを有効化します。
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");

   CANCEL ALTER TABLE COLUMN FROM example_db.example_table FORCE;

   -- 終了後は直ちに無効化します。
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "false");
   ```
