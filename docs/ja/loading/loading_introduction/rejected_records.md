---
sidebar_position: 60
displayed_sidebar: docs
sidebar_label: "拒否レコード"
keywords: ['rejected', 'records', 'max_filter_ratio', 'replay']
description: "max_filter_ratio > 0 の場合に StarRocks がフィルタリングされた行を _statistics_.rejected_records に永続化する方法、および拒否レコードを照会・リプレイする方法。"
---

# 拒否レコード

`max_filter_ratio` が 0 でないロードジョブでは、フィルタリングされたすべての行をシステムテーブル **`_statistics_.rejected_records`** に永続化するように設定できます。これにより、ジョブ全体を再実行せずに不良データを確認し、対象テーブルへリプレイできます。このトピックでは、拒否レコードのキャプチャ機能を有効化する方法、テーブルを照会する方法、および SQL で拒否された行をリプレイする方法を説明します。

`_statistics_.rejected_records` には、以下から拒否されたデータの情報が格納されます。

- Stream Load、Routine Load、Broker Load、および `INSERT`（`INSERT INTO ... SELECT ... FROM FILES()` を含む）
- Scanner の解析失敗（CSV の列数不一致、型変換エラー、strict mode によるフィルタリング）
- Sink 制約違反（NOT NULL、パーティション範囲の不一致、VARCHAR 長、DECIMAL 精度）
- ORC リーダーによる行の拒否（列指向フォーマット、フィルタ適用前）

## 拒否レコードのキャプチャを有効化する

拒否レコードのキャプチャはデフォルトで無効になっています。これにより、明示的に有効化するまで新しいクラスターがこのシステムテーブルに書き込むことはありません。

この機能を有効にするには、次の手順に従ってください。

1. BE 設定項目 `enable_rejected_record_sync` を `true` に設定して、拒否レコードの同期（システムテーブルへの書き込み）を有効化します。

   - 動的に変更するには、次の SQL を実行します。

     ```SQL
     UPDATE information_schema.be_configs SET VALUE = "true" WHERE name = "enable_rejected_record_sync";
     ```

   - 永続的に変更するには、`be.conf` に以下の設定を追加し、BE サービスを再起動してください。

     ```Properties
     enable_rejected_record_sync=true
     ```

2. セッションまたはロードジョブに対して、`log_rejected_record_num` を正の数（上限値）または `-1`（無制限）に設定します。

   ```sql
   -- INSERT / INSERT ... SELECT 用のセッションレベル設定
   SET log_rejected_record_num = -1;

   -- Broker Load のプロパティ
   LOAD LABEL mydb.my_label ( ... )
   PROPERTIES (
       "log_rejected_record_num" = "10000"
   );

   -- Routine Load のプロパティ
   CREATE ROUTINE LOAD mydb.my_job ON my_table ...
   PROPERTIES (
       "log_rejected_record_num" = "10000",
       ...
   );

   -- Stream Load のヘッダー
   curl -H "log_rejected_record_num: 10000" ...
   ```

拒否された行は `_statistics_.rejected_records` に同期され、その後の操作に利用できます。

## `_statistics_.rejected_records` を照会する

まず、次の SQL を実行して `_statistics_.rejected_records` のテーブルスキーマを確認できます。

```SQL
DESC _statistics_.rejected_records;
```

日常的なトリアージでは、次の列に注目してください。

| 列           | 説明                             |
| ------------ | --------------------------------------- |
| `raw_record` | 拒否された行を、列名をキーとした JSON 形式で格納します。 |
| `error_code`<br />`error_message`<br />`error_column` | 行が拒否された理由。 |
| `load_label`<br />`load_type`<br />`txn_id` | 拒否された行を生成したロードジョブのラベル、タイプ、およびトランザクション ID。Broker Load、Routine Load、および INSERT ジョブでは `user_name` が NULL のままになります。`load_label` で `_statistics_.rejected_records` と `_statistics_.loads_history` を結合すると `user_name` を取得できます。 |
| `source_info` | ロード元に関する情報。ファイルロードでは `file` と `line`、Routine Load では `topic`、`partition`、`offset` です。 |
| `created_at` | レコードが生成された時刻。このシステムテーブルのパーティションキーです。まずこの列でフィルタすることを推奨します。 |

以下の例では、このテーブルの基本的な使用方法を示します。

- 特定のロードの拒否された行を確認し、最新のレコードを先頭に表示します。

  ```SQL
  SELECT created_at, error_code, error_column, error_message, raw_record
  FROM _statistics_.rejected_records
  WHERE load_label = 'load_orders_20260327'
  ORDER BY created_at DESC
  LIMIT 100;
  ```

- 過去 24 時間の対象テーブルのエラー分布を確認します。

```SQL
SELECT error_code, error_column, COUNT(*) AS cnt
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND created_at >= NOW() - INTERVAL 1 DAY
GROUP BY error_code, error_column
ORDER BY cnt DESC;
```

- `information_schema.loads` との結合クエリに基づいて、あるロードジョブの拒否された行をすべて確認します。`information_schema.loads` は `txn_id` を公開していないため、ロードラベルで結合する必要があります。

```SQL
SELECT r.created_at, r.error_code, r.raw_record, l.state, l.scan_rows
FROM _statistics_.rejected_records AS r
JOIN information_schema.loads AS l
  ON r.load_label = l.label
WHERE r.load_label = 'my_load_label_2026_04_28';
```

## 拒否された行をリプレイする

`raw_record` 列は、拒否された行の列値を列名をキーとして格納した JSON 文字列です。`->>` 演算子で値を文字列として抽出し、`CAST(... AS <型>)` で対象の型に復元します。

次の例では、VARCHAR の長さ違反を切り詰めによって修正し、対象テーブルにその値を INSERT します。

```SQL
INSERT INTO mydb.orders (order_id, customer_name, amount, created_at)
SELECT
    CAST(raw_record->>'order_id'      AS BIGINT),
    LEFT(raw_record->>'customer_name', 64),
    CAST(raw_record->>'amount'        AS DECIMAL(10,2)),
    CAST(raw_record->>'created_at'    AS DATETIME)
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND error_code    = 'VALUE_OUT_OF_RANGE'
  AND created_at    > '2026-03-27';
```

Scanner が行をまったく分割できなかった場合（たとえば CSV で列数が一致しない場合）、`raw_record` には行の生データを保持する単一のキー `_raw` が格納されます。

次の例は、診断のために解析できなかった行の先頭 20 行を表示します。

```SQL
SELECT raw_record->>'_raw' AS raw_line
FROM _statistics_.rejected_records
WHERE error_code = 'PARSE_ERROR'
ORDER BY created_at DESC
LIMIT 20;
```

## データの保持とクリーンアップ

`_statistics_.rejected_records` は日単位でパーティション化されており、プロパティ `partition_live_number = 7` に基づいてパーティションが自動的に期限切れになります。FE 設定項目 `rejected_records_retained_days`（デフォルト `7`）を変更することで保持期間を調整できます。table-keeper
デーモンは、次回のティックでこのライブテーブルプロパティを反映します。

次の例では、特定の対象テーブルで指定した日付より前のレコードを削除します。

```sql
DELETE FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table    = 'orders'
  AND created_at      < '2026-03-01';
```

## 権限

`_statistics_.rejected_records` へのアクセスは、組み込みの行アクセスポリシーによって制御されます。

- 組み込みの **`root`** ユーザーは、このテーブルのすべての行を参照できます（このポリシーは root にはフィルタを適用しません）。
- **その他のすべてのユーザー**（`db_admin`、`cluster_admin`、`user_admin`、`security_admin` ロールを持つユーザーを含む）は、`SELECT` 権限を持つ `target_database.target_table` に対応する行のみを参照できます。ユーザーが `SELECT` 権限を持たないテーブルの行は結果セットから除外されます。
- ポリシーが行の対象を解決または検証できない場合、その行は非表示になります（フェイルクローズ、つまりデフォルトで拒否されます）。

運用者向けダッシュボードでは、完全な可視性が必要な場合は管理者アカウントを使用するか、レポート用ロールに対象テーブルの `SELECT` 権限を付与してください。

## 制限事項

- **Parquet の拒否レコードは、完全な行ではなくアンカー情報のみを保持します。**

  Parquet ロードでは、問題のある列の生の値を単一列の `raw_record` フラグメントに記録し、さらに `source_info` にソースアンカーを**追加**します。

  ```json
  {
    "format": "parquet",
    "file": "gs://bucket/orders.parquet",
    "row_in_file": 1817542,
    "file_size": 12345678,
    "file_mtime_ms": 1711531331000
  }
  ```

  `raw_record` だけでも、列ごとに何が問題だったかを診断するには十分です。行全体をリプレイするには、後続のコミットでアンカーを使用してソースファイルを再読み込みし、行全体を復元する `parquet_read_rows(file, anchors)` TVF が提供される予定です。この TVF が実装されるまでは、アンカーは元の Parquet ファイル内の正確な行をユーザーに示す（`row_in_file` は 0 始まり）ため、およびソースファイルが変更されていないことを検証する（`file_size` と `file_mtime_ms` はスキャン開始時にスナップショットされるため、手動でのリハイドレーションを試みる前に比較する必要があります）ために依然として有用です。

- **`information_schema.loads.rejected_record_path` は非推奨です。**

  この列が以前指していた BE ローカルのタブ区切り拒否レコードファイルは削除されました。この列はアップグレード互換性のために残されていますが、常に `NULL` になります。代わりに `load_label` または `txn_id` で `_statistics_.rejected_records` を直接照会してください。

- **秒単位の遅延。**

  拒否された行は、ロードが完了してから `rejected_record_sync_interval_sec`（デフォルト 30 秒）以内にシステムテーブルで照会可能になります（即時ではありません）。

## 関連する設定

| スコープ            | パラメータ                               | デフォルト | 説明                                                |
| ---------------- | --------------------------------------- | ------- | ---------------------------------------------------------- |
| セッション変数 | `log_rejected_record_num`               | `0`     | 記録する拒否レコードの数。`0` はこの機能を無効化することを示します。`-1` は無制限に記録することを示します。 |
| FE 設定項目 | `rejected_records_retained_days`        | `7`     | `_statistics_.rejected_records` に保持する日次パーティション数。 |
| BE 設定項目 | `enable_rejected_record_sync`           | `false` | 拒否された行を `_statistics_.rejected_records` に同期するデーモンのマスタースイッチ。 |
| BE 設定項目 | `rejected_record_sync_interval_sec`     | `30`    | 同期のティック間隔。                 |
| BE 設定項目 | `rejected_record_sync_max_batch_rows`   | `10000` | Merge Commit バッチあたりの行数のソフト上限。               |
| BE 設定項目 | `rejected_record_local_retention_hours` | `24`    | 同期できないレコードのローカルファイル GC（クリーンアップ）。 |
| BE 設定項目 | `rejected_record_sync_post_timeout_sec` | `60`    | リクエストごとの Stream Load タイムアウト。                           |
