---
displayed_sidebar: docs
---

# ALTER MATERIALIZED VIEW

ALTER MATERIALIZED VIEW では、次の操作を実行できます。

- 非同期マテリアライズドビューの名前を変更する。
- 非同期マテリアライズドビューのリフレッシュ戦略を変更する。
- 非同期マテリアライズドビューのステータスをアクティブまたは非アクティブに変更する。
- 2つの非同期マテリアライズドビュー間でアトミックなスワップを実行する。
- 非同期マテリアライズドビューのプロパティを変更する。

  このSQLステートメントを使用して、次のプロパティを変更できます。

  - `bloom_filter_columns`
  - `partition_ttl_number`
  - `partition_refresh_number`
  - `partition_refresh_strategy`
  - `resource_group`
  - `auto_refresh_partitions_limit`
  - `excluded_trigger_tables`
  - `mv_rewrite_staleness_second`
  - `unique_constraints`
  - `foreign_key_constraints`
  - `colocate_with`
  - `excluded_refresh_tables`
  - `task_priority`
  - すべてのセッション変数関連のプロパティ。セッション変数の詳細については、[システム変数](../../System_variable.md) を参照してください。

:::tip

- この操作には、ターゲットのマテリアライズドビューに対する ALTER 権限が必要です。[GRANT](../account-management/GRANT.md) の手順に従って、この権限を付与できます。
- ALTER MATERIALIZED VIEW は、マテリアライズドビューの構築に使用されるクエリステートメントを直接変更することはサポートしていません。新しいマテリアライズドビューを構築し、ALTER MATERIALIZED VIEW SWAP WITH を使用して元のマテリアライズドビューとスワップできます。

:::

## 構文

```SQL
ALTER MATERIALIZED VIEW [db_name.]<mv_name> 
    { RENAME [db_name.]<new_mv_name> 
    | REFRESH <new_refresh_scheme_desc> 
    | ACTIVE | INACTIVE 
    | SWAP WITH [db_name.]<mv2_name>
    | SET ( "<key>" = "<value>"[,...]) }
```

## パラメータ

| **パラメータ**           | **必須** | **説明**

## 例

例1: マテリアライズドビューの名前を変更します。

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

例2：マテリアライズドビューのリフレッシュ間隔を変更します。

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

例3：マテリアライズドビューのリフレッシュタスクのタイムアウト時間を1時間（デフォルト）に変更します。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.insert_timeout" = "3600");
```

例 4：マテリアライズドビューのステータスをアクティブに変更します。

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```

例5：マテリアライズドビュー `order_mv` と `order_mv1` 間でアトミックな交換を実行します。

```SQL
ALTER MATERIALIZED VIEW order_mv SWAP WITH order_mv1;
```

例6：マテリアライズドビューのリフレッシュ処理のプロファイルを有効にします。この機能はデフォルトで有効になっています。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_profile" = "true");
```

例7：マテリアライズドビューのリフレッシュ処理で中間結果のディスクへのスピルを有効にし、スピルのモードを `force` に設定します。中間結果のディスクへのスピルは、v3.1以降、デフォルトで有効になっています。

```SQL
-- Enable spilling during materialized view refresh.
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_spill" = "true");
-- Set spill_mode to force (the default value is auto).
ALTER MATERIALIZED VIEW mv1 SET ("session.spill_mode" = "force");
```

例8：マテリアライズドビューのクエリステートメントに外部テーブルまたは複数のジョインが含まれている場合、オプティマイザのタイムアウト時間を30秒（v3.3以降のデフォルト）に変更します。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.new_planner_optimize_timeout" = "30000");
```

例9: マテリアライズドビューのクエリの書き換えの有効期限を600秒に変更します。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```

例10: マテリアライズドビューのブルームフィルターインデックスを変更します。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("bloom_filter_columns" = "col1, col2");
```