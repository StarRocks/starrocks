---
displayed_sidebar: docs
---

# ALTER MATERIALIZED VIEW

## 説明

このSQLステートメントは以下のことができます：

- 非同期マテリアライズドビューの名前を変更する。
- 非同期マテリアライズドビューのリフレッシュ戦略を変更する。
- 非同期マテリアライズドビューのステータスをアクティブまたは非アクティブに変更する。
- 2つの非同期マテリアライズドビュー間でのアトミックスワップを実行する。
- 非同期マテリアライズドビューのプロパティを変更する。

  このSQLステートメントを使用して、以下のプロパティを変更できます：

  - `partition_ttl_number`
  - `partition_refresh_number`
  - `resource_group`
  - `auto_refresh_partitions_limit`
  - `excluded_trigger_tables`
  - `mv_rewrite_staleness_second`
  - `unique_constraints`
  - `foreign_key_constraints`
  - `colocate_with`
  - `excluded_refresh_tables`
  - すべてのセッション変数関連のプロパティ。セッション変数についての詳細は [System variables](../../System_variable.md) を参照してください。

:::tip

- この操作には、対象のマテリアライズドビューに対するALTER権限が必要です。[GRANT](../account-management/GRANT.md) の指示に従って、この権限を付与することができます。
- ALTER MATERIALIZED VIEW は、マテリアライズドビューを構築するために使用されるクエリステートメントを直接変更することをサポートしていません。新しいマテリアライズドビューを構築し、ALTER MATERIALIZED VIEW SWAP WITH を使用して元のビューと交換することができます。

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

| **パラメータ**          | **必須**    | **説明**                                                      |
| ----------------------- | ------------ | ------------------------------------------------------------ |
| mv_name                 | yes          | 変更するマテリアライズドビューの名前。                        |
| new_refresh_scheme_desc | no           | 新しいリフレッシュ戦略。詳細は [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](CREATE_MATERIALIZED_VIEW.md#parameters) を参照してください。 |
| new_mv_name             | no           | マテリアライズドビューの新しい名前。                          |
| ACTIVE                  | no           | マテリアライズドビューのステータスをアクティブに設定します。StarRocks は、ベーステーブルが変更された場合（例：削除され再作成された場合）、元のメタデータが変更されたベーステーブルと一致しない状況を防ぐために、マテリアライズドビューを自動的に非アクティブに設定します。非アクティブなマテリアライズドビューはクエリアクセラレーションやクエリの書き換えに使用できません。ベーステーブルを変更した後にこのSQLを使用してマテリアライズドビューをアクティブ化できます。 |
| INACTIVE                | no           | マテリアライズドビューのステータスを非アクティブに設定します。非アクティブな非同期マテリアライズドビューはリフレッシュできませんが、テーブルとしてクエリすることはできます。 |
| SWAP WITH               | no           | 必要な整合性チェックの後、他の非同期マテリアライズドビューとアトミック交換を実行します。 |
| key                     | no           | 変更するプロパティの名前。詳細は [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](CREATE_MATERIALIZED_VIEW.md#parameters) を参照してください。<br />**注意**<br />マテリアライズドビューのセッション変数関連のプロパティを変更したい場合は、プロパティに `session.` プレフィックスを追加する必要があります。例えば、`session.query_timeout` のようにします。非セッションプロパティにはプレフィックスを指定する必要はありません。例えば、`mv_rewrite_staleness_second` のようにします。 |
| value                   | no           | 変更するプロパティの値。                                     |

## 例

例1: マテリアライズドビューの名前を変更する。

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

例2: マテリアライズドビューのリフレッシュ間隔を変更する。

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

例3: マテリアライズドビューのリフレッシュタスクのタイムアウト時間を1時間（デフォルト）に変更する。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "3600");
```

例4: マテリアライズドビューのステータスをアクティブに変更する。

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```

例5: マテリアライズドビュー `order_mv` と `order_mv1` の間でアトミック交換を実行する。

```SQL
ALTER MATERIALIZED VIEW order_mv SWAP WITH order_mv1;
```

例6: マテリアライズドビューのリフレッシュプロセスのプロファイルを有効にする。この機能はデフォルトで有効です。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_profile" = "true");
```

例7: マテリアライズドビューのリフレッシュプロセスの中間結果のスピリングを有効にし、スピリングのモードを `force` に設定する。中間結果のスピリングはv3.1以降デフォルトで有効です。

```SQL
-- マテリアライズドビューのリフレッシュ中にスピリングを有効にする。
ALTER MATERIALIZED VIEW mv1 SET ("session.enable_spill" = "true");
-- スピルモードを force に設定する（デフォルト値は auto）。
ALTER MATERIALIZED VIEW mv1 SET ("session.spill_mode" = "force");
```

例8: 外部テーブルや複数のジョインを含むクエリステートメントがある場合、マテリアライズドビューのオプティマイザタイムアウト時間を30秒（v3.3以降のデフォルト）に変更する。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("session.new_planner_optimize_timeout" = "30000");
```

例9: マテリアライズドビューのクエリの書き換えの古さの時間を600秒に変更する。

```SQL
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```