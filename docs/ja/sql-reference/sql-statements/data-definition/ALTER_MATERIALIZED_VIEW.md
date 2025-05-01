---
displayed_sidebar: docs
---

# ALTER MATERIALIZED VIEW

## 説明

このSQLステートメントでは以下を行うことができます：

- 非同期マテリアライズドビューの名前を変更する。
- 非同期マテリアライズドビューのリフレッシュ戦略を変更する。
- 非同期マテリアライズドビューのステータスをアクティブまたは非アクティブに変更する。
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
  - すべてのセッション変数関連のプロパティ。セッション変数については、[System variables](../../../reference/System_variable.md)を参照してください。

## 構文

```SQL
ALTER MATERIALIZED VIEW [db_name.]<mv_name> 
    { RENAME [db_name.]<new_mv_name> 
    | REFRESH <new_refresh_scheme_desc> 
    | ACTIVE | INACTIVE 
    | SET ( "<key>" = "<value>"[,...]) }
```

角括弧 [] 内のパラメータはオプションです。

## パラメータ

| **パラメータ**          | **必須**     | **説明**                                                    |
| ----------------------- | ------------ | ----------------------------------------------------------- |
| mv_name                 | yes          | 変更するマテリアライズドビューの名前。                      |
| new_refresh_scheme_desc | no           | 新しい非同期リフレッシュ戦略。詳細は [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](../data-definition/CREATE_MATERIALIZED_VIEW.md#parameters) を参照してください。 |
| new_mv_name             | no           | マテリアライズドビューの新しい名前。                        |
| ACTIVE                  | no           | マテリアライズドビューのステータスをアクティブに設定します。StarRocksは、ベーステーブルが変更された場合（例：削除され再作成された場合）、元のメタデータが変更されたベーステーブルと一致しない状況を防ぐために、マテリアライズドビューを自動的に非アクティブに設定します。非アクティブなマテリアライズドビューはクエリアクセラレーションやクエリの書き換えに使用できません。ベーステーブルを変更した後にこのSQLを使用してマテリアライズドビューをアクティブ化できます。 |
| INACTIVE                | no           | マテリアライズドビューのステータスを非アクティブに設定します。非アクティブな非同期マテリアライズドビューはリフレッシュできませんが、テーブルとしてクエリすることはできます。 |
| key                     | no           | 変更するプロパティの名前。詳細は [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](../data-definition/CREATE_MATERIALIZED_VIEW.md#parameters) を参照してください。<br />**注意**<br />マテリアライズドビューのセッション変数関連のプロパティを変更する場合は、プロパティに `session.` プレフィックスを追加する必要があります。例えば、`session.query_timeout` のようにします。非セッションプロパティにはプレフィックスを指定する必要はありません。例えば、`mv_rewrite_staleness_second` のようにします。 |
| value                   | no           | 変更するプロパティの値。                                    |

## 例

例1: マテリアライズドビューの名前を変更する

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

例2: マテリアライズドビューのリフレッシュ間隔を変更する

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

例3: マテリアライズドビューのプロパティを変更する。

```SQL
-- mv1のquery_timeoutを40000秒に変更します。
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "40000");
-- mv1のmv_rewrite_staleness_secondを600秒に変更します。
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```

例4: マテリアライズドビューのステータスをアクティブに変更する。

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```