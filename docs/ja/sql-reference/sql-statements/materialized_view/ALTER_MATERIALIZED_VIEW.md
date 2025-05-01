---
displayed_sidebar: docs
---

# ALTER MATERIALIZED VIEW

## 説明

このSQL文は以下の操作が可能です:

- 非同期マテリアライズドビューの名前を変更します。
- 非同期マテリアライズドビューのリフレッシュ戦略を変更します。
- 非同期マテリアライズドビューのステータスをアクティブまたは非アクティブに変更します。
- 2つの非同期マテリアライズドビュー間でのアトミックスワップを実行します。
- 非同期マテリアライズドビューのプロパティを変更します。

  このSQL文を使用して以下のプロパティを変更できます:

  - `partition_ttl_number`
  - `partition_refresh_number`
  - `resource_group`
  - `auto_refresh_partitions_limit`
  - `excluded_trigger_tables`
  - `mv_rewrite_staleness_second`
  - `unique_constraints`
  - `foreign_key_constraints`
  - `colocate_with`
  - すべてのセッション変数関連のプロパティ。セッション変数についての詳細は [System variables](../../System_variable.md) を参照してください。

:::tip

- この操作には、対象のマテリアライズドビューに対するALTER権限が必要です。[GRANT](../account-management/GRANT.md) の指示に従ってこの権限を付与できます。
- ALTER MATERIALIZED VIEWは、マテリアライズドビューを構築するために使用されるクエリ文を直接変更することをサポートしていません。新しいマテリアライズドビューを構築し、ALTER MATERIALIZED VIEW SWAP WITHを使用して元のビューと交換することができます。

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

角括弧 [] 内のパラメータはオプションです。

## パラメータ

| **パラメータ**          | **必須**    | **説明**                                                      |
| ----------------------- | ------------ | ------------------------------------------------------------ |
| mv_name                 | はい         | 変更するマテリアライズドビューの名前。                        |
| new_refresh_scheme_desc | いいえ       | 新しいリフレッシュ戦略、詳細は [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](CREATE_MATERIALIZED_VIEW.md#parameters) を参照してください。 |
| new_mv_name             | いいえ       | マテリアライズドビューの新しい名前。                          |
| ACTIVE                  | いいえ       | マテリアライズドビューのステータスをアクティブに設定します。StarRocksは、ベーステーブルが変更された場合（例: 削除され再作成された場合）、元のメタデータが変更されたベーステーブルと一致しない状況を防ぐために、マテリアライズドビューを自動的に非アクティブに設定します。非アクティブなマテリアライズドビューはクエリアクセラレーションやクエリの書き換えには使用できません。ベーステーブルを変更した後にこのSQLを使用してマテリアライズドビューをアクティブ化できます。 |
| INACTIVE                | いいえ       | マテリアライズドビューのステータスを非アクティブに設定します。非アクティブな非同期マテリアライズドビューはリフレッシュできませんが、テーブルとしてクエリすることはできます。 |
| SWAP WITH               | いいえ       | 必要な整合性チェックの後、他の非同期マテリアライズドビューとアトミックな交換を実行します。 |
| key                     | いいえ       | 変更するプロパティの名前、詳細は [SQL Reference - CREATE MATERIALIZED VIEW - Parameters](CREATE_MATERIALIZED_VIEW.md#parameters) を参照してください。<br />**注意**<br />マテリアライズドビューのセッション変数関連のプロパティを変更する場合、プロパティに `session.` プレフィックスを追加する必要があります。例: `session.query_timeout`。非セッションプロパティにはプレフィックスを指定する必要はありません。例: `mv_rewrite_staleness_second`。 |
| value                   | いいえ       | 変更するプロパティの値。                                     |

## 例

例1: マテリアライズドビューの名前を変更します。

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

例2: マテリアライズドビューのリフレッシュ間隔を変更します。

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```

例3: マテリアライズドビューのプロパティを変更します。

```SQL
-- mv1のquery_timeoutを40000秒に変更します。
ALTER MATERIALIZED VIEW mv1 SET ("session.query_timeout" = "40000");
-- mv1のmv_rewrite_staleness_secondを600秒に変更します。
ALTER MATERIALIZED VIEW mv1 SET ("mv_rewrite_staleness_second" = "600");
```

例4: マテリアライズドビューのステータスをアクティブに変更します。

```SQL
ALTER MATERIALIZED VIEW order_mv ACTIVE;
```

例5: マテリアライズドビュー `order_mv` と `order_mv1` の間でアトミックな交換を実行します。

```SQL
ALTER MATERIALIZED VIEW order_mv SWAP WITH order_mv1;
```