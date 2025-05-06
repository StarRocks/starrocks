---
displayed_sidebar: docs
---

# REFRESH MATERIALIZED VIEW

## 説明

特定の非同期マテリアライズドビューまたはそのパーティションを手動でリフレッシュします。

> **注意**
>
> 手動でリフレッシュできるのは、ASYNC または MANUAL リフレッシュ戦略を採用しているマテリアライズドビューのみです。非同期マテリアライズドビューのリフレッシュ戦略は [SHOW MATERIALIZED VIEWS](SHOW_MATERIALIZED_VIEW.md) を使用して確認できます。この操作には、対象のマテリアライズドビューに対する REFRESH 権限が必要です。

## 構文

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
[PARTITION START ("<partition_start_date>") END ("<partition_end_date>")]
[FORCE]
[WITH { SYNC | ASYNC } MODE]
```

## パラメータ

| **パラメータ**            | **必須**    | **説明**                                                  |
| ------------------------- | ----------- | --------------------------------------------------------- |
| mv_name                   | はい        | 手動でリフレッシュするマテリアライズドビューの名前。       |
| PARTITION START () END () | いいえ      | 特定の時間間隔内のパーティションを手動でリフレッシュします。|
| partition_start_date      | いいえ      | 手動でリフレッシュするパーティションの開始日。             |
| partition_end_date        | いいえ      | 手動でリフレッシュするパーティションの終了日。             |
| FORCE                     | いいえ      | このパラメータを指定すると、StarRocks は対応するマテリアライズドビューまたはパーティションを強制的にリフレッシュします。このパラメータを指定しない場合、StarRocks はデータが更新されているかどうかを自動的に判断し、必要な場合にのみパーティションをリフレッシュします。 |
| WITH ... MODE             | いいえ      | リフレッシュタスクの同期または非同期呼び出しを行います。`SYNC` はリフレッシュタスクの同期呼び出しを示し、タスクが成功または失敗したときにのみ StarRocks がタスク結果を返します。`ASYNC` はリフレッシュタスクの非同期呼び出しを示し、タスクが送信された直後に成功を返し、バックグラウンドで非同期に実行されます。非同期マテリアライズドビューのリフレッシュタスクのステータスは、StarRocks の Information Schema 内の `tasks` および `task_runs` メタデータビューをクエリすることで確認できます。詳細は [Check the execution status of asynchronous materialized view](../../../using_starrocks/async_mv/Materialized_view.md#check-the-execution-status-of-asynchronous-materialized-view) を参照してください。デフォルト: `ASYNC`。v2.5.8 および v3.1.0 以降でサポートされています。 |

> **注意**
>
> external catalog に基づいて作成されたマテリアライズドビューをリフレッシュする場合、StarRocks はマテリアライズドビュー内のすべてのパーティションをリフレッシュします。

## 例

例 1: 非同期呼び出しを介して特定のマテリアライズドビューを手動でリフレッシュします。

```Plain
REFRESH MATERIALIZED VIEW lo_mv1;

REFRESH MATERIALIZED VIEW lo_mv1 WITH ASYNC MODE;
```

例 2: 特定のマテリアライズドビューの特定のパーティションを手動でリフレッシュします。

```Plain
REFRESH MATERIALIZED VIEW lo_mv1 
PARTITION START ("2020-02-01") END ("2020-03-01");
```

例 3: 特定のマテリアライズドビューの特定のパーティションを強制的にリフレッシュします。

```Plain
REFRESH MATERIALIZED VIEW lo_mv1
PARTITION START ("2020-02-01") END ("2020-03-01") FORCE;
```

例 4: 同期呼び出しを介してマテリアライズドビューを手動でリフレッシュします。

```Plain
REFRESH MATERIALIZED VIEW lo_mv1 WITH SYNC MODE;
```