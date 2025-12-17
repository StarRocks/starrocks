---
sidebar_position: 120
---

# 監査ログベースのリソースグループ設定

StarRocks では、**Resource Groups** がユーザーの識別やクエリタイプなどのクラシファイア（分類器）に基づいて CPU、メモリ、同時実行制限を割り当てることで、リソースの分離を効果的に行います。この機能は、マルチテナント環境での効率的なリソース利用を実現するために不可欠です。

従来のリソースグループ設定は経験的な判断に依存することが多いです。しかし、監査ログテーブル `starrocks_audit_db__.starrocks_audit_tbl__` からの過去のクエリデータを分析することで、管理者はリソースグループの調整に**データ駆動型アプローチ**を採用できます。CPU時間、メモリ消費、クエリの同時実行数といった主要な指標は、実際のワークロード特性に関する客観的な洞察を提供します。

このアプローチは以下を助けます：

- リソース競合によるクエリ遅延の防止
- クラスターのリソース枯渇からの保護
- 全体的な安定性と予測可能性の向上

このトピックでは、監査ログから観察されたワークロードパターンに基づいて適切なリソースグループパラメータを導き出すためのステップバイステップのチュートリアルを提供します。

:::note
このチュートリアルは、AuditLoader プラグインを使用した分析に基づいており、クラスター内で直接 SQL ステートメントを使用して監査ログをクエリできます。プラグインのインストールに関する詳細な手順については、[AuditLoader](../administration/management/audit_loader.md) を参照してください。
:::

## CPU リソースの割り当て

### 目的

ユーザーごとの CPU 消費を決定し、`cpu_weight` または `exclusive_cpu_cores` を使用して CPU リソースを比例配分します。

### 分析

以下の SQL は、過去 30 日間のユーザーごとの合計 CPU 時間 (`cpuCostNs`) を集計し、秒に変換して、総 CPU 使用率の割合を計算します。

```SQL
SELECT 
    user,
    SUM(cpuCostNs) / 1e9 AS total_cpu_seconds,                  -- クエリの合計 CPU 時間を取得。
    (
        SUM(cpuCostNs) /
        (
            SELECT SUM(cpuCostNs)
            FROM starrocks_audit_db__.starrocks_audit_tbl__
            WHERE state IN ('EOF','OK')
              AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        )
    ) * 100 AS cpu_usage_percentage                             -- ユーザーごとの総 CPU 使用率の割合を計算。
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                     -- 完了したクエリのみを含む。
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)    -- 過去 30 日間のデータをクエリ。
GROUP BY user
ORDER BY total_cpu_seconds DESC
LIMIT 20;                                                       -- CPU リソース消費が最も多い上位 20 ユーザーをリスト。
```

### ベストプラクティス

BE ごとに固定の CPU コア数（例：64 コア）を仮定します。あるユーザーが総 CPU 時間の 16% (`cpu_usage_percentage`) を占める場合、約 `64 × 16% ≈ 11 コア` を割り当てるのが合理的です。

リソースグループの CPU 制限を次のように設定できます：

- `exclusive_cpu_cores`:

  - その値は単一の BE 上の総コア数を超えてはなりません。
  - すべてのリソースグループの `exclusive_cpu_cores` の合計は、単一の BE 上の総コア数を超えてはなりません。

- `cpu_weight`:

  - **ソフトアイソレーション**リソースグループにのみ適用されます。
  - 残りのコアで競合するクエリ間の相対的な CPU シェアを決定します。
  - 固定の CPU コア数に直接マッピングされるわけではありません。

## メモリ管理

### 目的

メモリ集約型のユーザーを特定し、適切なメモリ制限とサーキットブレーカーを定義します。

### 分析

以下の SQL は、過去 30 日間のユーザーごとの単一クエリの最大メモリ使用量 (`memCostBytes`) を計算します。

```SQL
SELECT 
    user,
    MAX(memCostBytes) / (1024 * 1024) AS max_mem_mb            -- クエリごとの最大メモリ使用量（MB 単位）。
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                    -- 完了したクエリのみを含む。
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)   -- 過去 30 日間のデータをクエリ。
GROUP BY user
ORDER BY max_mem_mb DESC
LIMIT 20;                                                      -- メモリリソース消費が最も多い上位 20 ユーザーをリスト。
```

### ベストプラクティス

`max_mem_mb` は**すべての BEs にわたる総メモリ使用量**を表します。おおよその BE ごとのメモリ使用量を次のように計算できます：`max_mem_mb / number_of_BEs`。

リソースグループのメモリ制限を次のように設定できます：

- `big_query_mem_limit`:

  - 異常に大きなクエリからクラスターを保護します。
  - 偽陽性のクエリ終了を避けるために、比較的高いしきい値に設定できます。

- `mem_limit`:

  - ほとんどの場合、高い値（例：`0.9`）に設定します。

## 同時実行制御

### 目的

ユーザーごとのピーククエリ同時実行数を特定し、適切な `concurrency_limit` 値を定義します。

### 分析

以下の SQL は、過去 30 日間の 1 分ごとのクエリ同時実行数を分析し、ユーザーごとに観察された最大の同時実行数を抽出します。

```SQL
WITH UserConcurrency AS (
    SELECT 
        user,
        DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') AS minute_bucket,
        COUNT(*) AS query_concurrency
    FROM starrocks_audit_db__.starrocks_audit_tbl__
    WHERE state IN ('EOF', 'OK')                              -- 完了したクエリのみを含む。
      AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)       -- 過去 30 日間のデータをクエリ。
      AND LOWER(stmt) LIKE '%select%'                         -- SELECT ステートメントのみを含む。
    GROUP BY user, minute_bucket
    HAVING query_concurrency > 1                              -- 1 分あたりのクエリが 1 未満のシナリオを除外。
)
SELECT 
    user,
    minute_bucket,
    query_concurrency / 60.0 AS query_concurrency_per_second  -- 1 秒あたりの同時実行数をクエリ。
FROM (
    SELECT 
        user,
        minute_bucket,
        query_concurrency,
        ROW_NUMBER() OVER (
            PARTITION BY user
            ORDER BY query_concurrency DESC
        ) AS rn
    FROM UserConcurrency
) ranked
WHERE rn = 1                                                  -- 各ユーザーの最高記録を保持。
ORDER BY query_concurrency_per_second DESC
LIMIT 50;                                                     -- 同時実行数が最も高い上位 50 ユーザーをリスト。
```

### ベストプラクティス

上記の分析は**分単位の粒度**で行われます。実際の 1 秒あたりの同時実行数はより高い可能性があります。

リソースグループの同時実行制限を次のように設定できます：

- `concurrency_limit`

  - 観察されたピークの約**1.5 倍**に設定して余裕を持たせます。
  - 極端な同時実行スパイクを持つユーザーには、さらに**Query Queues** を有効にしてピークロードを平滑化し、クラスターの安定性を保護できます。

## 非同期マテリアライズドビューのためのリソース分離

### 目的

非同期マテリアライズドビューのリフレッシュ操作がインタラクティブクエリに影響を与えないようにします。

### 分析

以下の SQL は、通常 `INSERT OVERWRITE` ステートメントによって特徴付けられるメモリ集約型のマテリアライズドビューリフレッシュ操作を特定します。

```SQL
SELECT 
    user,
    MAX(memCostBytes) / (1024 * 1024) AS max_mem_mb             -- クエリごとの最大メモリ使用量（MB 単位）。
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                     -- 完了したクエリのみを含む。
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)    -- 過去 30 日間のデータをクエリ。
  AND LOWER(stmt) LIKE '%insert overwrite%'                     -- マテリアライズドビューリフレッシュ操作のみを含む。
GROUP BY user
ORDER BY max_mem_mb DESC
LIMIT 20;                                                       -- メモリリソース消費が最も多い上位 20 ユーザーをリスト。
```

### ベストプラクティス

StarRocks はデフォルトでマテリアライズドビューリフレッシュタスク用のシステム定義リソースグループ (`default_mv_wg`) を提供します。しかし、マテリアライズドビューリフレッシュタスク専用のリソースグループをカスタマイズして、厳密な分離を強制し、マテリアライズドビューリフレッシュ操作がフォアグラウンドのクエリパフォーマンスを低下させないようにすることを強く推奨します。

リソースグループ制限の設定に関する手順については、[CPU リソース割り当てのベストプラクティス](#ベストプラクティス) および [メモリ管理のベストプラクティス](#ベストプラクティス-1) を参照してください。

以下の例は、マテリアライズドビューリフレッシュタスクに専用のリソースグループを作成し、割り当てるためのガイダンスのみを提供します。

1. マテリアライズドビューリフレッシュ用の専用リソースグループを作成します：

    ```SQL
    CREATE RESOURCE GROUP rg_mv
    TO (
        user = 'mv_user',
        query_type IN ('insert', 'select')
    )
    WITH (
        'cpu_weight' = '32',
        'mem_limit' = '0.9',
        'concurrency_limit' = '10',
        'spill_mem_limit_threshold' = '0.5'
    );
    ```

2. リソースグループをマテリアライズドビューに割り当てます。

    - マテリアライズドビューを作成する際に割り当て：

    ```SQL
    CREATE MATERIALIZED VIEW mv_example
    REFRESH ASYNC
    PROPERTIES (
        'resource_group' = 'rg_mv'
    )
    AS
    SELECT * FROM example_table;
    ```

    - 既存のマテリアライズドビューに割り当て：

    ```SQL
    ALTER MATERIALIZED VIEW mv_example SET ("resource_group" = "rg_mv");
    ```

## 参照

- [AuditLoader](../administration/management/audit_loader.md)
- [Resource Group](../administration/management/resource_management/resource_group.md)
- [Query Queues](../administration/management/resource_management/query_queues.md)