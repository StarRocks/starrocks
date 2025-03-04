---
displayed_sidebar: docs
---

# SQL Digest

このトピックでは、StarRocks の SQL Digest 機能について紹介します。この機能は v3.3.6 以降でサポートされています。

## 概要

SQL Digest は、パラメータを削除した履歴 SQL ステートメントから生成されるフィンガープリントです。同じ構造で異なるパラメータを持つ SQL ステートメントをクラスタリングするのに役立ちます。

SQL Digest の一般的な使用例には以下があります:

- クエリ履歴で同じ構造だが異なるパラメータを持つ他の SQL ステートメントを見つける
- 同じ構造の SQL の実行頻度、累積時間、その他の統計を追跡する
- システム内で最も時間のかかる SQL パターンを特定する

StarRocks では、SQL Digest は主に監査ログ **fe.audit.log** を通じて記録されます。例えば、次の 2 つの SQL ステートメントを実行します:

```SQL
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920101';
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920202';
```

2 つの同じ Digest が **fe.audit.log** に生成されます:

```SQL
Digest=f58bb71850d112014f773717830e7f77
Digest=f58bb71850d112014f773717830e7f77
```

## 使用方法

### 前提条件

この機能を有効にするには、FE の設定項目 `enable_sql_digest` を `true` に設定する必要があります。

次のステートメントを実行して動的に有効にします:

```SQL
ADMIN SET FRONTEND CONFIG ('enable_sql_digest'='true');
```

永続的に有効にするには、FE 設定ファイル `fe.conf` に `enable_sql_digest = true` を追加し、FE を再起動する必要があります。

この機能を有効にした後、[AuditLoader](./management/audit_loader.md) プラグインをインストールして、SQL ステートメントの統計分析を行うことができます。

### 類似 SQL を見つける

```SQL
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
LIMIT 1;
```

### 類似 SQL の日次実行回数と時間を追跡する

```SQL
SELECT 
    date_trunc('day', `timestamp`) query_date, 
    count(*), 
    sum(queryTime), 
    sum(scanRows), 
    sum(cpuCostNs), 
    sum(memCostBytes)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
GROUP BY query_date
ORDER BY query_date 
DESC LIMIT 30;
```

### 類似 SQL の平均実行時間を計算する

```SQL
SELECT avg(queryTime), min(queryTime), max(queryTime), stddev(queryTime)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>';
```

### 類似 SQL を集約して最も時間のかかるパターンを分析する

```SQL
WITH top_sql AS (
    SELECT digest, sum(queryTime)
    FROM starrocks_audit_db__.starrocks_audit_tbl__ 
    GROUP BY digest
    ORDER BY sum(queryTime) 
    DESC LIMIT 10 
)
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest IN (SELECT digest FROM top_sql);
```

## パラメータ正規化ルール

- SQL 内の定数値は正規化されます。例えば、`WHERE a = 1` と `WHERE a = 2` を持つ類似の SQL ステートメントは同じ Digest を持ちます。
- IN 述語は正規化されます。例えば、`IN (1,2,3)` と `IN (1,2)` を持つ類似の SQL ステートメントは同じ Digest を持ちます。
- `LIMIT N` 句は正規化されます。例えば、`LIMIT 10` と `LIMIT 30` を持つ類似の SQL ステートメントは同じ Digest を持ちます。