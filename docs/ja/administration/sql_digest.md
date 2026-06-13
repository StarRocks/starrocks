---
displayed_sidebar: docs
description: "v3.3.6以降、SQL Digest はパラメータを削除した SQL フィンガープリントで同一構造クエリをクラスタリングします。"
---

# SQLダイジェスト

このトピックでは、StarRocksのSQLダイジェスト機能について説明します。この機能はv3.3.6以降でサポートされています。

## 概要

SQLダイジェストは、パラメータが削除された過去のSQLステートメントによって生成されるフィンガープリントです。これにより、同じ構造だが異なるパラメータを持つSQLステートメントをクラスタリングできます。

SQLダイジェストの一般的なユースケースは次のとおりです。

- クエリ履歴内で、同じ構造だが異なるパラメータを持つ他のSQLステートメントを見つける
- 同じ構造を持つSQLの実行頻度、累積時間、その他の統計を追跡する
- システム内で最も時間のかかるSQLパターンを特定する

StarRocksでは、SQLダイジェストは主に監査ログを通じて記録されます。**fe.audit.log**。例えば、次の2つのSQLステートメントを実行します。

```SQL
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920101';
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920202';
```

2つの同じダイジェストが生成されます。**fe.audit.log**：

```SQL
Digest=f58bb71850d112014f773717830e7f77
Digest=f58bb71850d112014f773717830e7f77
```

## 使用方法

### 前提条件

この機能を有効にするには、FE構成項目`enable_sql_digest`を`true`に設定する必要があります。

動的に有効にするには、次のステートメントを実行します。

```SQL
ADMIN SET FRONTEND CONFIG ('enable_sql_digest'='true');
```

永続的に有効にするには、FE構成ファイル`fe.conf`に`enable_sql_digest = true`を追加し、FEを再起動する必要があります。

この機能を有効にした後、[AuditLoader](./management/audit_loader.md)プラグインをインストールして、SQLステートメントの統計分析を実行できます。

### 類似のSQLを見つける

```SQL
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
LIMIT 1;
```

### 類似のSQLの1日の実行回数と時間を追跡する

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

### 類似のSQLの平均実行時間を計算する

```SQL
SELECT avg(queryTime), min(queryTime), max(queryTime), stddev(queryTime)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>';
```

### 類似のSQLを集計して、最も時間のかかるパターンを分析する

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

- SQL内の定数値は正規化されます。例えば、`WHERE a = 1`と`WHERE a = 2`を含む類似のSQLステートメントは同じダイジェストを持ちます。
- IN述語は正規化されます。例えば、`IN (1,2,3)`と`IN (1,2)`を含む類似のSQLステートメントは同じダイジェストを持ちます。
- `LIMIT N`句は正規化されます。例えば、`LIMIT 10`と`LIMIT 30`を含む類似のSQLステートメントは同じダイジェストを持ちます。

## クロスデータベースダイジェスト

デフォルトでは、SQLダイジェストは計算にデータベース名を含みます。これは、異なるデータベースで実行された同じSQLパターンが異なるダイジェストを生成することを意味します。例えば：

```SQL
USE db1;
SELECT * FROM t WHERE id = 1;
-- ダイジェスト: aaa...

USE db2;
SELECT * FROM t WHERE id = 1;
-- ダイジェスト: bbb... (上記とは異なる)
```

どのデータベースで実行されても同じSQLパターンが同じダイジェストを生成するようにしたい場合は、セッション変数`sql_digest_exclude_db`を`true`に設定できます。

```SQL
SET sql_digest_exclude_db = true;
```

このオプションを有効にすると、データベース名がダイジェスト計算から除外され、異なるデータベース間でSQLパターンを集計できるようになります。

```SQL
USE db1;
SELECT * FROM t WHERE id = 1;
-- ダイジェスト: ccc...

USE db2;
SELECT * FROM t WHERE id = 1;
-- ダイジェスト: ccc... (上記と同じ)
```

:::note

外部カタログ名は、この設定に関わらず常にダイジェストに含まれます。

:::
