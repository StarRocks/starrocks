---
displayed_sidebar: docs
---

# 仮想列

StarRocks は仮想列（Virtual Column）をサポートしています。これは、クエリで自動的に使用可能ですが、テーブルスキーマに保存されない読み取り専用のメタデータ列です。仮想列は、データストレージ構造に関するメタデータ情報を提供し、データ分布分析やトラブルシューティングに使用できます。

現在、StarRocks は以下の仮想列をサポートしています：

- `_tablet_id_`：各行が属する tablet ID（BIGINT 型）を返します。これは、その行がどの tablet に属するかを識別するために使用されます。

## 機能

- **デフォルトで非表示**：仮想列は `SELECT *`、`DESCRIBE`、または `SHOW CREATE TABLE` ステートメントに表示されません。
- **読み取り専用**：仮想列は自動的に計算され、変更または挿入することはできません。
- **クエリサポート**：仮想列は SELECT、WHERE、GROUP BY、ORDER BY、JOIN、および集約関数で使用できます。

## 構文

クエリで仮想列を使用するには、SELECT 句で明示的に指定する必要があります：

```SQL
SELECT _tablet_id_, <他の列> FROM <テーブル名>;
```

## 一般的な使用例

### 1. データ分布分析

tablet 間のデータ分布を分析して、データスキューを識別します：

```SQL
SELECT _tablet_id_,
       COUNT(*) AS row_count,
       SUM(k2) AS total_value
FROM test_table
GROUP BY _tablet_id_
ORDER BY row_count DESC;
```

### 2. tablet レベルの統計

各 tablet の統計情報を取得して、データ分布を理解します：

```SQL
SELECT _tablet_id_,
       COUNT(*) AS rows,
       MIN(k1) AS min_key,
       MAX(k1) AS max_key,
       AVG(k2) AS avg_value
FROM test_table
GROUP BY _tablet_id_;
```

### 3. トラブルシューティング

特定のデータを含む tablet を識別して、トラブルシューティングに使用します：

```SQL
-- 特定の値を含む tablet を検索
SELECT _tablet_id_, k1, k2
FROM test_table
WHERE k1 = 100;
```

## 制限事項

- 仮想列は `SELECT *` ステートメントに**含まれません**。SELECT 句で明示的に指定する必要があります。
- 仮想列は `DESCRIBE` または `SHOW CREATE TABLE` ステートメントに**表示されません**。
- 仮想列は**変更または直接挿入することはできません**。
- 仮想列は**読み取り専用**で、クエリ実行時に計算されます。

## 注意事項

- `_tablet_id_` の値は、その行を含む tablet を一意に識別する BIGINT 型です。
- 仮想列はクエリ実行時に計算され、ストレージスペースを消費しません。
- 仮想列は、WHERE、GROUP BY、ORDER BY、JOIN、および集約関数を含むすべての標準 SQL 操作で使用できます。
