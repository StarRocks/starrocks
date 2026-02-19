---
displayed_sidebar: docs
---

# Skew Join V2

Skew Join V2 は、StarRocks における高度な最適化機能で、JOIN 操作におけるデータスキュー問題をスキュー値をブロードキャストすることで解決します。この機能は、データ分布が大きく偏っている場合にクエリパフォーマンスを大幅に向上させます。

## 概要

データスキューは、ジョイン列の特定の値が他の値よりも頻繁に出現することで発生し、ノード間でのデータ分布が不均一になり、パフォーマンスのボトルネックを引き起こします。Skew Join V2 は以下の方法でこの問題を解決します。

1. **スキュー値の特定**: データスキューを引き起こす値を手動で指定
2. **スキュー値のブロードキャスト**: これらの特定の値をすべてのノードにブロードキャストし、データ分布を均一化
3. **ハイブリッド実行**: 最適なパフォーマンスを実現するために、shuffle と broadcast join を組み合わせて使用

Skew Join V2 は、以下を組み合わせたハイブリッド実行プランを作成します。

1. **Shuffle Join**: 非スキュー データに対して、標準のシャッフルベースのジョインを使用
2. **Broadcast Join**: スキュー値に対して、右テーブルデータをすべてのノードにブロードキャスト

## Skew Join V2 の有効化

デフォルトでは、Skew Join V2 の最適化は無効化されており、その前身である Skew Join V1 が有効化されています。Skew Join V1 のパフォーマンスが満足できない場合、Skew Join V1 を無効化してから Skew Join V2 を有効化する必要があります。

```SQL
-- Skew Join V1 を無効化
SET enable_optimize_skew_join_v1 = false;

-- Skew Join V2 を有効化
SET enable_optimize_skew_join_v2 = true;
```

Skew Join V2 を有効化した後、明示的に指定されたスキュー値を使用して [syntax](#usage) に基づいてクエリを更新できます。

:::note
現在、Skew Join V2 は統計に基づく自動プランの書き換えをサポートしていません。ヒントベースの手動 SQL 書き換えのみがサポートされています。
:::

## 使用法

Skew Join V1 のクエリの書き換え方法と比較して、Skew Join V2 は Broadcast を使用してスキュー値を明示的に指定できる新しい構文を提供します。

構文:

```SQL
SELECT select_list FROM
table1 JOIN [skew|table1.column(skew_value1, skew_value2, ...)] table2
ON join_condition
[WHERE where_clause]
```

パラメータ:

`[skew|table1.column(skew_value1, skew_value2, ...)]`: スキューヒント。以下を含みます:
- `table1.column`: スキュー値を含む左テーブルの列。
- `skew_value1, skew_value2, ...`: データスキューを引き起こす値のカンマ区切りリスト。

:::note
スキューヒントの括弧を省略しないでください。
:::

## 例

### 基本的な使用法

1. テストテーブルを作成します。

    ```SQL
    CREATE TABLE orders (
        order_id INT,
        customer_id INT,
        order_date DATE,
        amount DECIMAL(10,2)
    ) DUPLICATE KEY(order_id)
    DISTRIBUTED BY HASH(order_id) BUCKETS 8;

    CREATE TABLE customers (
        customer_id INT,
        customer_name VARCHAR(100),
        city VARCHAR(50)
    ) DUPLICATE KEY(customer_id)
    DISTRIBUTED BY HASH(customer_id) BUCKETS 8;
    ```

2. スキューを含むサンプルデータを挿入します。

    ```SQL
    INSERT INTO orders VALUES 
    (1, 1001, '2024-01-01', 100.00),
    (2, 1001, '2024-01-02', 200.00),
    (3, 1001, '2024-01-03', 150.00),
    (4, 1002, '2024-01-01', 300.00),
    (5, 1003, '2024-01-01', 250.00);

    INSERT INTO customers VALUES 
    (1001, 'John Doe', 'New York'),
    (1002, 'Jane Smith', 'Los Angeles'),
    (1003, 'Bob Johnson', 'Chicago');
    ```

3. Skew Join V2 を使用してデータをクエリします。

    ```SQL
    SELECT o.order_id, c.customer_name, o.amount
    FROM orders o 
    JOIN [skew|o.customer_id(1001)] c 
    ON o.customer_id = c.customer_id;
    ```

### 複数のスキュー値

Skew Join V2 は複数のスキュー値をサポートします。

```SQL
SELECT o.order_id, c.customer_name, o.amount
FROM orders o 
JOIN [skew|o.customer_id(1001, 1002, 1003)] c 
ON o.customer_id = c.customer_id;
```

### 異なるデータ型

Skew Join V2 は、スキュー値に対してさまざまなデータ型をサポートします。

```SQL
-- 文字列値
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics', 'clothing', 'books')] t2 
ON t1.category = t2.category;

-- 日付値
SELECT t1.id, t2.event_name
FROM events t1 
JOIN [skew|t1.event_date('2024-01-01', '2024-01-02')] t2 
ON t1.event_date = t2.event_date;

-- 数値値
SELECT t1.id, t2.region
FROM sales t1 
JOIN [skew|t1.region_id(1, 2, 3)] t2 
ON t1.region_id = t2.region_id;
```

### 複雑なジョイン条件

Skew Join V2 は複雑なジョイン条件をサポートします。

```SQL
-- 複雑な式を含むジョイン条件
SELECT t1.id, t2.value
FROM table1 t1 
JOIN [skew|t1.key(abs(t1.key))] t2 
ON abs(t1.key) = abs(t2.key);

-- 複数のジョイン条件
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category AND t1.region = t2.region;
```

### 異なるジョインタイプ

Skew Join V2 はさまざまなジョインタイプをサポートします。

```SQL
-- Left Join
SELECT t1.id, t2.name
FROM table1 t1 
LEFT JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;

-- Left Semi Join
SELECT t1.id
FROM table1 t1 
LEFT SEMI JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;

-- Left Anti Join
SELECT t1.id
FROM table1 t1 
LEFT ANTI JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;
```

## ベストプラクティス

### ステップ 1. スキュー値の特定

Skew Join V2 を使用する前に、データスキューを引き起こす値を特定します。

```SQL
-- データ分布を分析
SELECT customer_id, COUNT(*) as cnt
FROM orders 
GROUP BY customer_id 
ORDER BY cnt DESC 
LIMIT 10;
```

### ステップ 2. 適切なスキュー値の選択

適切なスキュー値を選択するためのルールに従います。

- 重大なスキューを引き起こす最も頻繁な値のみを含める。
- 多くの値を含めすぎないようにし、ブロードキャストのオーバーヘッドを増やさない。
- スキューの最適化とブロードキャストコストのトレードオフを考慮する。

## 制限事項

- **サポートされるジョインタイプ**
  現在、INNER JOIN、LEFT JOIN、LEFT SEMI JOIN、および LEFT ANTI JOIN のみがサポートされています。
- **データ型**
  現在、基本的なデータ型 (INT、BIGINT、STRING、DATE、DATETIME、および文字列型) のみがサポートされています。
- **複雑な式**
  ジョイン条件における複雑な式のサポートは限定的です。
- **スキューテーブル**
  Skew Join V2 は、JOIN 操作において大きなテーブルがスキューしているシナリオのみを処理でき、大きなテーブルは左テーブルとして使用されなければなりません。
- **Join の順序変更**
  `skew` ヒントを使用すると、オプティマイザが Join の順序を変更するのを防ぎます。SQL で指定された順序で Join が実行され、オプティマイザはヒントを含む Join ノードの左テーブルと右テーブルを入れ替えたり、Join の順序を変更したりしようとはしません。

## トラブルシューティング

### デバッグ情報

クエリに対して EXPLAIN VERBOSE を実行し、その実行プランを収集します。

```SQL
EXPLAIN VERBOSE 
SELECT ... FROM ... JOIN [skew|...] ...;
```

プランの以下のフィールドを確認します。

- `SplitCastDataSink`: データの分割。
- `BROADCAST` および `SHUFFLE`: 分布タイプ。
- `UNION`: 両方のジョインタイプの結果の結合。

## 関連トピック

- [ Query Planning ](../best_practices/query_tuning/query_planning.md)
- [ Query Profile Tuning ](../best_practices/query_tuning/query_profile_tuning_recipes.md)
- [ System Variables ](../sql-reference/System_variable.md)
- [ JOIN Operations ](../sql-reference/sql-statements/table_bucket_part_index/SELECT.md#join)
