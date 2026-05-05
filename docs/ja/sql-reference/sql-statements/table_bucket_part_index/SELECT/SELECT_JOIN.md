---
displayed_sidebar: docs
sidebar_label: "JOIN"
---

# Join

Join （ジョイン）オペレーションは、2つ以上のテーブルからデータを結合し、それらのテーブルからいくつかのカラムの結果セットを返します。

StarRocks は、以下のジョインをサポートしています。
- [Self Join](#self-join)
- [Cross Join](#cross-join)
- [Inner Join](#inner-join)
- [Outer Join](#outer-join) （Left Join、Right Join、Full Join を含む）
- [Semi Join](#semi-join)
- [Anti Join](#anti-join)
- [Equi-join and Non-equi-join](#equi-join-と-non-equi-join)
- [USING 句を使用した Join](#using-句を使用した-Join)
- [ASOF Join](#asof-join)

## 構文

```sql
SELECT select_list FROM
table_or_subquery1 [INNER] JOIN table_or_subquery2 |
table_or_subquery1 {LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]} JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} SEMI JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} ANTI JOIN table_or_subquery2 |
[ ON col1 = col2 [AND col3 = col4 ...] |
USING (col1 [, col2 ...]) ]
[other_join_clause ...]
[ WHERE where_clauses ]
```

```sql
SELECT select_list FROM
table_or_subquery1, table_or_subquery2 [, table_or_subquery3 ...]
[other_join_clause ...]
WHERE
col1 = col2 [AND col3 = col4 ...]
```

```sql
SELECT select_list FROM
table_or_subquery1 CROSS JOIN table_or_subquery2
[other_join_clause ...]
[ WHERE where_clauses ]
```

## Self Join

StarRocks は、Self Join をサポートしています。たとえば、同じテーブルの異なるカラムが Join されます。

実際には、Self Join を識別するための特別な構文はありません。Self Join における Join の両側の条件は、同じテーブルから来ています。

異なるエイリアスを割り当てる必要があります。

例：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

## Cross Join

Cross Join は大量の結果を生成する可能性があるため、使用には注意が必要です。

どうしても Cross Join を使用する必要がある場合でも、フィルタ条件を使用し、返される結果が少なくなるようにする必要があります。例：

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

## Inner Join

Inner Join は、最もよく知られ、一般的に使用される Join です。2 つの類似したテーブルから要求された列の結果を返し、両方のテーブルの列に同じ値が含まれている場合に Join されます。

両方のテーブルの列名が同じ場合は、完全名 (table_name.column_name の形式) を使用するか、列名にエイリアスを付ける必要があります。

例：

次の 3 つのクエリは同等です。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

## Outer Join

Outer Join は、左または右のテーブル、または両方のテーブルのすべての行を返します。別のテーブルに一致するデータがない場合は、NULL に設定します。例：

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

## 等価 Join と不等 Join

通常、等価 Join が最も一般的に使用される結合です。結合条件の演算子として等号が必要です。

不等 Join は結合条件として `!=` を使用します。不等 Join は大量の結果を生成し、計算中にメモリ制限を超える可能性があります。

使用には注意が必要です。不等 Join は Inner Join のみをサポートします。例：

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

## Semi Join

Left Semi Join は、右テーブルのデータと一致する左テーブルの行のみを返します。右テーブルの行がいくつ一致するかは関係ありません。

左テーブルのこの行は、最大で1回返されます。Right Semi Join も同様に機能しますが、返されるデータは右テーブルになります。

例：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

## Anti Join

Left Anti Join は、右側のテーブルに一致しない左側のテーブルの行のみを返します。

Right Anti Join はこの比較を逆にして、左側のテーブルに一致しない右側のテーブルの行のみを返します。例：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

## Equi-join と Non-equi-join

StarRocks がサポートするさまざまなジョインは、ジョインで指定されたジョイン条件に応じて、Equi-join と Non-equi-join に分類できます。

| **Join タイプ** | **バリアント**                                                      |
| -------------- | ----------------------------------------------------------------- |
| Equi-join      | Self join、cross join、inner join、outer join、semi join、anti join |
| Non-equi-join  | cross join、inner join、left semi join、left anti join、outer join  |

- Equi-join
  
  Equi-join は、2 つのジョインアイテムが `=` 演算子で結合されるジョイン条件を使用します。例：`a JOIN b ON a.id = b.id`。

- Non-equi-join
  
  Non-equi-join は、2 つのジョインアイテムが `<`、`<=`、`>`、`>=`、`<>` などの比較演算子で結合されるジョイン条件を使用します。例：`a JOIN b ON a.id < b.id`。Non-equi-join は Equi-join よりも実行速度が遅くなります。Non-equi-join を使用する場合は注意することをお勧めします。

  次の 2 つの例は、Non-equi-join を実行する方法を示しています。

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

## USING 句を使用した Join

v4.0.2以降、StarRocks は `ON` に加えて、`USING` 句によるジョイン条件の指定をサポートしています。これにより、同じ名前の列を持つ等価ジョインを簡素化できます。例：`SELECT * FROM t1 JOIN t2 USING (id)`。

**バージョン間の違い：**

- **v4.0.2より前のバージョン**
  
  `USING` は構文糖として扱われ、内部的に `ON` 条件に変換されます。結果には、左と右のテーブル両方からの USING 列が個別の列として含まれ、USING 列を参照する際にはテーブルエイリアス修飾子（例：`t1.id`）が許可されていました。

  例：

  ```SQL
  SELECT t1.id, t2.id FROM t1 JOIN t2 USING (id);  -- Returns two separate id columns
  ```

- **v4.0.2 以降**
  
  StarRocks は、SQL 標準の `USING` セマンティクスを実装しています。主な機能は次のとおりです。
  
  - `FULL OUTER JOIN` を含む、すべてのジョインタイプがサポートされています。
  - USING カラムは、結果に単一の結合されたカラムとして表示されます。FULL OUTER JOIN の場合、`COALESCE(left.col, right.col)` セマンティクスが使用されます。
  - テーブルエイリアス修飾子 (例: `t1.id`) は、USING カラムではサポートされなくなりました。非修飾カラム名 (例: `id`) を使用する必要があります。
  - `SELECT *` の結果の場合、カラムの順序は `[USING カラム, 左側の非 USING カラム, 右側の非 USING カラム]` となります。

  例：

  ```SQL
  SELECT t1.id FROM t1 JOIN t2 USING (id);        -- ❌ Error: Column 'id' is ambiguous
  SELECT id FROM t1 JOIN t2 USING (id);           -- ✅ Correct: Returns a single coalesced 'id' column
  SELECT * FROM t1 FULL OUTER JOIN t2 USING (id); -- ✅ FULL OUTER JOIN is supported
  ```

これらの変更は、StarRocks の動作を SQL 標準に準拠したデータベースに合わせるものです。

## ASOF Join

ASOF Join は、時系列分析で一般的に使用される時間ベースまたは範囲ベースのジョインの一種です。特定のキーが等しいことと、時間フィールドまたはシーケンスフィールドに関する不等式条件（例：`t1.time >= t2.time`）に基づいて、2 つのテーブルをジョインできます。ASOF Join は、左側のテーブルの各行に対して、右側のテーブルから最も新しい一致する行を選択します。v4.0 以降でサポートされています。

実際のシナリオでは、時系列データを含む分析は、多くの場合、次の課題に直面します。
- データ収集タイミングのずれ（異なるセンサーのサンプリング時間など）
- イベントの発生時刻と記録時刻のわずかなずれ
- 特定のタイムスタンプに最も近い過去のレコードを見つける必要性

従来の等価ジョイン（INNER Join）では、このようなデータを処理する際にデータが大幅に失われることがよくあります。一方、不等式ジョインではパフォーマンスの問題が発生する可能性があります。ASOF Join は、これらの特定の課題に対処するために設計されました。

ASOF Join は、一般的に次のような場合に使用されます。

- **金融市場分析**
  - 株価と取引量のマッチング
  - 異なる市場からのデータの調整
  - デリバティブ価格の参照データマッチング
- **IoT データ処理**
  - 複数のセンサーデータストリームの調整
  - デバイスの状態変化の相関付け
  - 時系列データの補間
- **ログ分析**
  - システムイベントとユーザーアクションの相関付け
  - 異なるサービスからのログのマッチング
  - 障害分析と問題追跡

構文：

```SQL
SELECT [select_list]
FROM left_table [AS left_alias]
ASOF LEFT JOIN right_table [AS right_alias]
    ON equality_condition
    AND asof_condition
[WHERE ...]
[ORDER BY ...]
```

- `ASOF LEFT JOIN`: 時間またはシーケンスで最も近い一致に基づいて、非等価ジョインを実行します。 ASOF LEFT JOIN は、左側のテーブルからすべての行を返し、一致しない右側の行を NULL で埋めます。
- `equality_condition`: 標準的な等価制約（たとえば、ティッカーシンボルまたは ID の一致）。
- `asof_condition`: 通常 `left.time >= right.time` として記述される範囲条件で、`left.time` を超えない最新の `right.time` レコードを検索することを示します。

:::note
`asof_condition` では DATE 型と DATETIME 型のみがサポートされています。また、サポートされる `asof_condition` は 1 つのみです。
:::

例：

```SQL
SELECT *
FROM holdings h ASOF LEFT JOIN prices p             
ON h.ticker = p.ticker            
AND h.when >= p.when
ORDER BY ALL;
```

制限事項：

- 現在、Inner Join (デフォルト) と Left Outer Join のみがサポートされています。
- `asof_condition` では、DATE 型と DATETIME 型のみがサポートされています。
- `asof_condition` は 1 つのみサポートされています。
