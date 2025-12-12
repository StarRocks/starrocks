# 再帰 CTE（Recursive Common Table Expression）

## 概要

再帰共通表式（Recursive CTE）は、自分自身を参照できる特殊な CTE（Common Table Expression）であり、再帰クエリを実現します。再帰 CTE は、組織構造、ファイルシステム、グラフトラバーサルなどの階層構造データの処理に非常に便利です。

StarRocks はバージョン 4.1 から再帰 CTE 機能をサポートしており、反復実行方式を使用して再帰クエリを実装し、さまざまな階層構造やツリー構造データを効率的に処理できます。

## 基本概念

再帰 CTE は次のコンポーネントで構成されます：

1. **アンカーメンバー（Anchor Member）**：再帰の開始データセットを提供する非再帰の初期クエリ
2. **再帰メンバー（Recursive Member）**：CTE 自身を参照する再帰クエリ
3. **終了条件**：無限再帰を防ぐ条件、通常は WHERE 句で実装

再帰 CTE の実行プロセス：

1. アンカーメンバーを実行し、初期結果セット（レベル 0）を取得
2. 初期結果を入力として、再帰メンバーを実行し、レベル 1 の結果を取得
3. レベル 1 の結果を入力として、再帰メンバーを再度実行し、レベル 2 の結果を取得
4. 再帰メンバーが行を返さなくなるか、最大再帰深度に達するまでこのプロセスを繰り返す
5. UNION ALL（または UNION）を使用してすべてのレベルの結果をマージ

## 構文

```sql
WITH RECURSIVE cte_name [(column_list)] AS (
    -- アンカーメンバー（非再帰部分）
    anchor_query
    UNION [ALL | DISTINCT]
    -- 再帰メンバー（再帰部分）
    recursive_query
)
SELECT ... FROM cte_name ...;
```

**パラメータ説明：**

- `cte_name`：CTE の名前
- `column_list`（オプション）：CTE 結果セットの列名リスト
- `anchor_query`：CTE 自身を参照できない非再帰の初期クエリ
- `UNION [ALL | DISTINCT]`：結合演算子
  - `UNION ALL`：すべての行を保持（重複を含む）、パフォーマンスが良いため推奨
  - `UNION`：重複行を削除
- `recursive_query`：CTE 自身を参照する再帰クエリ

## 制限事項

StarRocks の再帰 CTE には次の制限があります：

1. **機能フラグが必要**：再帰 CTE を使用するには `enable_recursive_cte = true` を設定する必要があります

2. **構造要件**：
   - 再帰 CTE はアンカーメンバーと再帰メンバーを接続するために UNION または UNION ALL を使用する必要があります
   - アンカーメンバーは CTE 自身を参照できません
   - 再帰メンバーが CTE 自身を参照しない場合、通常の CTE として実行されます

3. **再帰深度の制限**：
   - デフォルトの最大再帰深度は 5 レベル
   - 無限再帰を防ぐために `recursive_cte_max_depth` パラメータで調整可能

4. **実行制約**：
   - 現在のバージョンは多階層ネストされた再帰 CTE をサポートしていません
   - 複雑な再帰 CTE はパフォーマンスが低下する可能性があります
   - `anchor_query` の定数は `recursive_query` の出力型と一致する型にする必要があります

## セッション変数

再帰 CTE を使用するには、次のセッション変数を設定する必要があります：

| 変数名 | 型 | デフォルト値 | 説明 |
|--------|-----|--------------|------|
| `enable_recursive_cte` | BOOLEAN | false | 再帰 CTE 機能を有効にするかどうか |
| `recursive_cte_max_depth` | INT | 5 | 無限再帰を防ぐための最大再帰深度 |


## 使用例

### 例 1：組織階層のクエリ
これは再帰 CTE の最も一般的な使用例の 1 つで、従業員の組織階層関係をクエリします。

**データの準備：**

```sql
CREATE TABLE employees (
  employee_id INT,
  name VARCHAR(100),
  manager_id INT,
  title VARCHAR(50)
) DUPLICATE KEY(employee_id)
DISTRIBUTED BY RANDOM;

INSERT INTO employees VALUES
(1, 'Alicia', NULL, 'CEO'),
(2, 'Bob', 1, 'CTO'),
(3, 'Carol', 1, 'CFO'),
(4, 'David', 2, 'VP of Engineering'),
(5, 'Eve', 2, 'VP of Research'),
(6, 'Frank', 3, 'VP of Finance'),
(7, 'Grace', 4, 'Engineering Manager'),
(8, 'Heidi', 4, 'Tech Lead'),
(9, 'Ivan', 5, 'Research Manager'),
(10, 'Judy', 7, 'Senior Engineer');
```

**組織階層のクエリ：**

```sql
WITH RECURSIVE org_hierarchy AS (
    -- アンカーメンバー：CEO から開始（上司のいない従業員）
    SELECT 
        employee_id, 
        name, 
        manager_id, 
        title, 
        CAST(1 AS BIGINT) AS level,
        name AS path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- 再帰メンバー：次のレベルの部下を検索
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        e.title,
        oh.level + 1,
        CONCAT(oh.path, ' -> ', e.name) AS path
    FROM employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true) */
    employee_id,
    name,
    title,
    level,
    path
FROM org_hierarchy
ORDER BY employee_id;
```

**結果：**

```
+-------------+---------+----------------------+-------+----------------------------------+
| employee_id | name    | title                | level | path                             |
+-------------+---------+----------------------+-------+----------------------------------+
|           1 | Alicia  | CEO                  |     1 | Alicia                           |
|           2 | Bob     | CTO                  |     2 | Alicia -> Bob                    |
|           3 | Carol   | CFO                  |     2 | Alicia -> Carol                  |
|           4 | David   | VP of Engineering    |     3 | Alicia -> Bob -> David           |
|           5 | Eve     | VP of Research       |     3 | Alicia -> Bob -> Eve             |
|           6 | Frank   | VP of Finance        |     3 | Alicia -> Carol -> Frank         |
|           7 | Grace   | Engineering Manager  |     4 | Alicia -> Bob -> David -> Grace  |
|           8 | Heidi   | Tech Lead            |     4 | Alicia -> Bob -> David -> Heidi  |
|           9 | Ivan    | Research Manager     |     4 | Alicia -> Bob -> Eve -> Ivan     |
|          10 | Judy    | Senior Engineer      |     5 | Alicia -> Bob -> David -> Grace -> Judy |
+-------------+---------+----------------------+-------+----------------------------------+
```

### 例 2：複数の再帰 CTE

1 つのクエリで複数の再帰 CTE を定義できます：

```sql
WITH RECURSIVE
cte1 AS (
    SELECT CAST(1 AS BIGINT) AS n
    UNION ALL
    SELECT n + 1 FROM cte1 WHERE n < 5
),
cte2 AS (
    SELECT CAST(10 AS BIGINT) AS n
    UNION ALL
    SELECT n + 1 FROM cte2 WHERE n < 15
)
SELECT /*+ SET_VAR(enable_recursive_cte=true) */
    'cte1' AS source,
    n
FROM cte1
UNION ALL
SELECT 
    'cte2' AS source,
    n
FROM cte2
ORDER BY source, n;
```
