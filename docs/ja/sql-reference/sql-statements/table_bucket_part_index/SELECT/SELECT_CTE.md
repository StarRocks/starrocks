---
displayed_sidebar: docs
sidebar_label: "CTE"
---

# Common Table Expression

Common Table Expression (CTE) を使用すると、SQL 文の範囲内で複数回参照可能な一時的な結果セットを定義できます。

## WITH

SELECT ステートメントの前に追加できる句で、SELECT 内で複数回参照される複雑な式のエイリアスを定義します。

CREATE VIEW と似ていますが、句で定義されたテーブル名とカラム名はクエリ終了後も保持されず、実際のテーブルまたは VIEW の名前と競合しません。

WITH 句を使用する利点は次のとおりです。

便利でメンテナンスが容易で、クエリ内の重複を減らすことができます。

クエリの最も複雑な部分を個別のブロックに抽象化することで、SQL コードをより簡単に読み、理解することができます。

例：

```sql
-- UNION ALL クエリの初期段階として、外側のレベルに 1 つのサブクエリを定義し、内側のレベルに別のサブクエリを定義します。

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

## RECURSIVE

v4.1 以降、StarRocks は再帰的 Common Table Expression (CTE) をサポートし、反復的な実行アプローチを使用して、さまざまな階層型およびツリー構造化データを効率的に処理します。

再帰的 CTE は、それ自体を参照できる特殊なタイプの CTE であり、再帰クエリを可能にします。再帰的 CTE は、組織図、ファイルシステム、グラフ走査などの階層型データ構造の処理に特に役立ちます。

再帰的 CTE は、次のコンポーネントで構成されます。

- **アンカーメンバー**: 再帰的ではない初期クエリで、再帰の開始データセットを提供します。
- **再帰メンバー**: CTE 自体を参照する再帰クエリ。
- **終了条件**: 無限再帰を防ぐための条件。通常は WHERE 句を使用して実装されます。

再帰的 CTE の実行プロセスは次のとおりです。

1. アンカーメンバーを実行して、初期結果セット（レベル 0）を取得します。
2. レベル 0 の結果を入力として使用し、再帰メンバーを実行してレベル 1 の結果を取得します。
3. レベル 1 の結果を入力として使用し、再帰メンバーを再度実行してレベル 2 の結果を取得します。
4. 再帰メンバーが行を返さなくなるか、最大再帰深度に達するまで、このプロセスを繰り返します。
5. UNION ALL (または UNION) を使用して、すべてのレベルの結果をマージします。

:::tip
この機能を使用する前に、システム変数 `enable_recursive_cte` を `true` に設定して、この機能を有効にする必要があります。
:::

### 構文

```sql
WITH RECURSIVE cte_name [(column_list)] AS (
    -- Anchor member (non-recursive part)
    anchor_query
    UNION [ALL | DISTINCT]
    -- Recursive member (recursive part)
    recursive_query
)
SELECT ... FROM cte_name ...;
```

### パラメータ

- `cte_name`: CTE の名前。
- `column_list` (オプション): CTE の結果セットの列名のリスト。
- `anchor_query`: 非再帰的で、CTE 自体を参照できない初期クエリ。
- `UNION`: Union operator。
  - `UNION ALL`: すべての行 (重複を含む) を保持します。パフォーマンス向上のため推奨されます。
  - `UNION` または `UNION DISTINCT`: 重複行を削除します。
- `recursive_query`: CTE 自体を参照する再帰クエリ。

### 制限事項

StarRocks の再帰 CTE には、次の制限があります。

- **フィーチャーフラグが必要**

  システム変数 `enable_recursive_cte` を `true` に設定して、再帰 CTE を手動で有効にする必要があります。

- **構造要件**
  - UNION または UNION ALL を使用して、アンカーメンバーと再帰メンバーを接続する必要があります。
  - アンカーメンバーは CTE 自体を参照できません。
  - 再帰メンバーが CTE 自体を参照しない場合、通常の CTE として実行されます。

- **再帰深度の制限**
  - デフォルトでは、再帰の最大深度は 5 (レベル) です。
  - 無限再帰を防ぐために、システム変数 `recursive_cte_max_depth` を使用して最大深度を調整できます。

- **実行制約**
  - 現在、複数レベルのネストされた再帰 CTE はサポートされていません。
  - 複雑な再帰 CTE は、パフォーマンスの低下につながる可能性があります。
  - `anchor_query` の定数は、`recursive_query` の出力タイプと一致するタイプである必要があります。

### 構成

再帰 CTE を使用するには、次のシステム変数が必要です。

| 変数名                      | タイプ   | デフォルト | 説明                                                         |
| -------------------------- | ------- | -------- | ------------------------------------------------------------ |
| `enable_recursive_cte`     | BOOLEAN | false    | 再帰 CTE を有効にするかどうか。                                  |
| `recursive_cte_max_depth`  | INT     | 5        | 無限再帰を防ぐための再帰の最大深度。                              |

### 例

**例 1: 組織階層のクエリ**

組織階層のクエリは、再帰 CTE の最も一般的なユースケースの 1 つです。次の例では、従業員の組織階層の関係をクエリします。

1. データの準備:

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

2. 組織階層のクエリ：

    ```sql
    WITH RECURSIVE org_hierarchy AS (
        -- Anchor member: Start from CEO (employee with no manager)
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
        
        -- Recursive member: Find subordinates at next level
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

結果：

```Plain
+-------------+---------+----------------------+-------+-----------------------------------------+
| employee_id | name    | title                | level | path                                    |
+-------------+---------+----------------------+-------+-----------------------------------------+
|           1 | Alicia  | CEO                  |     1 | Alicia                                  |
|           2 | Bob     | CTO                  |     2 | Alicia -> Bob                           |
|           3 | Carol   | CFO                  |     2 | Alicia -> Carol                         |
|           4 | David   | VP of Engineering    |     3 | Alicia -> Bob -> David                  |
|           5 | Eve     | VP of Research       |     3 | Alicia -> Bob -> Eve                    |
|           6 | Frank   | VP of Finance        |     3 | Alicia -> Carol -> Frank                |
|           7 | Grace   | Engineering Manager  |     4 | Alicia -> Bob -> David -> Grace         |
|           8 | Heidi   | Tech Lead            |     4 | Alicia -> Bob -> David -> Heidi         |
|           9 | Ivan    | Research Manager     |     4 | Alicia -> Bob -> Eve -> Ivan            |
|          10 | Judy    | Senior Engineer      |     5 | Alicia -> Bob -> David -> Grace -> Judy |
+-------------+---------+----------------------+-------+-----------------------------------------+
```

**例 2: 複数の再帰的 CTE**

1 つのクエリで複数の再帰的 CTE を定義できます。

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
