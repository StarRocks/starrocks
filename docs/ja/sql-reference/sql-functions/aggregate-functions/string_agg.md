---
displayed_sidebar: docs
---

# string_agg

## 説明

グループ内の非NULL値を1つの文字列に連結します。この関数は [group_concat](./group_concat.md) 関数のシンタックスシュガーであり、PostgreSQL および SQL Server の同名関数との互換性を提供するように設計されています。その基礎実装は、既存の [group_concat](./group_concat.md) 関数のロジックを完全に再利用しています。

## 構文

```SQL
VARCHAR STRING_AGG([DISTINCT] expr, delimiter
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]])
```

## パラメータ

- `expr`: 連結する値で、NULL値は無視されます。VARCHAR に評価される必要があります。出力文字列から重複する値を排除するには、オプションで `DISTINCT` を指定できます。複数の `expr` を直接連結したい場合は、[concat](../string-functions/concat.md) または [concat_ws](../string-functions/concat_ws.md) を使用してフォーマットを指定してください。
- `delimiter`: 異なる行の非NULL値を連結するために使用される**必須**のセパレータです。VARCHAR 型である必要があります。セパレータを排除するには、空の文字列 `''` を指定してください。
- ORDER BY の項目は、符号なし整数（1から始まる）、列名、または通常の式であることができます。結果はデフォルトで昇順にソートされます。ASC キーワードを明示的に指定することもできます。降順にソートしたい場合は、ソートする列名に DESC キーワードを追加してください。

> **NOTE**
>
> - STRING_AGG は PostgreSQL および SQL Server 互換の関数で、デリミタは第2パラメータとして指定され、必須です。
> - MySQL スタイルの構文（`SEPARATOR` キーワードを使用）の場合は、[group_concat](./group_concat.md) を使用してください。

## 戻り値

各グループに対して文字列値を返し、非NULL値がない場合は NULL を返します。

string_agg が返す文字列の長さを制限するには、[セッション変数](../../System_variable.md) `group_concat_max_len` を設定します。デフォルトは1024です。最小値: 4。単位: 文字。

例:

```sql
SET [GLOBAL | SESSION] group_concat_max_len = <value>;
```

## 例

1. 従業員情報を含むテーブル `employees` を作成します。

   ```sql
   CREATE TABLE `employees` (
     `dept_id` int(11) NULL COMMENT "",
     `name` varchar(255) NULL COMMENT "",
     `salary` int(11) NULL COMMENT ""
   ) ENGINE=OLAP
   DUPLICATE KEY(`dept_id`)
   DISTRIBUTED BY HASH(`dept_id`) BUCKETS 4
   PROPERTIES (
   "replication_num" = "1"
   );

   insert into employees values (1, "Alice", 5000);
   insert into employees values (1, "Bob", 6000);
   insert into employees values (2, "Charlie", 5500);
   insert into employees values (2, "David", 7000);
   insert into employees values (2, NULL, 8000);
   insert into employees values (3, "Eve", NULL);

   select * from employees order by dept_id, name;
   +---------+---------+--------+
   | dept_id | name    | salary |
   +---------+---------+--------+
   |       1 | Alice   |   5000 |
   |       1 | Bob     |   6000 |
   |       2 | Charlie |   5500 |
   |       2 | David   |   7000 |
   |       2 | NULL    |   8000 |
   |       3 | Eve     |   NULL |
   +---------+---------+--------+
   ```

2. string_agg を使用して値を連結します。

   例1: 各部門の従業員名をカンマをデリミタとして連結し、NULL値を無視します。

   ```sql
   select dept_id, string_agg(name, ',') as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Alice,Bob     |
   |       2 | Charlie,David |
   |       3 | Eve           |
   +---------+---------------+
   ```

   例2: ` | ` をデリミタとして従業員名を連結します。

   ```sql
   select dept_id, string_agg(name, ' | ') as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+-----------------+
   | dept_id | names           |
   +---------+-----------------+
   |       1 | Alice | Bob     |
   |       2 | Charlie | David |
   |       3 | Eve             |
   +---------+-----------------+
   ```

   例3: カンマをデリミタとして使用し、重複しない従業員名を連結します。

   ```sql
   insert into employees values (1, "Alice", 5500);
   
   select dept_id, string_agg(distinct name, ',') as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Alice,Bob     |
   |       2 | Charlie,David |
   |       3 | Eve           |
   +---------+---------------+
   ```

   例4: 給与の昇順で従業員名を連結します。

   ```sql
   select dept_id, string_agg(name, ',' order by salary) as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Alice,Bob     |
   |       2 | Charlie,David |
   |       3 | Eve           |
   +---------+---------------+
   ```

   例5: 給与の降順で従業員名を連結します。

   ```sql
   select dept_id, string_agg(name, ',' order by salary desc) as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+---------------+
   | dept_id | names         |
   +---------+---------------+
   |       1 | Bob,Alice     |
   |       2 | David,Charlie |
   |       3 | Eve           |
   +---------+---------------+
   ```

   例6: ネストされた concat() を使用して、"name:salary" 形式で従業員情報を連結します。

   ```sql
   select dept_id, 
          string_agg(concat(name, ':', cast(salary as varchar)), '; ' order by salary) as employee_info
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+-----------------------------+
   | dept_id | employee_info               |
   +---------+-----------------------------+
   |       1 | Alice:5000; Bob:6000        |
   |       2 | Charlie:5500; David:7000    |
   |       3 | Eve:NULL                    |
   +---------+-----------------------------+
   ```

   例7: すべての従業員名をグローバルに連結します（GROUP BY なし）。

   ```sql
   select string_agg(name, ', ' order by name) as all_names 
   from employees;
   +---------------------------+
   | all_names                 |
   +---------------------------+
   | Alice,Bob,Charlie,David,Eve |
   +---------------------------+
   ```

   例8: 一致する結果が見つからず、NULL が返されます。

   ```sql
   select string_agg(name, ',') as names 
   from employees 
   where dept_id > 100;
   +-------+
   | names |
   +-------+
   | NULL  |
   +-------+
   ```

   例9: 返される文字列の長さを10文字に制限します。

   ```sql
   set group_concat_max_len = 10;

   select dept_id, string_agg(name, ',' order by name) as names 
   from employees 
   group by dept_id 
   order by dept_id;
   +---------+------------+
   | dept_id | names      |
   +---------+------------+
   |       1 | Alice,Bob  |
   |       2 | Charlie,Da |
   |       3 | Eve        |
   +---------+------------+
   ```

## GROUP_CONCAT との比較

STRING_AGG と GROUP_CONCAT は同じ機能を提供しますが、構文が異なります：

```sql
-- PostgreSQL スタイル (STRING_AGG)
SELECT STRING_AGG(name, ',' ORDER BY name) FROM employees;

-- MySQL スタイル (GROUP_CONCAT)
SELECT GROUP_CONCAT(name ORDER BY name SEPARATOR ',') FROM employees;
```

どちらも同じ結果を生成します。SQL 方言の好みやアプリケーションの互換性要件に基づいて、どちらを使用するかを選択してください。

## keyword

STRING_AGG, GROUP_CONCAT, CONCAT, ARRAY_AGG, POSTGRESQL
