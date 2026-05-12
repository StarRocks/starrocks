---
displayed_sidebar: docs
---

# native_query

`native_query` は JDBC catalog のテーブル関数です。この関数は JDBC catalog を通じてソースデータベースでネイティブの `SELECT` 文を実行し、その結果を StarRocks のリレーションとして返します。

ソースデータベース側で、単一の JDBC external table に対するクエリでは表現しにくい SQL を実行する必要がある場合に使用します。例えば、事前にフィルタリングしたサブクエリ、ソースデータベース内の Join、またはベンダー固有の SQL 構文を使用する場合です。`native_query` がリレーションを返した後、StarRocks 側でさらにフィルター、Join、集計、射影を適用したり、[INSERT INTO](../../sql-statements/loading_unloading/INSERT.md) で結果をロードしたりできます。

この関数は v4.1 以降でサポートされています。

## 構文

```SQL
SELECT ...
FROM TABLE(<jdbc_catalog>.native_query('<select_sql>')) [AS] <alias>
[WHERE ...];
```

## パラメーター

### `jdbc_catalog`

[JDBC catalog](../../../data_source/catalog/jdbc_catalog.md) の名前です。この関数は JDBC catalog でのみサポートされます。

### `select_sql`

ソースデータベースで実行されるパススルー SQL 文を含む文字列リテラルです。

StarRocks は `select_sql` の先頭コメントと末尾のセミコロンを削除します。削除後の文は `SELECT` で始まる必要があります。ソースデータベースの SQL 方言、オブジェクト名、引用規則、および関数構文に従って記述してください。

`select_sql` に単一引用符 (`'`) が含まれる場合は、StarRocks SQL 文字列内で 2 つの単一引用符 (`''`) としてエスケープします。

### `alias`

結果リレーションに付ける任意のテーブルエイリアスです。

`AS q(c1, c2)` のように、テーブルエイリアスの後に列エイリアスを指定する形式はサポートされません。列エイリアスが必要な場合は、`SELECT id AS id_alias FROM ...` のように `select_sql` 内で定義します。

## 戻り値

`native_query` は、`select_sql` の JDBC 結果セットメタデータから推論された列を持つリレーションを返します。StarRocks は JDBC catalog のスキーマリゾルバーを使用して、JDBC 列型を StarRocks 列型にマッピングします。

## 使用上の注意

- StarRocks ユーザーには JDBC catalog に対する `USAGE` 権限が必要です。ソースデータベースオブジェクトの権限は、JDBC catalog に設定されたユーザーを使用してリモートデータベース側で確認されます。
- `select_sql` は文字列リテラルであり、唯一の引数である必要があります。名前付き引数はサポートされません。
- 先頭コメントを削除した後、`select_sql` は `SELECT` で始まる必要があります。`WITH`、`INSERT`、`UPDATE`、`DELETE`、またはその他の非 `SELECT` キーワードで始まる文はサポートされません。
- 従来の `<catalog>.system.query(...)` 形式はサポートされません。
- ソースクエリが列を返さない場合、StarRocks は解析時にエラーを返します。

## 例

以下の例では、ソース MySQL データベースに `app.orders` テーブルが存在し、StarRocks に `jdbc0` という JDBC catalog が作成済みであることを前提としています。

例 1: ソース側のサブクエリを実行し、StarRocks 側で外側のフィルターを適用します。

```SQL
SELECT id, name, doubled_score
FROM TABLE(jdbc0.native_query(
    'SELECT id, name, score * 2 AS doubled_score
     FROM app.orders
     WHERE score >= 20'
)) q
WHERE doubled_score < 70
ORDER BY id;
```

例 2: パススルークエリが返した結果を集計します。

```SQL
SELECT category, SUM(score) AS total_score
FROM TABLE(jdbc0.native_query(
    'SELECT category, score
     FROM app.orders
     WHERE status = ''PAID'''
)) q
GROUP BY category
ORDER BY category;
```

例 3: ネイティブクエリの結果を StarRocks テーブルにロードします。

```SQL
INSERT INTO paid_order_summary
SELECT category, SUM(score) AS total_score
FROM TABLE(jdbc0.native_query(
    'SELECT category, score
     FROM app.orders
     WHERE status = ''PAID'''
)) q
GROUP BY category;
```

## サポートされない形式

```SQL
-- 名前付き引数はサポートされません。
SELECT * FROM TABLE(jdbc0.native_query(query => 'SELECT id FROM app.orders'));

-- 従来の system.query エイリアスはサポートされません。
SELECT * FROM TABLE(jdbc0.system.query('SELECT id FROM app.orders'));

-- SQL は SELECT で始まる必要があるため、WITH クエリはサポートされません。
SELECT * FROM TABLE(jdbc0.native_query('WITH q AS (SELECT id FROM app.orders) SELECT * FROM q'));

-- テーブルエイリアスの後に列エイリアスを指定する形式はサポートされません。
SELECT * FROM TABLE(jdbc0.native_query('SELECT id FROM app.orders')) q(id_alias);
```
