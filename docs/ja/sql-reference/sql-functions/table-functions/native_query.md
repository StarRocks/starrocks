---
displayed_sidebar: docs
description: "native_query は JDBC カタログテーブル関数です。"
---

# native_query

`native_query` は JDBC カタログテーブル関数です。JDBC カタログを通じてデータベースネイティブの `SELECT` ステートメントを実行し、その結果を StarRocks のリレーションとして公開します。

ソースデータベースで単一の JDBC 外部テーブルに対して表現しにくい SQL を実行する必要がある場合にこの関数を使用します。たとえば、事前フィルタリングされたサブクエリ、ソースデータベースでの結合、またはベンダー固有の SQL 構文などです。`native_query` がリレーションを返した後、StarRocks を使用して追加のフィルタ、結合、集計、射影を適用したり、`INSERT INTO` で結果をロードしたりできます。

この関数は v4.1 以降でサポートされています。

## 構文

```SQL
SELECT ...
FROM TABLE(<jdbc_catalog>.native_query('<select_sql>')) [AS] <alias>
[WHERE ...];
```

## パラメータ

### `jdbc_catalog`

JDBC カタログの名前。この関数をサポートするのは JDBC カタログのみです。

### `select_sql`

ソースデータベースが実行するパススルー SQL ステートメントを含む文字列リテラル。

StarRocks が `select_sql` から先頭のコメントと末尾のセミコロンを除去した後、ステートメントは `SELECT` で始まる必要があります。ソースデータベースの SQL 方言、オブジェクト名、クォートルール、および関数を使用してください。

`select_sql` に単一引用符（`'`）が含まれる場合、StarRocks の SQL 文字列内では 2 つの単一引用符（`''`）としてエスケープしてください。

### `alias`

結果リレーションのオプションのテーブルエイリアス。

`AS q(c1, c2)` のようなテーブルエイリアスの後に続く列エイリアスはサポートされていません。列エイリアスは `select_sql` の内部で定義してください。例：`SELECT id AS id_alias FROM ...`。

## 戻り値

`native_query` は、`select_sql` の JDBC 結果セットメタデータから推論された列を持つリレーションを返します。StarRocks は JDBC カタログのスキーマリゾルバーを使用して、JDBC の列型を StarRocks の列型にマッピングします。

## 使用上の注意

- StarRocks ユーザーは JDBC カタログに対する `USAGE` 権限が必要です。ソースデータベースオブジェクトに対する権限は、JDBC カタログで設定されたユーザーによってリモートデータベースが確認します。
- `select_sql` は文字列リテラルでなければならず、唯一の引数である必要があります。名前付き引数はサポートされていません。
- `select_sql` は、先頭のコメントを除去した後に `SELECT` で始まる必要があります。`WITH`、`INSERT`、`UPDATE`、`DELETE`、またはその他の `SELECT` 以外のキーワードで始まるステートメントはサポートされていません。
- レガシーの `<catalog>.system.query(...)` 形式はサポートされていません。
- ソースクエリが列を返さない場合、StarRocks は解析中にエラーを返します。

## 例

以下の例では、ソース MySQL データベースに `app.orders` という名前のテーブルが含まれており、StarRocks に `jdbc0` という名前の JDBC カタログが作成済みであることを前提としています。

例 1: ソース側のサブクエリを実行し、外部の StarRocks フィルタを適用する。

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

例 2: パススルークエリが返した結果を集計する。

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

例 3: ネイティブクエリの結果を StarRocks テーブルにロードする。

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

## サポートされていない形式

```SQL
-- 名前付き引数はサポートされていません。
SELECT * FROM TABLE(jdbc0.native_query(query => 'SELECT id FROM app.orders'));

-- レガシーの system.query エイリアスはサポートされていません。
SELECT * FROM TABLE(jdbc0.system.query('SELECT id FROM app.orders'));

-- SQL は SELECT で始まる必要があるため、WITH クエリはサポートされていません。
SELECT * FROM TABLE(jdbc0.native_query('WITH q AS (SELECT id FROM app.orders) SELECT * FROM q'));

-- テーブルエイリアスの後に続く列エイリアスはサポートされていません。
SELECT * FROM TABLE(jdbc0.native_query('SELECT id FROM app.orders')) q(id_alias);
```
