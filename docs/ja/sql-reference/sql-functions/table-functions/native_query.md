---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# `native_query`

`native_query()` は JDBC クエリ用のテーブル関数です。`SELECT` 文を JDBC catalog に直接送信し、リモートデータベースが返す結果メタデータからスキーマを推論して、その結果を通常の StarRocks のテーブルリレーションとして公開します。

この関数は、JDBC データソースに対するクエリを単一の external table scan として表現できない場合に使用します。たとえば、リモート側で Join を実行したい場合、リモートビューを参照したい場合、データベース固有の述語を使いたい場合、またはリモート側で事前にサブクエリによる絞り込みを行いたい場合です。

## 構文

```SQL
TABLE(<catalog_name>.native_query('<sql>'))
```

## パラメータ

- `catalog_name`: 既存の JDBC catalog 名。
- `sql`: リモート側で実行する 1 つの `SELECT` クエリを含む文字列リテラル。リモート SQL 自体にシングルクォートが含まれる場合は、`''2026-01-01''` のように 2 つ重ねてエスケープします。

## 戻り値

リモートクエリの結果メタデータから推論されたスキーマを持つテーブルを返します。

`native_query()` がリモート結果セットを返した後も、StarRocks はその結果に対してローカルのフィルタ、Join、投影、集約、および `INSERT INTO` 処理を継続して適用できます。

## 使用上の注意

- `native_query()` は JDBC catalog でのみサポートされます。
- `native_query()` は必ず `TABLE()` で呼び出す必要があります。
- `native_query()` は引数を 1 つだけ受け取り、その引数は文字列リテラルでなければなりません。
- 名前付き引数はサポートされていません。
- パススルー SQL は `SELECT` で始まる必要があります。`WITH` クエリおよび `SELECT` 以外の文はサポートされていません。
- テーブルエイリアスはサポートされますが、テーブル関数の後ろに列エイリアスのリストを付けることはできません。出力列名を指定したい場合は、パススルー SQL の中で列エイリアスを定義してください。
- 出力列名は一意である必要があります。リモートクエリが重複した列名を返す場合は、パススルー SQL で一意な列エイリアスを付けてください。
- 現在のユーザーには、対象 JDBC catalog に対する `USAGE` 権限が必要です。

## 例

例 1: JDBC catalog 経由でリモートテーブルをクエリします。

```SQL
SELECT *
FROM TABLE(pg_jdbc.native_query(
    'SELECT c_custkey, c_name, c_nationkey FROM customer'
));
```

例 2: パススルー SQL でデータベース固有のフィルタリングを行い、その後 StarRocks 側で追加フィルタを適用します。

```SQL
SELECT *
FROM TABLE(pg_jdbc.native_query(
    'SELECT id, email FROM app_user WHERE email ILIKE ''%starrocks.com'''
)) AS t
WHERE t.id < 100;
```

例 3: リモートクエリ結果をローカルの StarRocks テーブルと Join します。

```SQL
SELECT local_ids.id, remote_users.name
FROM local_ids
JOIN TABLE(pg_jdbc.native_query(
    'SELECT id, name FROM dim_user'
)) AS remote_users
ON local_ids.id = remote_users.id;
```

例 4: リモートクエリ結果を StarRocks の内部テーブルにロードします。

```SQL
INSERT INTO sr_orders_stage
SELECT order_id, customer_id, amount
FROM TABLE(pg_jdbc.native_query(
    'SELECT order_id, customer_id, amount
     FROM orders
     WHERE order_date >= DATE ''2026-01-01'''
));
```

## 関連トピック

- [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md)

## キーワード

table function, jdbc, native query, jdbc catalog
