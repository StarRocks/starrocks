---
displayed_sidebar: docs
---

# variant_query

VARIANT オブジェクト内でパス式によって特定できる要素の値を取得し、VARIANT 型の値として返します。

本関数は、Parquet 形式の Iceberg テーブルから読み取った VARIANT データに対して、サブ要素のナビゲーションおよび抽出を行うために使用されます。

## 構文

```Haskell
variant_query(variant_expr, path)
```

## パラメータ

- `variant_expr`: VARIANT オブジェクトを表す式です。通常は、Iceberg テーブルの VARIANT 列を指定します。

- `path`: VARIANT オブジェクト内の要素を指定するパスを表す式です。文字列型で指定します。パスの構文は JSON Path に類似しています。
  - `$`：ルート要素を表します。
  - `.`：オブジェクトのフィールドにアクセスします。
  - `[index]`：配列要素にアクセスします（0 始まり）。
  - 特殊文字を含むフィールド名は引用符で囲みます（例：`$."field.name"`）。

## 戻り値

VARIANT 型の値を返します。

指定した要素が存在しない場合、またはパス式が不正な場合は、NULL を返します。

## 例

例 1：VARIANT 値のルート要素を取得する。

```SQL
SELECT variant_query(data, '$') AS root_data
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------------------------------------------------------------------------------------+
| root_data                                                                                 |
+-------------------------------------------------------------------------------------------+
| {"commit":{"collection":"app.bsky.graph.follow","operation":"delete","rev":"3lcgs4mw...}  |
+-------------------------------------------------------------------------------------------+
```

例 2：VARIANT オブジェクト内のネストされたフィールドを取得する。

```SQL
SELECT variant_query(data, '$.commit') AS commit_info
FROM bluesky
LIMIT 1;
```

```plaintext
+--------------------------------------------------------------------------------+
| commit_info                                                                    |
+--------------------------------------------------------------------------------+
| {"collection":"app.bsky.graph.follow","operation":"delete","rev":"3lcgs4mw...} |
+--------------------------------------------------------------------------------+
```

例 3：`variant_typeof` を使用してネストされたフィールドの型を確認する。

```SQL
SELECT
    variant_typeof(variant_query(data, '$.commit')) AS commit_type,
    variant_typeof(variant_query(data, '$.time_us')) AS time_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+-----------+
| commit_type | time_type |
+-------------+-----------+
| Object      | Int64     |
+-------------+-----------+
```

例 4：取得結果を SQL 型にキャストする。

```SQL
SELECT CAST(variant_query(data, '$.commit') AS STRING) AS commit_info
FROM bluesky
LIMIT 1;
```

```plaintext
+-----------------------------------------------------------------------------------+
| commit_info                                                                       |
+-----------------------------------------------------------------------------------+
| {"cid":"bafyreia3k...","collection":"app.bsky.feed.repost","operation":"create"...} |
+-----------------------------------------------------------------------------------+
```

例 5：`variant_query` の結果を使用してフィルタリングを行う。

```SQL
SELECT COUNT(*) AS total
FROM bluesky
WHERE variant_query(data, '$.commit.record') IS NOT NULL;
```

```plaintext
+---------+
| total   |
+---------+
| 9500118 |
+---------+
```

## Keywords

VARIANT_QUERY,VARIANT
