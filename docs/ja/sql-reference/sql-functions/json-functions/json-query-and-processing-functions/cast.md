---
displayed_sidebar: docs
---

# cast

値を JSON 型と SQL 型の間で変換します。

## 構文

- JSON から SQL への変換

```Haskell
cast(json_expr AS sql_data_type)
```

- SQL から JSON への変換

```Haskell
cast(sql_expr AS JSON)
```

## パラメータ

- `json_expr`: SQL 値に変換したい JSON 値を表す式。

- `sql_data_type`: JSON 値を変換したい SQL データ型。STRING、VARCHAR、CHAR、BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DOUBLE、FLOAT のみがサポートされています。

- `sql_expr`: JSON 値に変換したい SQL 値を表す式。このパラメータは、`sql_data_type` パラメータでサポートされているすべての SQL データ型をサポートします。

## 戻り値

- `cast(json_expr AS sql_data_type)` 構文を使用した場合、キャスト関数は `sql_data_type` パラメータで指定された SQL データ型の値を返します。

- `cast(sql_expr AS JSON)` 構文を使用した場合、キャスト関数は JSON 値を返します。

## 使用上の注意

- SQL から JSON への変換

  - SQL 値が JSON でサポートされている精度を超える場合、キャスト関数は算術オーバーフローを防ぐために `NULL` を返します。

  - SQL 値が `NULL` の場合、キャスト関数は SQL 値 `NULL` を JSON 値の `NULL` に変換しません。戻り値は依然として SQL 値の `NULL` です。

- JSON から SQL への変換

  - キャスト関数は、互換性のある JSON と SQL データ型間の変換のみをサポートします。たとえば、JSON 文字列を SQL 文字列に変換できます。

  - キャスト関数は、互換性のない JSON と SQL データ型間の変換をサポートしません。たとえば、JSON 数値を SQL 文字列に変換すると、関数は `NULL` を返します。

  - 算術オーバーフローが発生した場合、キャスト関数は SQL 値の `NULL` を返します。

  - JSON 値の `NULL` を SQL 値に変換すると、関数は SQL 値の `NULL` を返します。

  - JSON 文字列を VARCHAR 値に変換すると、関数は二重引用符 (") で囲まれていない VARCHAR 値を返します。

## 例

例 1: JSON 値を SQL 値に変換します。

```plaintext
-- JSON 値を INT 値に変換します。
mysql> select cast(parse_json('{"a": 1}') -> 'a' as int);
+--------------------------------------------+
| CAST((parse_json('{"a": 1}')->'a') AS INT) |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+

-- JSON 文字列を VARCHAR 値に変換します。
mysql> select cast(parse_json('"star"') as varchar);
+---------------------------------------+
| cast(parse_json('"star"') AS VARCHAR) |
+---------------------------------------+
| star                                  |
+---------------------------------------+

-- JSON オブジェクトを VARCHAR 値に変換します。
mysql> select cast(parse_json('{"star": 1}') as varchar);
+--------------------------------------------+
| cast(parse_json('{"star": 1}') AS VARCHAR) |
+--------------------------------------------+
| {"star": 1}                                |
+--------------------------------------------+

-- JSON 配列を VARCHAR 値に変換します。

mysql> select cast(parse_json('[1,2,3]') as varchar);
+----------------------------------------+
| cast(parse_json('[1,2,3]') AS VARCHAR) |
+----------------------------------------+
| [1, 2, 3]                              |
+----------------------------------------+
```

例 2: SQL 値を JSON 値に変換します。

```plaintext
-- INT 値を JSON 値に変換します。
mysql> select cast(1 as json);
+-----------------+
| cast(1 AS JSON) |
+-----------------+
| 1               |
+-----------------+

-- VARCHAR 値を JSON 値に変換します。
mysql> select cast("star" as json);
+----------------------+
| cast('star' AS JSON) |
+----------------------+
| "star"               |
+----------------------+

-- BOOLEAN 値を JSON 値に変換します。
mysql> select cast(true as json);
+--------------------+
| cast(TRUE AS JSON) |
+--------------------+
| true               |
+--------------------+
```