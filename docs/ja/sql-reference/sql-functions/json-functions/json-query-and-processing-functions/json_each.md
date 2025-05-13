---
displayed_sidebar: docs
---

# json_each

JSON オブジェクトの最外層の要素をキーと値のペアに展開し、各要素ごとに1行のテーブルを返します。

:::tip
すべての JSON 関数と演算子はナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## Syntax

```Haskell
json_each(json_object_expr)
```

## Parameters

`json_object_expr`: JSON オブジェクトを表す式です。オブジェクトは JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

## Return value

2つのカラムを返します。1つは key という名前で、もう1つは value という名前です。key カラムは VARCHAR 値を格納し、value カラムは JSON 値を格納します。

## Usage notes

json_each 関数はテーブル関数であり、テーブルを返します。返されるテーブルは複数行からなる結果セットです。そのため、元のテーブルに返されたテーブルを結合するために lateral join を FROM 句で使用する必要があります。lateral join は必須ですが、LATERAL キーワードはオプションです。json_each 関数は SELECT 句で使用することはできません。

## Examples

```plaintext
-- tj という名前のテーブルを例として使用します。tj テーブルでは、j カラムが JSON オブジェクトです。
mysql> SELECT * FROM tj;
+------+------------------+
| id   | j                |
+------+------------------+
|    1 | {"a": 1, "b": 2} |
|    3 | {"a": 3}         |
+------+------------------+

-- tj テーブルの j カラムをキーと値で2つのカラムに展開し、複数行からなる結果セットを取得します。この例では、LATERAL キーワードを使用して結果セットを tj テーブルに結合しています。

mysql> SELECT * FROM tj, LATERAL json_each(j);
+------+------------------+------+-------+
| id   | j                | key  | value |
+------+------------------+------+-------+
|    1 | {"a": 1, "b": 2} | a    | 1     |
|    1 | {"a": 1, "b": 2} | b    | 2     |
|    3 | {"a": 3}         | a    | 3     |
+------+------------------+------+-------+
```