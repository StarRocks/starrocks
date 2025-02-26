---
displayed_sidebar: docs
---

# json_each

JSON オブジェクトの最外層の要素をキーと値のペアに展開し、各要素に対して1行のテーブルを返します。

## Syntax

```Haskell
json_each(json_object_expr)
```

## Parameters

`json_object_expr`: JSON オブジェクトを表す式です。このオブジェクトは JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

## Return value

2つのカラムを返します。1つは key という名前で、もう1つは value という名前です。key カラムは VARCHAR 値を格納し、value カラムは JSON 値を格納します。

## Usage notes

json_each 関数はテーブル関数であり、テーブルを返します。返されるテーブルは複数行からなる結果セットです。そのため、元のテーブルに返されたテーブルをジョインするために、FROM 句で Lateral Join を使用する必要があります。Lateral Join は必須ですが、LATERAL キーワードはオプションです。json_each 関数は SELECT 句で使用することはできません。

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

-- tj テーブルの j カラムをキーと値で2つのカラムに展開し、複数行からなる結果セットを取得します。この例では、LATERAL キーワードを使用して結果セットを tj テーブルにジョインしています。

mysql> SELECT * FROM tj, LATERAL json_each(j);
+------+------------------+------+-------+
| id   | j                | key  | value |
+------+------------------+------+-------+
|    1 | {"a": 1, "b": 2} | a    | 1     |
|    1 | {"a": 1, "b": 2} | b    | 2     |
|    3 | {"a": 3}         | a    | 3     |
+------+------------------+------+-------+
```