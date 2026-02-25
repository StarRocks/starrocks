---
displayed_sidebar: docs
---

# json_array

SQL 配列の各要素を JSON 値に変換し、JSON 値からなる JSON 配列を返します。

:::tip
すべての JSON 関数と演算子は、ナビゲーションおよび [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## Syntax

```Haskell
json_array(value, ...)
```

## Parameters

`value`: SQL 配列の要素。`NULL` 値と次のデータ型のみがサポートされています: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。

## Return value

JSON 配列を返します。

## Examples

Example 1: 異なるデータ型の値からなる JSON 配列を構築します。

```plaintext
mysql> SELECT json_array(1, true, 'starrocks', 1.1);

       -> [1, true, "starrocks", 1.1]
```

Example 2: 空の JSON 配列を構築します。

```plaintext
mysql> SELECT json_array();

       -> []
```