---
displayed_sidebar: docs
---

# json_array

SQL 配列の各要素を JSON 値に変換し、JSON 値からなる JSON 配列を返します。

## Syntax

```Haskell
json_array(value, ...)
```

## Parameters

`value`: SQL 配列の要素。`NULL` 値と以下のデータ型のみサポートされています: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。

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