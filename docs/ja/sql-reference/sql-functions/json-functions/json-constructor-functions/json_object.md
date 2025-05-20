---
displayed_sidebar: docs
---

# json_object

## 説明

1つ以上のキーと値のペアをJSONオブジェクトに変換します。このオブジェクトはキーと値のペアで構成され、キーは辞書順にソートされます。

:::tip
すべての JSON 関数と演算子はナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## Syntax

```Haskell
json_object(key, value, ...)
```

## Parameters

<<<<<<< HEAD
- `key`: JSONオブジェクトのキー。VARCHARデータ型のみサポートされます。

- `value`: JSONオブジェクトの値。`NULL` 値と以下のデータ型のみサポートされます: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。
=======
- `key`: JSON オブジェクト内のキー。VARCHAR データ型のみサポートされます。

- `value`: JSON オブジェクト内の値。`NULL` 値と、次のデータ型のみサポートされます: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。
>>>>>>> 6cd234eef0 ([Doc] add link to overview (#58805))

## Return value

JSONオブジェクトを返します。

> キーと値の合計数が奇数の場合、JSON_OBJECT関数は最後のフィールドに `NULL` を埋めます。

## Examples

<<<<<<< HEAD
例 1: 異なるデータ型の値で構成されるJSONオブジェクトを構築します。
=======
Example 1: 異なるデータ型の値で構成される JSON オブジェクトを構築します。
>>>>>>> 6cd234eef0 ([Doc] add link to overview (#58805))

```plaintext
mysql> SELECT json_object('name', 'starrocks', 'active', true, 'published', 2020);

       -> {"active": true, "name": "starrocks", "published": 2020}            
```

<<<<<<< HEAD
例 2: 入れ子になったJSON_OBJECT関数を使用してJSONオブジェクトを構築します。
=======
Example 2: 入れ子になった JSON_OBJECT 関数を使用して JSON オブジェクトを構築します。
>>>>>>> 6cd234eef0 ([Doc] add link to overview (#58805))

```plaintext
mysql> SELECT json_object('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));

       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

<<<<<<< HEAD
例 3: 空のJSONオブジェクトを構築します。
=======
Example 3: 空の JSON オブジェクトを構築します。
>>>>>>> 6cd234eef0 ([Doc] add link to overview (#58805))

```plaintext
mysql> SELECT json_object();

       -> {}
```