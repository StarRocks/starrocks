---
displayed_sidebar: docs
---

# json_object

1 つ以上のキーと値のペアを JSON オブジェクトに変換します。このオブジェクトはキーと値のペアで構成され、キーは辞書順にソートされます。

:::tip
すべての JSON 関数と演算子はナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## Syntax

```Haskell
json_object(key, value, ...)
```

## Parameters

- `key`: JSON オブジェクト内のキー。VARCHAR データ型のみサポートされます。

- `value`: JSON オブジェクト内の値。`NULL` 値と、次のデータ型のみサポートされます: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。

## Return value

JSON オブジェクトを返します。

> キーと値の合計数が奇数の場合、JSON_OBJECT 関数は最後のフィールドに `NULL` を埋めます。

## Examples

Example 1: 異なるデータ型の値で構成される JSON オブジェクトを構築します。

```plaintext
mysql> SELECT json_object('name', 'starrocks', 'active', true, 'published', 2020);

       -> {"active": true, "name": "starrocks", "published": 2020}            
```

Example 2: 入れ子になった JSON_OBJECT 関数を使用して JSON オブジェクトを構築します。

```plaintext
mysql> SELECT json_object('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));

       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

Example 3: 空の JSON オブジェクトを構築します。

```plaintext
mysql> SELECT json_object();

       -> {}
```