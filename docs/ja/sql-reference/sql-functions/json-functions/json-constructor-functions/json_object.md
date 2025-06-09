---
displayed_sidebar: docs
---

# json_object

1つ以上のキーと値のペアをJSONオブジェクトに変換します。このオブジェクトはキーと値のペアで構成され、キーは辞書順にソートされます。

:::tip
すべての JSON 関数と演算子はナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## 構文

```Haskell
json_object(key, value, ...)
```

## パラメータ

- `key`: JSON オブジェクト内のキー。VARCHAR データ型のみサポートされます。

- `value`: JSON オブジェクト内の値。`NULL` 値と、次のデータ型のみサポートされます: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。

## 戻り値

JSONオブジェクトを返します。

> キーと値の合計数が奇数の場合、JSON_OBJECT関数は最後のフィールドに `NULL` を埋めます。

## 例

例 1: 異なるデータ型の値で構成されるJSONオブジェクトを構築します。

```plaintext
mysql> SELECT json_object('name', 'starrocks', 'active', true, 'published', 2020);

       -> {"active": true, "name": "starrocks", "published": 2020}            
```

例 2: 入れ子になったJSON_OBJECT関数を使用してJSONオブジェクトを構築します。

```plaintext
mysql> SELECT json_object('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));

       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

例 3: 空のJSONオブジェクトを構築します。

```plaintext
mysql> SELECT json_object();

       -> {}
```
