---
displayed_sidebar: docs
---

# json_object

## 説明

1 つ以上のキーと値のペアを、キーと値のペアからなる JSON オブジェクトに変換します。キーと値のペアは、キーの辞書順でソートされます。

## 構文

```Haskell
json_object(key, value, ...)
```

## パラメータ

- `key`: JSON オブジェクト内のキー。VARCHAR データ型のみサポートされます。

- `value`: JSON オブジェクト内の値。`NULL` 値と、次のデータ型のみサポートされます: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。

## 戻り値

JSON オブジェクトを返します。

> キーと値の合計数が奇数の場合、JSON_OBJECT 関数は最後のフィールドに `NULL` を埋めます。

## 例

例 1: 異なるデータ型の値からなる JSON オブジェクトを構築します。

```plaintext
mysql> SELECT json_object('name', 'starrocks', 'active', true, 'published', 2020);

       -> {"active": true, "name": "starrocks", "published": 2020}            
```

例 2: ネストされた JSON_OBJECT 関数を使用して JSON オブジェクトを構築します。

```plaintext
mysql> SELECT json_object('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));

       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

例 3: 空の JSON オブジェクトを構築します。

```plaintext
mysql> SELECT json_object();

       -> {}
```