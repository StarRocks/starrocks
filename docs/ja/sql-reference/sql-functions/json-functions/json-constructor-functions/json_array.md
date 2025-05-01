---
displayed_sidebar: docs
---

# json_array

## 説明

SQL 配列の各要素を JSON 値に変換し、JSON 値からなる JSON 配列を返します。

## 構文

```Haskell
json_array(value, ...)
```

## パラメータ

`value`: SQL 配列の要素。`NULL` 値と以下のデータ型のみがサポートされています: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, および BOOLEAN。

## 戻り値

JSON 配列を返します。

## 例

例 1: 異なるデータ型の値からなる JSON 配列を構築します。

```plaintext
mysql> SELECT json_array(1, true, 'starrocks', 1.1);

       -> [1, true, "starrocks", 1.1]
```

例 2: 空の JSON 配列を構築します。

```plaintext
mysql> SELECT json_array();

       -> []
```