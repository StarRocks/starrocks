---
displayed_sidebar: docs
---

# json_string

JSON オブジェクトを JSON 文字列に変換する

## 構文

```SQL
json_string(json_object_expr)
```

## パラメータ

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは、JSON カラムまたは PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトである可能性があります。

## 戻り値

VARCHAR 値を返します。

## 例

例 1: JSON オブジェクトを JSON 文字列に変換する

```Plain
select json_string('{"Name": "Alice"}');
+----------------------------------+
| json_string('{"Name": "Alice"}') |
+----------------------------------+
| {"Name": "Alice"}                |
+----------------------------------+
```

例 1: PARSE_JSON の結果を JSON 文字列に変換する

```Plain
select json_string(parse_json('{"Name": "Alice"}'));
+----------------------------------+
| json_string('{"Name": "Alice"}') |
+----------------------------------+
| {"Name": "Alice"}                |
+----------------------------------+
```

:::tip
すべての JSON 関数と演算子は、ナビゲーションおよび [概要ページ](../overview-of-json-functions-and-operators.md) に一覧されています。
:::