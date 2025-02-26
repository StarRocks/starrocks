---
displayed_sidebar: docs
---

# to_json

Map または Struct 値を JSON 文字列に変換します。入力値が NULL の場合、NULL が返されます。

他のデータ型の値をキャストしたい場合は、[cast](./cast.md) を参照してください。

この関数は v3.1 以降でサポートされています。

## 構文

```Haskell
to_json(any_value)
```

## パラメータ

`any_value`: 変換したい Map または Struct 式です。入力値が無効な場合、エラーが返されます。Map または Struct 値の各キーと値のペアの値は nullable です。最後の例を参照してください。

## 戻り値

JSON 値を返します。

## 例

```Haskell
select to_json(map{1:'a',2:'b'});
+---------------------------+
| to_json(map{1:'a',2:'b'}) |
+---------------------------+
| {"1": "a", "2": "b"}      |
+---------------------------+

select to_json(row('asia','eu'));
+--------------------------------+
| to_json(row('asia', 'eu'))     |
+--------------------------------+
| {"col1": "asia", "col2": "eu"} |
+--------------------------------+

select to_json(map('a', named_struct('b', 1)));
+----------------------------------------+
| to_json(map{'a':named_struct('b', 1)}) |
+----------------------------------------+
| {"a": {"b": 1}}                        |
+----------------------------------------+

select to_json(named_struct("k1", cast(null as string), "k2", "v2"));
+-----------------------------------------------------------------------+
| to_json(named_struct('k1', CAST(NULL AS VARCHAR(65533)), 'k2', 'v2')) |
+-----------------------------------------------------------------------+
| {"k1": null, "k2": "v2"}                                              |
+-----------------------------------------------------------------------+
```

## 関連項目

- [Map data type](../../../data-types/semi_structured/Map.md)
- [Struct data type](../../../data-types/semi_structured/STRUCT.md)
- [Map functions](../../README.md#map-functions)
- [Struct functions](../../README.md#struct-functions)