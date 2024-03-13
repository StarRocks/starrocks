---
displayed_sidebar: "English"
---

# to_json

## Description

Converts a Map or Struct value into a JSON string. If the input value is NULL, NULL is returned.

If you want to cast values of other data types, see [cast](./cast.md).

This function is supported from v3.1 onwards.

## Syntax

```Haskell
to_json(any_value)
```

## Parameters

`any_value`: the Map or Struct expression you want to convert. If the input value is invalid, an error is returned. The value in each key-value pair of the Map or Struct value is nullable. See the last example.

## Return value

Returns a JSON value.

## Examples

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

## See also

- [Map data type](../../../sql-statements/data-types/Map.md)
- [Struct data type](../../../sql-statements/data-types/STRUCT.md)
- [Map functions](../../function-list.md#map-functions)
- [Struct functions](../../function-list.md#struct-functions)
