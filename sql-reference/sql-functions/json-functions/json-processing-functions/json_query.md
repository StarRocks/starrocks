# JSON_QUERY

## **功能**

查询 JSON 对象中指定路径（`json_path`）的值，并输出为 JSON 类型。

## **语法**

```Plain Text
JSON_QUERY(json_object_expr, json_path)
```

## **参数说明**

- `json_object_expr`：JSON 对象的表达式，可以是 JSON 类型的列，或者 PARSE_JSON 等 JSON 函数构造的 JSON 对象。

- `json_path`: 查询 JSON 对象时的路径。支持的数据类型为字符串。StarRocks 支持的 JSON Path 的语法，请参见 [JSON Path 语法](../json-functions-and-operators.md#JSON%Path)。

## **返回值说明**

返回 JSON 类型的值。

> 如果查询的字段不存在，返回 SQL 类型的 NULL。

## **示例**

示例一：查询 JSON 对象中路径表达式 `'$.a.b'` 指定的值，返回 JSON 类型的 1。

```Plain Text
mysql> SELECT JSON_QUERY(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;
       -> 1
```

示例二：查询 JSON 对象中路径表达式 `'$.a.c'` 指定的值，由于不存在该值，因此返回 SQL 类型的 NULL。

```Plain Text
mysql> SELECT JSON_QUERY(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;
       -> NULL
```

示例三：查询 JSON 对象中路径表达式 `'$.a[2]'` （a 数组的第 2 个元素）指定的值，返回 JSON 类型的 3。

```Plain Text
mysql> SELECT JSON_QUERY(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;
       -> 3
```

示例四：查询 JSON 对象中路径表达式 `'$.a[3]'` （a 数组的第 3 个元素）指定的值，由于不存在该值，因此返回 SQL 类型的 NULL。

```Plain Text
mysql> SELECT JSON_QUERY(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;
       -> NULL
```

## 关键词

JSON, JSON_QUERY
