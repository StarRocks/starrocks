# json_string

## 功能

将 JSON 类型转化为 JSON 字符串。

## 语法

```SQL
json_string(json_object_expr)
```

## 参数说明

- `json_object_expr`：JSON 对象的表达式，可以是 JSON 类型的列，或者 PARSE_JSON() 等 JSON 函数构造的 JSON 对象。

## 返回值说明

返回 VARCHAR 类型的值。

## 示例

示例一: 将 JSON 对象转化为 JSON 字符串。

```Plain
select json_string('{"Name": "Alice"}');
+----------------------------------+
| json_string('{"Name": "Alice"}') |
+----------------------------------+
| {"Name": "Alice"}                |
+----------------------------------+
```

示例二: 将 PARSE_JSON() 的结果转化为 JSON 字符串。

```Plain
select json_string(parse_json('{"Name": "Alice"}'));
+----------------------------------+
| json_string('{"Name": "Alice"}') |
+----------------------------------+
| {"Name": "Alice"}                |
+----------------------------------+
```
