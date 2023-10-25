# JSON 函数和运算符

StarRocks 支持如下 JSON 构造函数、JSON 查询和处理函数、JSON 运算符以及查询 JSON 对象的 JSON Path。

## JSON 构造函数

JSON 构造函数可以构造 JSON 类型的数据。例如 JSON 类型的对象、JSON 类型的数组等。

| 函数名称                                                     | 功能                                 | 示例                                                      | 返回结果                               |
| ------------------------------------------------------------ | ------------------------------------ | --------------------------------------------------------- | -------------------------------------- |
| [json_object](../../sql-functions/json-functions/json-creation-functions/json_object.md) | 构造 JSON 类型的对象。                 | `SELECT JSON_OBJECT(' Daniel Smith', 26, 'Lily Smith', 25)` | `{"Daniel Smith": 26, "Lily Smith": 25}` |
| [json_array](../../sql-functions/json-functions/json-creation-functions/json_array.md)   | 构造 JSON 类型的数组。                | `SELECT JSON_ARRAY(1, 2, 3)`                                | `[1,2,3]`                                |
| [parse_json](../../sql-functions/json-functions/json-creation-functions/parse_json.md)   | 从字符串解析并构造出 JSON 类型的数据。    | `SELECT PARSE_JSON('{"a": 1}')`                             | `{"a": 1}`                               |

## JSON 查询和处理函数

JSON 查询和处理函数可以查询和处理 JSON 类型的数据。例如查询 JSON 对象中指定路径下的值。

| 函数名称                                                     | 功能                                 | 示例                                                      | 返回结果                              |
| ------------------------------------------------------------ | ------------------------------------ | --------------------------------------------------------- | -------------------------------------- |
| [箭头函数](../../sql-functions/json-functions/json-processing-functions/arrow-function.md) | 查询 JSON 对象中指定路径下的值。                       | `SELECT {"a": {"b": 1}} -> '$.a.b'`                         | `1`                                                            |
| [json_string](../../sql-functions/json-functions/json-processing-functions/json_string.md)   | 将JSON对象转化为JSON字符串。      | `select json_string(parse_json('{"Name": "Alice"}'))` | `{"Name": "Alice"}`  |
| [get_json_double](../../sql-functions/json-functions/json-processing-functions/get_json_double.md)| 解析并获取 json_str 内 json_path 的浮点型内容。      | `SELECT get_json_double('{"k1":1.3, "k2":"2"}', "$.k1");`|  `1.3` |
| [get_json_int](../../sql-functions/json-functions/json-processing-functions/get_json_int.md)| 解析并获取 json_str 内 json_path 的整型内容。      | `SELECT get_json_int('{"k1":1, "k2":"2"}', "$.k1");`|  1 |
| [get_json_string](../../sql-functions/json-functions/json-processing-functions/get_json_string.md)| 解析并获取 json_str 内 json_path 指定的字符串。该函数别名为 get_json_object.      | `SELECT get_json_string('{"k1":"v1", "k2":"v2"}', "$.k1");`| `v1` |
| [json_each](../../sql-functions/json-functions/json-processing-functions/json_each.md)   | 将最外层的 JSON 对象展开为键值对。      | `SELECT * FROM JSON_EACH('{"a": 1, "b":{"c": 3, "d": null}}` | `&ensp;key&ensp;&#124;&ensp;value<br />-----+----<br />&ensp;&ensp;a&ensp;&ensp;&#124;&ensp;&ensp;1<br />&ensp;&ensp;b&ensp;&ensp;&#124; &ensp;{"c": 3, "d": null}`  |
| [json_exists](../../sql-functions/json-functions/json-processing-functions/json_exists.md)| 查询 JSON 对象中是否存在某个值。如果存在，则返回 1；如果不存在，则返回 0。 | `SELECT JSON_EXISTS({"a": 1}, '$.a')`             | `1`                                                            |
| [json_query](../../sql-functions/json-functions/json-processing-functions/json_query.md) | 查询 JSON 对象中指定路径下的值。                             | `SELECT JSON_QUERY({"a": 1}, '$.a')`                      | `1`                                                            |
| [json_string](../../sql-functions/json-functions/json-processing-functions/json_string.md)   | 将 JSON 对象转化为 JSON 字符串。      | `select json_string(parse_json('{"Name": "Alice"}'))` | `{"Name": "Alice"}`  |

## JSON 运算符

StarRocks 支持使用 `<，<=，>，>=， =，!=` 运算符查询 JSON 数据，不支持使用 IN 运算符。JSON 运算符的更多说明，请参见 [JSON 运算符](../../sql-functions/json-functions/json-operators.md)。

## JSON Path

您可以使用 JSON Path 路径表达式，查询 JSON 类型的对象中指定路径的值。JSON Path 为字符串类型，一般结合多种 JSON 函数使用（例如 JSON_QUERY）。目前 StarRocks 中 JSON Path 没有完全遵循 [SQL/JSONPath 标准](https://modern-sql.com/blog/2017-06/whats-new-in-sql-2016#json-path)。StarRocks 中 JSON Path 语法说明，参见下表（以如下 JSON object 为例）。

```JSON
{
    "people": [{
        "name": "Daniel",
        "surname": "Smith"
    }, {
        "name": "Lily",
        "surname": "Smith",
        "active": true
    }]
}
```

| JSON Path 的符号 | 说明                                                         | JSON Path 示例        | 查询上述 JSON 对象的值                                         |
| --------------- | ------------------------------------------------------------ | -------------------- | ------------------------------------------------------------ |
| $               | 表示根节点的对象。                                           | '$'                  | `{ "people": [ { "name": "Daniel", "surname": "Smith" }, { "name": "Lily", "surname": Smith, "active": true } ] }` |
| .               | 表示子节点。                                                 | ' $.people'          | `[ { "name": "Daniel", "surname": "Smith" }, { "name": "Lily", "surname": Smith, "active": true } ]` |
| []              | 表示一个或多个数组下标。[n] 表示选择数组中第 n 个元素，从 0 开始计数。 | '$.people [0]'        | `{ "name": "Daniel", "surname": "Smith" }`                     |
| [*]             | 表示数组中的全部元素。                                       | '$.people[*].name'   | ["Daniel", "Lily"]                                            |
| [start: end]     | 表示数组片段，区间为 [start, end)，不包含 end 代表的元素。       | '$.people[0: 1].name' | ["Daniel"]                                                   |
