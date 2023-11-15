# JSON_EACH

## 功能

将 JSON 对象的最外层按照键和值展开为两列，返回一行或多行数据的集合。

## 语法

```Plain Text
JSON_EACH(json_object_expr)
```

## 参数说明

`json_object_expr`：JSON 对象的表达式，可以是 JSON 类型的列，或者 PARSE_JSON 等 JSON 函数构造的 JSON 对象。

## 返回值说明

返回键和值两列。键的列名为 key，值的列名为 value。键、值的列类型分别为 VARCHAR 和 JSON。

## 注意事项

JSON_EACH 属于表函数，返回的是行的集合。因此 JSON_EACH 必须在 FROM 子句中通过 LATERAL 连接使用（LATERAL 关键字可省略），不能用于 SELECT 子句。

## 示例

```Plain Text
-- 本示例以表tj进行说明。
mysql> SELECT * FROM tj;
+------+------------------+
| id   | j                |
+------+------------------+
|    1 | {"a": 1, "b": 2} |
|    3 | {"a": 3}         |
+------+------------------+

-- 表tj中j列为JSON类型的对象，将其按照键和值展开为两列，得到多行数据的集合，然后通过LATERAL连接表tj。
mysql> SELECT * FROM tj, LATERAL JSON_EACH(j);
+------+------------------+------+-------+
| id   | j                | key  | value |
+------+------------------+------+-------+
|    1 | {"a": 1, "b": 2} | a    | 1     |
|    1 | {"a": 1, "b": 2} | b    | 2     |
|    3 | {"a": 3}         | a    | 3     |
+------+------------------+------+-------+
```

## 关键词

JSON, JSON_EACH
