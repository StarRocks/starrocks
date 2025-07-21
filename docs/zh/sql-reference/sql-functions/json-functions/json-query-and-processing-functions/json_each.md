---
displayed_sidebar: docs
---

# json_each

将 JSON 对象的最外层元素展开为一组包含在两列中的键值对，并返回一个由每个元素组成一行的表。

:::tip
所有的 JSON 函数和操作符都列在导航栏和 [概览页面](../overview-of-json-functions-and-operators.md)

通过 [生成列](../../../sql-statements/generated_columns.md) 加速查询
:::

## 语法

```Haskell
json_each(json_object_expr)
```

## 参数

`json_object_expr`：表示 JSON 对象的表达式。该对象可以是一个 JSON 列，或者是由 JSON 构造函数（如 PARSE_JSON）生成的 JSON 对象。

## 返回值

返回两列：一列名为 key，另一列名为 value。key 列存储 VARCHAR 值，value 列存储 JSON 值。

## 使用说明

json_each 函数是一个表函数，返回一个表。返回的表是由多行组成的结果集。因此，必须在 FROM 子句中使用 lateral join 将返回的表与原始表连接。lateral join 是必须的，但 LATERAL 关键字是可选的。json_each 函数不能在 SELECT 子句中使用。

## 示例

```plaintext
-- 以一个名为 tj 的表为例。在 tj 表中，j 列是一个 JSON 对象。
mysql> SELECT * FROM tj;
+------+------------------+
| id   | j                |
+------+------------------+
|    1 | {"a": 1, "b": 2} |
|    3 | {"a": 3}         |
+------+------------------+

-- 将 tj 表的 j 列按键和值展开为两列，以获得由多行组成的结果集。在此示例中，使用 LATERAL 关键字将结果集与 tj 表连接。

mysql> SELECT * FROM tj, LATERAL json_each(j);
+------+------------------+------+-------+
| id   | j                | key  | value |
+------+------------------+------+-------+
|    1 | {"a": 1, "b": 2} | a    | 1     |
|    1 | {"a": 1, "b": 2} | b    | 2     |
|    3 | {"a": 3}         | a    | 3     |
+------+------------------+------+-------+
```