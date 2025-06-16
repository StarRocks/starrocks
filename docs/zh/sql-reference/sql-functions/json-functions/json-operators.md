---
displayed_sidebar: docs
---

# JSON 操作符

StarRocks 支持以下 JSON 比较操作符：`<`、`<=`、`>`、`>=`、`=` 和 `!=`。你可以使用这些操作符来查询 JSON 数据。然而，StarRocks 不允许使用 `IN` 来查询 JSON 数据。

- > 操作符的两个操作数必须都是 JSON 值。

- > 如果一个操作数是 JSON 值而另一个不是，在算术运算中，非 JSON 值的操作数会被转换为 JSON 值。有关转换规则的更多信息，请参见 [CAST](./json-query-and-processing-functions/cast.md)。

:::tip
所有的 JSON 函数和操作符都列在导航栏和 [概览页面](./overview-of-json-functions-and-operators.md)

通过 [生成列](../../sql-statements/generated_columns.md) 加速你的查询
:::

## 算术规则

JSON 操作符遵循以下算术规则：

- 当操作符的操作数是相同数据类型的 JSON 值时：
  - 如果两个操作数都是基本数据类型的 JSON 值，如 NUMBER、STRING 或 BOOLEAN，操作符会根据基本数据类型的算术规则执行算术运算。

> 注意：如果两个操作数都是数字，但一个是 DOUBLE 值而另一个是 INT 值，操作符会将 INT 值转换为 DOUBLE 值。

- 如果两个操作数是复合数据类型的 JSON 值，如 OBJECT 或 ARRAY，操作符会根据第一个操作数中键的顺序按字典顺序对操作数中的键进行排序，然后比较操作数之间键的值。

示例 1：

第一个操作数是 `{"a": 1, "c": 2}`，第二个操作数是 `{"b": 1, "a": 2}`。在这个例子中，操作符比较操作数之间键 `a` 的值。第一个操作数中键 `a` 的值是 `1`，而第二个操作数中键 `a` 的值是 `2`。值 `1` 大于值 `2`。因此，操作符得出结论，第一个操作数 `{"a": 1, "c": 2}` 小于第二个操作数 `{"b": 1, "a": 2}`。

```plaintext
mysql> SELECT PARSE_JSON('{"a": 1, "c": 2}') < PARSE_JSON('{"b": 1, "a": 2} ');

       -> 1
```

示例 2：

第一个操作数是 `{"a": 1, "c": 2}`，第二个操作数是 `{"b": 1, "a": 1}`。在这个例子中，操作符首先比较操作数之间键 `a` 的值。操作数中键 `a` 的值都是 `1`。然后，操作符比较操作数之间键 `c` 的值。第二个操作数不包含键 `c`。因此，操作符得出结论，第一个操作数 `{"a": 1, "c": 2}` 大于第二个操作数 `{"b": 1, "a": 1}`。

```plaintext
mysql> SELECT PARSE_JSON('{"a": 1, "c": 2}') < PARSE_JSON('{"b": 1, "a": 1}');

       -> 0
```

- 当操作符的操作数是两种不同数据类型的 JSON 值时，操作符根据以下算术规则比较操作数：NULL < BOOLEAN < ARRAY < OBJECT < DOUBLE < INT < STRING。

```plaintext
mysql> SELECT PARSE_JSON('"a"') < PARSE_JSON('{"a": 1, "c": 2}');

       -> 0
```