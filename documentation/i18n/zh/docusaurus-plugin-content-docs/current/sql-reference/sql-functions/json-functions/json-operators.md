---
displayed_sidebar: "Chinese"
---

# JSON 运算符

StarRocks 支持使用 `<`，`<=`，`>`，`>=`，`=`，`!=` 运算符查询 JSON 数据，不支持使用 `IN` 运算符。

> - 运算符两边必须均为 JSON 类型的数据。
> - 如果运算符一边是 JSON 类型的数据，另一边不是，则运算时会通过隐式类型转换，将不是 JSON 类型的数据转换为 JSON 类型的数据。类型转换规则，请参见 [JSON 类型转换](/sql-reference/sql-functions/json-functions/json-query-and-processing-functions/cast.md)。

## 运算规则

JSON 运算符遵循以下规则：

- 当运算符两边 JSON 数据的值属于相同的数据类型时
  - 如果为基本的数据类型（数字类型、字符串类型、布尔类型)，则运算时，遵循基本类型的运算规则。

   > 如果都是数值类型，但分别为 DOUBLE 和 INT 时，则会将 INT 转型成 DOUBLE 进行比较。

  - 如果为复合数据类型（对象类型、数组类型 ），则运算时，按照元素逐个比较：按 key 的字典序排序，再逐个比较 key 对应的 value。

    比如，对于 JSON 对象 `{"a": 1, "c": 2}` 和 `{"b": 1, "a": 2}`，按照运算符左侧 JSON 对象中键的字典序进行对比。对比节点 `a`，由于左边的值 `1` < 右边的值 `2` ，因此`{"a": 1, "c": 2}` < `{"b": 1, "a": 2}`。

```Plain Text
mysql> SELECT PARSE_JSON('{"a": 1, "c": 2}') < PARSE_JSON('{"b": 1, "a": 2} ');
       -> 1
```

   对于 JSON 对象 `{"a": 1, "c": 2}` 和 `{"b": 1, "a": 1}`，按照运算符左侧 JSON 对象中键的字典序进行对比。首先对比节点 `a`， 左右的值均为 `1`。对比节点 `c`，由于右侧不存在该值，因此 `{"a": 1, "c": 2}` > `{"b": 1, "a": 1}`。

```Plain Text
mysql> SELECT PARSE_JSON('{"a": 1, "c": 2}') < PARSE_JSON('{"b": 1, "a": 1}');
       -> 0
```

- 当运算符两边 JSON 数据的值为不同的数据类型时，运算时，按照类型排序，进行比较。目前类型排序为 NULL < BOOLEAN < ARRAY < OBJECT < DOUBLE < INT < STRING。

```Plain Text
mysql> SELECT PARSE_JSON('"a"') < PARSE_JSON('{"a": 1, "c": 2}');
       -> 0
```
