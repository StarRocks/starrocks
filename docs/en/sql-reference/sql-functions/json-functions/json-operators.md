---
displayed_sidebar: "English"
---

# JSON operators

StarRocks supports the following JSON comparison operators: `<`, `<=`, `>`, `>=`, `=`, and `!=`. You can use these operators to query JSON data. However, StarRocks does not allow you to use `IN` to query JSON data.

- > The operands of an operator must both be JSON values.

- > If one operand of an operator is a JSON value while the other is not, the operand that is not a JSON value is converted to a JSON value during the arithmetic operation. For more information about the conversion rules, see [CAST](./json-query-and-processing-functions/cast.md).

## Arithmetic rules

JSON operators comply with the following arithmetic rules:

- When the operands of an operator are JSON values of the same data type:
  - If both operands are JSONs values of a basic data type, such as NUMBER, STRING, or BOOLEAN, the operator performs the arithmetic operation in compliance with the arithmetic rules for the basic data type.

> Note: If both operands are numbers but one is a DOUBLE value and the other is an INT value, the operator converts the INT value to a DOUBLE value.

- If both operands are JSON values of a composite data type, such as OBJECT or ARRAY, the operator sorts the keys in the operands in dictionary order based on the sequence of the keys in the first operand and then compares the values of the keys between the operands.

Example 1:

The first operand is `{"a": 1, "c": 2}`, and the second operand is `{"b": 1, "a": 2}`. In this example, the operator compares the values of the keys `a` between the operands. The value of the key `a` in the first operand is  `1`, whereas the value of the key `a` in the second operand is `2`. The value `1` is greater than the value `2`. Therefore, the operator concludes that the first operand `{"a": 1, "c": 2}` is less than the second operand `{"b": 1, "``a``": 2}`.

```plaintext
mysql> SELECT PARSE_JSON('{"a": 1, "c": 2}') < PARSE_JSON('{"b": 1, "a": 2} ');

       -> 1
```

Example 2:

The first operand is `{"a": 1, "c": 2}`, and the second operand is `{"b": 1, "a": 1}`. In this example, the operator first compares the values of the keys `a` between the operands. The values of the keys `a` in the operands are both  `1`. Then, the operator compares the values of the keys `c` in the operands. The second operand does not contain the key `c`. Therefore, the operator concludes that the first operand `{"a": 1, "c": 2}` is greater than the second operand `{"b": 1, "a": 1}`.

```plaintext
mysql> SELECT PARSE_JSON('{"a": 1, "c": 2}') < PARSE_JSON('{"b": 1, "a": 1}');

       -> 0
```

- When the operands of an operator are JSON values of two distinct data types, the operator compares the operands in compliance with the following arithmetic rules: NULL < BOOLEAN < ARRAY < OBJECT < DOUBLE < INT < STRING.

```plaintext
mysql> SELECT PARSE_JSON('"a"') < PARSE_JSON('{"a": 1, "c": 2}');

       -> 0
```
