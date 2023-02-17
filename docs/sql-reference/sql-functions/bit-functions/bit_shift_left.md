# bit_shift_left

## Description

Shifts the binary representation of a numeric expression to the left by a specified number of bits.

This function performs an **arithmetic left shift**, during which the bit length does not change, 0 is appended to the end, and the high bit remains unchanged. For example, shifting `10101` to the left by one bit results in `11010`.

## Syntax

```Haskell
bit_shift_left(value, shift)
```

## Parameters

`value`: the value or numeric expression to shift. Supported data types are TINYINT, SMALLINT, INT, BIGINT, and LARGEINT.

`shift`: the number of bits to shift. The supported data type is BIGINT.

## Return value

Returns a value of the same type as `value`.

## Usage notes

- If any input parameter is NULL, NULL is returned.
- If `shift` is less than 0, 0 is returned.
- Shifting a `value` by `0` always results in the original `value`.
- Shifting `0` by a `shift` always results in `0`.
- If the data type of `value` is numeric but not an integer, that value will be cast to an integer. See [Examples](#examples).
- If the data type of `value` is a string, the value will be cast to an integer if possible. If the value cannot be cast to an integer, it will be processed as NULL. See [Examples](#examples).

## Examples

Use this function to shift numeric values.

```Plain
SELECT bit_shift_left(2, 1);
+----------------------+
| bit_shift_left(2, 1) |
+----------------------+
|                    4 |
+----------------------+

SELECT bit_shift_left(2.2, 1);
+------------------------+
| bit_shift_left(2.2, 1) |
+------------------------+
|                      4 |
+------------------------+

SELECT bit_shift_left("2", 1);
+------------------------+
| bit_shift_left('2', 1) |
+------------------------+
|                      4 |
+------------------------+

SELECT bit_shift_left(-2, 1);
+-----------------------+
| bit_shift_left(-2, 1) |
+-----------------------+
|                    -4 |
+-----------------------+
```

## References

- [bit_shift_right](bit_shift_right.md)
- [bit_shift_right_logical](bit_shift_right_logical.md)
