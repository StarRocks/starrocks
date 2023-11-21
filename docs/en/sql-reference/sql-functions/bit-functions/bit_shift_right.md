---
displayed_sidebar: "English"
---

# bit_shift_right_logical

## Description

Shifts the binary representation of a numeric expression to the right by a specified number of bits.

This function performs a **logical right shift,** during which the bit length does not change, the low bit is dropped, and 0 is appended to the high bit despite whether the original bit is positive or negative. **Logical** shifts are unsigned shifts. For example, shifting `10101` by one bit results in `00101`.

bit_shift_right() and bit_shift_right_logical() return the same result for a positive value.

## Syntax

```Haskell
bit_shift_right_logical(value, shift)
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
- If the data type of `value` is a string, the value will be cast to an integer if possible. For example, the string "2.3" will be cast to 2. If the value cannot be cast to an integer, the value will be treated as NULL. See [Examples](#examples).

## Examples

Use this function to shift numeric values.

```Plain
SELECT bit_shift_right_logical(2, 1);
+-------------------------------+
| bit_shift_right_logical(2, 1) |
+-------------------------------+
|                             1 |
+-------------------------------+

SELECT bit_shift_right_logical(2.2, 1);
+---------------------------------+
| bit_shift_right_logical(2.2, 1) |
+---------------------------------+
|                               1 |
+---------------------------------+

SELECT bit_shift_right_logical("2", 1);
+---------------------------------+
| bit_shift_right_logical('2', 1) |
+---------------------------------+
|                               1 |
+---------------------------------+

SELECT bit_shift_right_logical(cast('-2' AS INTEGER(32)), 1);
+-----------------------------------------------+
| bit_shift_right_logical(CAST('-2' AS INT), 1) |
+-----------------------------------------------+
|                                    2147483647 |
+-----------------------------------------------+
```

## References

- [bit_shift_left](bit_shift_left.md)
- [bit_shift_right](bit_shift_right.md)
