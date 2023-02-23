# sub_bitmap

## Description

Intercepts `len` elements from a BITMAP value `src` starting from the position specified by `offset`. The output elements are a subset of `src`.

This function is mainly used for scenarios such as paginated queries. It is supported from v2.5.

## Syntax

```Haskell
BITMAP sub_bitmap(BITMAP src, BIGINT offset, BIGINT len)
```

## Parameters

- `src`: the BITMAP value from which you want to obtain elements.
- `offset`: the starting position. It must be a BIGINT value. If the starting position specified by this parameter exceeds the actual length of the BITMAP value, NULL is returned. See Example 6.
- `len`: the number of elements to obtain. It must be a BIGINT value greater than or equal to 1. If the number of matching elements is less than the value of `len`, all the matching elements are returned. See Examples 2, 3, and 7.

## Return value

Returns a value of the BITMAP type. NULL is returned if any of the input parameters is invalid.

## Usage notes

- Offsets start from 0.
- Negative offsets are counted from right to left.
- If the starting position specified by `offset` exceeds the actual length of the BITMAP value, NULL is returned.

## Examples

In the following examples, the input of sub_bitmap() is the output of bitmap_from_string(). For example, `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` returns `1, 3, 5, 7, 9`. sub_bitmap() takes this BITMAP value as the input. For more information about bitmap_from_string(), see [bitmap_from_string](./bitmap_from_string.md).

Example 1: Obtain two elements from the BITMAP value with the offset set to 0.

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 2)) value;
+-------+
| value |
+-------+
| 1,3   |
+-------+
```

Example 2: Obtain 100 elements from the BITMAP value with the offset set to 0. 100 exceeds the length of the BITMAP value and all the matching elements are returned.

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

Example 3: Obtain 100 elements from the BITMAP value with the offset set to -3. 100 exceeds the length of the BITMAP value and all the matching elements are returned.

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), -3, 100)) value;
+-------+
| value |
+-------+
| 5,7,9 |
+-------+
```

Example 4: Obtain two elements from the BITMAP value with the offset set to -3. 

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), -3, 2)) value;
+-------+
| value |
+-------+
| 5,7   |
+-------+
```

Example 5: NULL is returned because `-10` is an invalid input of `len`.

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, -10)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

Example 6: The starting position specified by offset 5 exceeds the length of the BITMAP value `1,3,5,7,9`. NULL is returned.

```Plain
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 5, 1)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

Example 7: `len` is set to 5 but only two elements match the condition. All of these two elements are returned.

```Plain
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), -2, 5)) value;
+-------+
| value |
+-------+
| 7,9   |
+-------+
```
