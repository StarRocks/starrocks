# bitmap_subset_in_range

## Description

Intercepts elements from a BITMAP value `src` starting from the position of `start range` to the position of `end range` . The output elements are a subset of `src`.

This function is mainly used for scenarios such as paginated queries. It is supported from v2.5.

## Syntax

```Haskell
BITMAP bitmap_subset_in_range(BITMAP src, BIGINT start_range, BIGINT end_range)
```

## Parameters

- `src`: the BITMAP value from which you want to obtain elements.
- `start_range`: the start range we want to intercept elements. It must be a BIGINT value. If the start range specified by this parameter exceeds the maximum element of the BITMAP value, NULL is returned. See Example 4.
- `end_range`: the end range we want to intercept elements. It must be a BIGINT value. If the `end range` equals or less than `start range` NULL is returned. See Examples 3.

## Return value

Returns a value of the BITMAP type. NULL is returned if any of the input parameters is invalid.

## Usage notes

- The subset elements include `start range` but exclude `end range`.

## Examples

In the following examples, the input of bitmap_subset_in_range() is the output of bitmap_from_string(). For example, `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` returns `1, 3, 5, 7, 9`. bitmap_subset_in_range() takes this BITMAP value as the input. For more information about bitmap_from_string(), see [bitmap_from_string](./bitmap_from_string.md).

Example 1: Obtain subset elements from the BITMAP value with elements values from 1 to 4.

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+-------+
| value |
+-------+
| 1,3   |
+-------+
```

Example 2: Obtain subset elements from the BITMAP value with elements value from 1 to 100, the end value exceeds the maximum element value of the BITMAP value and all the matching elements are returned.

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

Example 3: NULL is returned because the end range `3` is less than start range `4`.

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 4, 3)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

Example 6: The start range 10 exceeds the maximum element of the BITMAP value `1,3,5,7,9`. NULL is returned.

```Plain
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 10, 15)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```
