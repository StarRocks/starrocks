---
displayed_sidebar: docs
---

# bitmap_subset_in_range

## Description

Intercepts elements from a Bitmap value within the range of `start_range` and `end_range` (exclusive). The output elements are a subset of the Bitmap value.

This function is mainly used for scenarios such as paginated queries. It is supported from v3.1.

## Syntax

```Haskell
BITMAP bitmap_subset_in_range(BITMAP src, BIGINT start_range, BIGINT end_range)
```

## Parameters

- `src`: the Bitmap value from which to obtain elements.
- `start_range`: the start range to intercept elements. It must be a BIGINT value. If the specified start range exceeds the maximum length of the BITMAP value, NULL is returned. See Example 4.
- `end_range`: the end range to intercept elements. It must be a BIGINT value. If `end_range` equals or is less than `start range`, NULL is returned. See Example 3.

## Return value

Returns a value of the BITMAP type. NULL is returned if any of the input parameters is invalid.

## Usage notes

The subset elements include `start_range` but exclude `end_range`. See Example 5.

## Examples

In the following examples, the input of bitmap_subset_in_range() is the output of [bitmap_from_string](./bitmap_from_string.md). For example, `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` returns `1, 3, 5, 7, 9`. bitmap_subset_in_range() takes this BITMAP value as the input.

Example 1: Obtain subset elements from the BITMAP value with elements values in the range of 1 to 4. The values within this range are 1 and 3.

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+-------+
| value |
+-------+
| 1,3   |
+-------+
```

Example 2: Obtain subset elements from the BITMAP value with elements value in the range 1 to 100. The end value exceeds the maximum length of the BITMAP value and all the matching elements are returned.

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

Example 3: NULL is returned because the end range `3` is less than the start range `4`.

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 4, 3)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

Example 4: The start range 10 exceeds the maximum length (5) of the BITMAP value `1,3,5,7,9`. NULL is returned.

```Plain
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 10, 15)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

Example 5: The returned subset includes the start value `1` but excludes the end value `3`.

```plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,4,5,6,7,9'), 1, 3)) value;
+-------+
| value |
+-------+
| 1     |
+-------+
```

## References

[bitmap_subset_limit](./bitmap_subset_limit.md)
