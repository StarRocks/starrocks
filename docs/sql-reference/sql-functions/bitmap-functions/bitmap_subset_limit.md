# bitmap_subset_limit

## Description

Intercepts a specified number of elements from a BITMAP value with element value starting from `start range`. The output elements are a subset of `src`.

This function is mainly used for scenarios such as paginated queries. It is supported from v2.5.

## Syntax

```Haskell
BITMAP bitmap_subset_limit(BITMAP src, BIGINT start_range, BIGINT limit)
```

## Parameters

- `src`: the BITMAP value from which to obtain elements.
- `start_range`: the start range to intercept elements. It must be a BIGINT value. If the specified start range exceeds the maximum element of the BITMAP value and `limit` is positive, NULL is returned. See Example 4.
- `limit`: the number of elements to obtain starting from `start_range`. Negative limits are counted from right to left.

## Return value

Returns a value of the BITMAP type. NULL is returned if any of the input parameters is invalid.

## Usage notes

- The subset elements include `start range`.
- Negative limits are counted from right to left. See example 3.

## Examples

In the following examples, the input of bitmap_subset_limit() is the output of [bitmap_from_string](./bitmap_from_string.md). For example, `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` returns `1, 3, 5, 7, 9`. bitmap_subset_limit() takes this BITMAP value as the input.

Example 1: Obtain 4 elements from the BITMAP value with elements values starting from 1.

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+---------+
|  value  |
+---------+
| 1,3,5,7 |
+---------+
```

Example 2: Obtain 100 elements from the BITMAP value with elements value starting from 1,. The limit exceeds the length of the BITMAP value and all the matching elements are returned.

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

Example 3: Obtain -2 elements from the BITMAP value with elements value starting from 5 (counting from right to left).

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 5, -2)) value;
+-----------+
| value     |
+-----------+
| 3,5       |
+-----------+
```

Example 4: The start range 10 exceeds the maximum element of the BITMAP value `1,3,5,7,9` and limit is positive. NULL is returned.

```Plain
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 10, 15)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```
