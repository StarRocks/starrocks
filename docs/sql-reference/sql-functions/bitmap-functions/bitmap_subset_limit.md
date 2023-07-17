# bitmap_subset_limit

## Description

Intercepts elements from a BITMAP value `src` starting from the position of `start range` until `limit` elements . The output elements are a subset of `src`.

This function is mainly used for scenarios such as paginated queries. It is supported from v2.5.

## Syntax

```Haskell
BITMAP bitmap_subset_limit(BITMAP src, BIGINT start_range, BIGINT limit)
```

## Parameters

- `src`: the BITMAP value from which you want to obtain elements.
- `start_range`: the start range we want to intercept elements. It must be a BIGINT value. If the start range specified by this parameter exceeds the maximum element of the BITMAP value and limit is positive, NULL is returned. See Example 4.
- `limit`: the number of elements to obtain from start range to 1.

## Return value

Returns a value of the BITMAP type. NULL is returned if any of the input parameters is invalid.

## Usage notes

- The subset elements include `start range`.
- Negative limit are counted from right to left. See example 3.

## Examples

In the following examples, the input of bitmap_subset_limit() is the output of bitmap_from_string(). For example, `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` returns `1, 3, 5, 7, 9`. bitmap_subset_limit() takes this BITMAP value as the input. For more information about bitmap_from_string(), see [bitmap_from_string](./bitmap_from_string.md).

Example 1: Obtain subset elements from the BITMAP value with elements values from 1, limit 4.

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+---------+
|  value  |
+---------+
| 1,3,5,7 |
+---------+
```

Example 2: Obtain subset elements from the BITMAP value with elements value from 1, limit 100, the limit exceeds the length of the BITMAP value and all the matching elements are returned.

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

Example 3: Obtain subset elements from the BITMAP value with elements value from 5, negative limit -2.

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
