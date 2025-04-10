---
displayed_sidebar: docs
---

# array_to_bitmap

## Description

Converts an array into BITMAP values. This function is supported from v2.3.

## Syntax

```Haskell
BITMAP array_to_bitmap(array)
```

## Parameters

`array`: Elements in an array can be of the BIGINT, INT, TINYINT, or SMALLINT type.

## Return value

Returns a value of the BITMAP type.

## Usage notes

- If the data type of elements in the input array is invalid, such as STRING or DECIMAL, an error is returned.

- If an empty array is entered, an empty BITMAP value is returned.

- If `NULL` is entered, `NULL` is returned.

## Examples

Example 1: Convert an array into BITMAP values. This function must be nested in `bitmap_to_array` because BITMAP values cannot be displayed.

```Plain
MySQL > select bitmap_to_array(array_to_bitmap([1,2,3]));
+-------------------------------------------+
| bitmap_to_array(array_to_bitmap([1,2,3])) |
+-------------------------------------------+
| [1,2,3]                                   |
+-------------------------------------------+
```

Example 2: Enter an empty array and an empty array is returned.

```Plain
MySQL > select bitmap_to_array(array_to_bitmap([]));
+--------------------------------------+
| bitmap_to_array(array_to_bitmap([])) |
+--------------------------------------+
| []                                   |
+--------------------------------------+
```

Example 3: Enter `NULL` and `NULL` is returned.

```Plain
MySQL > select array_to_bitmap(NULL);
+-----------------------+
| array_to_bitmap(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```
