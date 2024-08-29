---
displayed_sidebar: docs
---

# array_join

## Description

Concatenates the elements of an array into a string.

## Syntax

```Haskell
array_join(array, sep[, null_replace_str])
```

## Parameters

- `array`: the array whose elements you want to concatenate. Only the ARRAY data type is supported.

- `sep`: the delimiter that is used to separate the concatenated array elements. Only the VARCHAR data type is supported.

- `null_replace_str`: the string that is used to substitute `NULL` values. Only the VARCHAR data type is supported.

## Return value

Returns a value of the VARCHAR data type.

## Usage notes

- The value of the `array` parameter must be a one-dimensional array.

- The `array` parameter does not support DECIMAL values.

- If you set the `sep` parameter to `NULL`, the return value is `NULL`.

- If you do not specify the `null_replace_str` parameter, `NULL` values are discarded.

- If you set the `null_replace_str` parameter to `NULL`, the return value is `NULL`.

## Examples

Example 1: Concatenate the elements of an array. In this example, the `NULL` value in the array is discarded, and the concatenated array elements are separated by underscores (`_`).

```plaintext
mysql> select array_join([1, 3, 5, null], '_');

+-------------------------------+

| array_join([1,3,5,NULL], '_') |

+-------------------------------+

| 1_3_5                         |

+-------------------------------+
```

Example 2: Concatenate the elements of an array. In this example, the `NULL` value in the array is substituted with `NULL` strings, and the concatenated array elements are separated by underscores (`_`).

```plaintext
mysql> select array_join([1, 3, 5, null], '_', 'NULL');

+---------------------------------------+

| array_join([1,3,5,NULL], '_', 'NULL') |

+---------------------------------------+

| 1_3_5_NULL                            |

+---------------------------------------+
```
