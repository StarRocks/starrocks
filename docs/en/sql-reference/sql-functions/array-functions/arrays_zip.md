---
displayed_sidebar: docs
---

# arrays_zip

Merges the given arrays by index into an array of structs in which the n-th struct contains n-th values of all input arrays. When array lengths differ, missing elements are filled with NULL.

## Syntax

```sql
arrays_zip(array_1, array_2[, ...])
```

## Parameters

- `array_n`: Arrays to be merged. Each array can contain elements of any supported type.

## Return value

Returns an ARRAY of STRUCT, where the n-th struct contains the n-th field of each input array. If input arrays have different lengths, shorter arrays are padded with NULL values.

## Usage notes

- If the input arrays have different lengths, the result array length is the maximum length among input arrays.
- Missing values in shorter arrays are filled with NULL.
- If any input array is NULL, the result is NULL.
- If all input arrays are empty, the result is an empty array.

## Examples

Example 1: Merge two arrays with equal length.

```sql
mysql> SELECT arrays_zip([1, 2], ['a', 'b']) AS result;
+---------------------------------------------------+
| result                                            |
+---------------------------------------------------+
| [{"col1":"1","col2":"a"},{"col1":"2","col2":"b"}] |
+---------------------------------------------------+
```

Example 2: Merge two arrays with different lengths.

```sql
mysql> SELECT arrays_zip([1, 2], ['a', null, 'c']) AS result;
+-----------------------------------------------------------------------------+
| result                                                                      |
+-----------------------------------------------------------------------------+
| [{"col1":"1","col2":"a"},{"col1":"2","col2":null},{"col1":null,"col2":"c"}] |
+-----------------------------------------------------------------------------+
```

Example 3: Merge three arrays.

```sql
mysql> SELECT arrays_zip([1, 2, 3], ['a', 'b', 'c'], [10, 20, 30]) AS result;
+---------------------------------------------------------------------------------------------------------------+
| result                                                                                                        |
+---------------------------------------------------------------------------------------------------------------+
| [{"col1":"1","col2":"a","col3":"10"},{"col1":"2","col2":"b","col3":"20"},{"col1":"3","col2":"c","col3":"30"}] |
+---------------------------------------------------------------------------------------------------------------+
```

Example 4: Merge two empty arrays.

```sql
mysql> SELECT arrays_zip([], []) AS result;
+--------+
| result |
+--------+
| []     |
+--------+
```

Example 5: Merge a single array.

```sql
mysql> SELECT arrays_zip([1, 2, 3]) AS result;
+------------------------------------------+
| result                                   |
+------------------------------------------+
| [{"col1":"1"},{"col1":"2"},{"col1":"3"}] |
+------------------------------------------+
```

Example 6: Merge arrays when one of them is NULL.

```sql
mysql> SELECT arrays_zip(null, [1, 2]) AS result;
+--------+
| result |
+--------+
| NULL   |
+--------+
```

Example 7: Merge arrays of mixed types with NULL values.

```sql
mysql> SELECT arrays_zip([1, null, 3], ['a', 'b', null]) AS result;
+-----------------------------------------------------------------------------+
| result                                                                      |
+-----------------------------------------------------------------------------+
| [{"col1":"1","col2":"a"},{"col1":null,"col2":"b"},{"col1":"3","col2":null}] |
+-----------------------------------------------------------------------------+
```

Example 8: Merge arrays from table data.

```sql
mysql> CREATE TABLE IF NOT EXISTS test (id INT, arr1 ARRAY<INT>, arr2 ARRAY<VARCHAR>) PROPERTIES ("replication_num"="1");
mysql> INSERT INTO test VALUES (1, [1, 2], ['a', 'b']), (2, [3, 4, 5], ['x', 'y']);
mysql> SELECT id, arrays_zip(arr1, arr2) AS result FROM test;
+------+----------------------------------------------------------------------------+
| id   | result                                                                     |
+------+----------------------------------------------------------------------------+
|    1 | [{"col1":"1","col2":"a"},{"col1":"2","col2":"b"}]                          |
|    2 | [{"col1":"3","col2":"x"},{"col1":"4","col2":"y"},{"col1":"5","col2":null}] |
+------+----------------------------------------------------------------------------+
```