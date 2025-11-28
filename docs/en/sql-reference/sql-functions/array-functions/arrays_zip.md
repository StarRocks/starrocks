---
displayed_sidebar: docs
---

# arrays_zip

Merges the given arrays, element-wise, into a single array of rows. The M-th element of the N-th argument will be the N-th field of the M-th output element. If the arguments have an uneven length, missing values are filled with NULL.

## Syntax

```sql
arrays_zip(array1, array2[, ...])
```

## Parameters

- `array1`, `array2`, ...: Multiple arrays to merge. Each array can contain elements of any type supported by StarRocks.

## Return value

Returns an array containing elements where each element is a row (struct) containing the corresponding elements from all input arrays. If input arrays have different lengths, shorter arrays are padded with NULL values.

## Usage notes

- If the input arrays have different lengths, the result array length is the maximum length among input arrays.
- Missing values in shorter arrays are filled with NULL.
- If any input array is NULL, the result is NULL.
- If all input arrays are empty, the result is an empty array.

## Examples

Example 1: Basic zip operation with equal length arrays

```sql
mysql> SELECT arrays_zip([1, 2], ['a', 'b']) AS result;
+---------------------------------------------------+
| result                                            |
+---------------------------------------------------+
| [{"col1":"1","col2":"a"},{"col1":"2","col2":"b"}] |
+---------------------------------------------------+
```

Example 2: Zip with different length arrays

```sql
mysql> SELECT arrays_zip([1, 2], ['a', null, 'c']) AS result;
+----------------------------------------------------------+
| result                                                   |
+----------------------------------------------------------+
| [{"col1":"1","col2":"a"},{"col1":"2","col2":null},{"col1":null,"col2":"c"}] |
+----------------------------------------------------------+
```

Example 3: Zip with three arrays

```sql
mysql> SELECT arrays_zip([1, 2, 3], ['a', 'b', 'c'], [10, 20, 30]) AS result;
+------------------------------------------------------------------------------------------+
| result                                                                                   |
+------------------------------------------------------------------------------------------+
| [{"col1":"1","col2":"a","col3":"10"},{"col1":"2","col2":"b","col3":"20"},{"col1":"3","col2":"c","col3":"30"}] |
+------------------------------------------------------------------------------------------+
```

Example 4: Zip with empty arrays

```sql
mysql> SELECT arrays_zip([], []) AS result;
+--------+
| result |
+--------+
| []     |
+--------+
```

Example 5: Single array

```sql
mysql> SELECT arrays_zip([1, 2, 3]) AS result;
+----------------------------------+
| result                           |
+----------------------------------+
| [{"col1":"1"},{"col1":"2"},{"col1":"3"}] |
+----------------------------------+
```

Example 6: NULL handling

```sql
mysql> SELECT arrays_zip(null, [1, 2]) AS result;
+--------+
| result |
+--------+
| NULL   |
+--------+
```

Example 7: Mixed types with NULL values

```sql
mysql> SELECT arrays_zip([1, null, 3], ['a', 'b', null]) AS result;
+----------------------------------------------------------+
| result                                                   |
+----------------------------------------------------------+
| [{"col1":"1","col2":"a"},{"col1":null,"col2":"b"},{"col1":"3","col2":null}] |
+----------------------------------------------------------+
```

Example 8: With table data

```sql
mysql> CREATE TABLE IF NOT EXISTS test (id INT, arr1 ARRAY<INT>, arr2 ARRAY<VARCHAR>) PROPERTIES ("replication_num"="1");
mysql> INSERT INTO test VALUES (1, [1, 2], ['a', 'b']), (2, [3, 4, 5], ['x', 'y']);
mysql> SELECT id, arrays_zip(arr1, arr2) AS result FROM test;
+------+-------------------------------------------------------+
| id   | result                                                |
+------+-------------------------------------------------------+
|    1 | [{"col1":"1","col2":"a"},{"col1":"2","col2":"b"}]        |
|    2 | [{"col1":"3","col2":"x"},{"col1":"4","col2":"y"},{"col1":"5","col2":null}] |
+------+-------------------------------------------------------+
```