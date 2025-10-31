---
displayed_sidebar: docs
---

# array_top_n

Returns an array of the top n elements from a given array, sorted according to its natural descending order. If n is larger than the size of the given array, the returned list will be the same size as the input instead of n.

## Syntax

```sql
array_top_n(array, count)
```

## Parameters

- `array`: The array from which to extract the top elements. The array can contain elements of any comparable data type supported by StarRocks.

- `count`: The number of top elements to return, of type INT. Must be a non-negative integer.

## Return value

Returns an array containing the top n elements from the input array, sorted in descending order. The data type of the return value is the same ARRAY type as the input array.

## Usage notes

- Elements are sorted in descending order according to their natural ordering.
- If `count` is 0, an empty array is returned.
- If `count` is negative, an empty array is returned.
- If `count` is larger than the array size, the entire array is returned sorted in descending order.
- When the array parameter is NULL, the result is NULL.
- When the count parameter is NULL, the result is NULL.
- NULL elements within the array are considered smaller than any non-NULL value and will appear at the end if included in the result.

## Examples

Example 1: Get top 3 elements from an integer array

```sql
mysql> SELECT array_top_n([1, 100, 2, 5, 3], 3);
+-----------------------------------+
| array_top_n([1,100,2,5,3], 3)    |
+-----------------------------------+
| [100,5,3]                         |
+-----------------------------------+
```

Example 2: Count larger than array size

```sql
mysql> SELECT array_top_n([1, 100], 5);
+--------------------------+
| array_top_n([1,100], 5)  |
+--------------------------+
| [100,1]                  |
+--------------------------+
```

Example 3: String array

```sql
mysql> SELECT array_top_n(['a', 'zzz', 'zz', 'b', 'g', 'f'], 3);
+----------------------------------------------------+
| array_top_n(['a','zzz','zz','b','g','f'], 3)      |
+----------------------------------------------------+
| ['zzz','zz','g']                                   |
+----------------------------------------------------+
```

Example 4: Count is 0

```sql
mysql> SELECT array_top_n([1, 2, 3], 0);
+--------------------------+
| array_top_n([1,2,3], 0)  |
+--------------------------+
| []                       |
+--------------------------+
```

Example 5: NULL handling

```sql
mysql> SELECT array_top_n([1, NULL, 3, 2], 3);
+---------------------------------+
| array_top_n([1,null,3,2], 3)   |
+---------------------------------+
| [3,2,1]                         |
+---------------------------------+
```

Example 6: With table data

```sql
mysql> CREATE TABLE IF NOT EXISTS test (id INT, arr ARRAY<INT>) PROPERTIES ("replication_num"="1");
mysql> INSERT INTO test VALUES (1, [5, 2, 8, 1, 9]), (2, [10, 3, 7]);
mysql> SELECT id, array_top_n(arr, 2) FROM test;
+------+----------------------+
| id   | array_top_n(arr, 2)  |
+------+----------------------+
|    1 | [9,8]                |
|    2 | [10,7]               |
+------+----------------------+
```