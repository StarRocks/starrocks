# array_cum_sum

## Description

Returns the cumulative sums of elements in an array.

## Syntax

```Haskell
array_cum_sum(array(bigint))
array_cum_sum(array(double))
```

## Parameters

`array`: The elements in the array must be of the BIGINT (8-byte signed integer) or DOUBLE (8-byte floating-point number) type.

## Return values

Returns an array.

NULL is returned if the input array is NULL.

## Examples

Example 1: Return the cumulative sums of an array.

```Plain
select array_cum_sum([11, 11, 12]);
+---------------------------+
| array_cum_sum([11,11,12]) |
+---------------------------+
| [11,22,34]                |
+---------------------------+
```

Example 2: Use this function with CAST.

```Plain
select array_cum_sum([cast(11.33 as double),cast(11.11 as double),cast(12.324 as double)]);
+---------------------------------------------------------------------------------------+
| array_cum_sum([CAST(11.33 AS DOUBLE), CAST(11.11 AS DOUBLE), CAST(12.324 AS DOUBLE)]) |
+---------------------------------------------------------------------------------------+
| [11.33,22.439999999999998,34.763999999999996]                                         |
+---------------------------------------------------------------------------------------+
```

Example 3: The input array contains a null.

```Plain
select array_cum_sum([null,1,2]);
+---------------------------------+
| array_cum_sum([null,1,2])       |
+---------------------------------+
| [null,1,3]                      |
+---------------------------------+
```

Example 4: The input array is null.

```Plain
select array_cum_sum(null);
+---------------------------------+
| array_cum_sum(NULL)             |
+---------------------------------+
| NULL                            |
+---------------------------------+
```
