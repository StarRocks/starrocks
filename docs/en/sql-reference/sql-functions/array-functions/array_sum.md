---
displayed_sidebar: docs
---

# array_sum

## Description

Sums all the elements in an array.

From StarRocks 2.5, array_sum() can take a lambda expression as an argument. However, it cannot work directly with the Lambda expression. It must work on the result converted from [array_map()](./array_map.md).

## Syntax

```Haskell
array_sum(array(type))
array_sum(lambda_function, arr1,arr2...) = array_sum(array_map(lambda_function, arr1,arr2...))
```

## Parameters

- `array(type)`: the array you want to calculate the sum. Array elements support the following data types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, and DECIMALV2.
- `lambda_function`: the lambda expression used to calculate the target array for array_sum().

## Return value

Returns a numeric value.

## Examples

### Use array_sum without lambda function

```plain text
mysql> select array_sum([11, 11, 12]);
+-----------------------+
| array_sum([11,11,12]) |
+-----------------------+
| 34                    |
+-----------------------+

mysql> select array_sum([11.33, 11.11, 12.324]);
+---------------------------------+
| array_sum([11.33,11.11,12.324]) |
+---------------------------------+
| 34.764                          |
+---------------------------------+
```

### Use array_sum with lambda function

```plain text
-- Multiply [1,2,3] by [1,2,3] and sum the elements.
select array_sum(array_map(x->x*x,[1,2,3]));
+---------------------------------------------+
| array_sum(array_map(x -> x * x, [1, 2, 3])) |
+---------------------------------------------+
|                                          14 |
+---------------------------------------------+
```

## keyword

ARRAY_SUM,ARRAY
