# array_map

## Description

array_map() is a higher-order SQL function that can take lambda functions as arguments.It applies the lambda function to the input arrays and returns a new array.

array_map() can accept multiple arrays and can be nested with lambda functions to capture a variable. For more information about lambda functions, see [Lambda expression](../Lambda_expression.md). This function is supported from v2.5.

The alias is transform().

## Syntax

```Haskell
array_map(lambda_function, arr1, arr2...)
array_map(arr1, arr2..., lambda_function)
```

## Usage notes

- Lambda functions can only be used as the **first** or **last** parameter of a higher-order function. Either position does not affect the calculation.
- The number of arrays must be the same as the input parameters in the lambda function. Otherwise, an error is returned.
- All the arrays must have the same number of elements. Otherwise, an error is returned.

## Examples

Example 1: Increment every element of the array by 100.

```Plain
select array_map(x -> x+100,[1,2,3]);
+------------------------------------+
| array_map(x -> x + 100, [1, 2, 3]) |
+------------------------------------+
| [101,102,103]                      |
+------------------------------------+
```

Example 2: Sum array1 [1,2,3] and array2 [11,12,13] element by element.

```Plain
select array_map((x,y) -> x + y, [1,2,3], [11,12,13]);
+-----------------------------------------------------+
| array_map((x, y) -> x + y, [1, 2, 3], [11, 12, 13]) |
+-----------------------------------------------------+
| [12,14,16]                                          |
+-----------------------------------------------------+
```

Example 3: Determine whether the element in `x` is greater than 1.5. If yes, log(x) is returned for the element. If not, (x+y) is returned for the corresponding element.

```Plain
select array_map((x,y) -> if(x>1.5,log(x),x+y), [1,2,3], [11,12,13]);
+--------------------------------------------------------------------------+
| array_map((x, y) -> if(x > 1.5, log(x), x + y), [1, 2, 3], [11, 12, 13]) |
+--------------------------------------------------------------------------+
| [12,0.6931471805599453,1.0986122886681098]                               |
+--------------------------------------------------------------------------+
```

Example 4: Use array_map() to capture variables. The returned element whose value is 1 indicates the condition is met.

```Plain
-- In the example table, last_avg is the average score in the last exam. score represents the scores of three subjects in the current exam.

+------+----------+------------+
| id   | last_avg | score      |
+------+----------+------------+
|    1 |       55 | [50,60,70] |
|    2 |       73 | [70,65,75] |
|    3 |       89 | [88,92,90] |
+------+----------+------------+

-- Find the score that is higher than the average score of the last exam.
select array_map(x -> x > last_avg, score) from test_tbl;
+--------------------------------------+
| array_map(x -> x > last_avg, score)  |
+--------------------------------------+
| [0,1,1]                              |
| [0,0,1]                              |
| [0,1,1]                              |
+--------------------------------------+
```

Example 5: Use nested lambda functions.

```Plain
select array_map(x -> array_map(x->x+100, x),[[1,2.3],[4,3,2]]);
+-------------------------------------------------------------------+
| array_map(x -> array_map(x -> x + 100, x), [[1, 2.3], [4, 3, 2]]) |
+-------------------------------------------------------------------+
| [[101,102.3],[104,103,102]]                                       |
+-------------------------------------------------------------------+
```

Example 6: An error is returned because the numbers of arrays and lambda parameters are inconsistent. The lambda function requires only one array but two arrays are passed in.

```Plain
select array_map(x -> x,[1],[2,4]);
ERROR 1064 (HY000): Lambda arguments should equal to lambda input arrays.
```
