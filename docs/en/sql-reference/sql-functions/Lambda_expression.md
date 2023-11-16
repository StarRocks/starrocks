---
displayed_sidebar: "English"
---

# Lambda expression

Lambda expressions are anonymous functions that can be passed as parameters into higher-order SQL functions. Lambda expressions allow you to develop code that is more concise, elegant, and extensible.

Lambda expressions are written with the `->` operator, which reads "goes to". The left side of `->` is the input parameters (if any) and the right side is an expression.

From v2.5 onwards, StarRocks supports using lambda expressions in the following higher-order SQL functions: [array_map()](./array-functions/array_map.md), [array_filter()](./array-functions/array_filter.md), [array_sum()](./array-functions/array_sum.md), and [array_sortby()](./array-functions/array_sortby.md).

## Syntax

```Haskell
parameter -> expression
```

## Parameters

- `parameter`: the input parameters for the lambda expression, which can accept zero, one, or more parameters. Two or more input parameters are enclosed in parentheses.

- `expression`: a simple expression that references `parameter`. The expression must be valid for the input parameters.

## Return value

The type of the return value is determined by the result type of `expression`.

## Usage notes

Almost all scalar functions can be used in a lambda body. But there are a few exceptions:

- Subqueries are not supported, for example, `x -> 5 + (SELECT 3)`.
- Aggregate functions are not supported, for example, `x -> min(y)`.
- Window functions are not supported.
- Table functions are not supported.
- Correlated columns cannot occur in lambda functions.

## Examples

Simple examples of lambda expressions:

```SQL
-- Accepts no parameters and returns 5.
() -> 3    
-- Takes x and returns the value of (x + 2).
x -> x + 2 
-- Takes x and y, and returns their sum.
(x, y) -> x + y 
-- Takes x and applies a function to x.
x -> COALESCE(x, 0)
x -> day(x)
x -> split(x,",")
x -> if(x>0,"positive","negative")
```

An example of using lambda expressions in higher-order functions:

```Haskell
select array_map((x,y,z) -> x + y, [1], [2], [4]);
+----------------------------------------------+
| array_map((x, y, z) -> x + y, [1], [2], [4]) |
+----------------------------------------------+
| [3]                                          |
+----------------------------------------------+
1 row in set (0.01 sec)
```
