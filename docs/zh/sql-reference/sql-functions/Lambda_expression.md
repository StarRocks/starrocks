---
displayed_sidebar: docs
sidebar_position: 0.9
---

# Lambda 表达式

Lambda 表达式（Lambda expression）是一种匿名函数，可以作为高阶函数的传入参数，不能单独使用。Lambda 表达式可以让代码更加简洁、紧凑、可扩展。

Lambda 表达式使用运算符 `->` 来表示，读作 “goes to”。运算符左侧为输入参数，右侧为表达式。

从 2.5 版本开始，StarRocks 支持在以下高阶函数 (higher-order function) 中应用 Lambda 表达式：[array_map()](./array-functions/array_map.md)，[array_filter()](./array-functions/array_filter.md)，[array_sum()](./array-functions/array_sum.md)，[array_sortby()](./array-functions/array_sortby.md)。

## 语法

```Haskell
parameter -> expression
```

## 参数说明

`parameter`: Lambda 表达式接收的参数，可接收 0 个、1 个或多个参数。如果参数个数大于等于 2，需要用括号包裹。

`expression`: Lambda 表达式，可以内嵌标量函数。表达式必须为支持输入参数的合法表达式。

## 返回值说明

返回值的类型由 `expression` 的结果类型决定。

## 注意事项

几乎所有的标量函数都可以嵌套在 lamdba 表达式里。有以下几个例外：

- 不支持子查询，例如 `x -> 5 + (SELECT 3)`。
- 不支持聚合函数，例如 `x -> min(y)`。
- 不支持窗口函数。
- 不支持表函数。
- 不支持相关列 (correlated column)。

## 示例

几个 lambda 表达式的简单举例：

```SQL
-- 不需要传入参数, 直接返回 3。
() -> 3    
-- 接收一个数值类型的参数,返回加 2 的结果。
x -> x + 2 
-- 接收两个参数，返回两个参数之和。
(x, y) -> x + y 
-- Lambda 表达式内嵌函数。
x -> COALESCE(x, 0)
x -> day(x)
x -> split(x,",")
x -> if(x>0,"positive","negative")
```

在高阶函数里使用 lambda 表达式：

```Haskell
select array_map((x,y,z) -> x + y, [1], [2], [4]);
+----------------------------------------------+
| array_map((x, y, z) -> x + y, [1], [2], [4]) |
+----------------------------------------------+
| [3]                                          |
+----------------------------------------------+
1 row in set (0.01 sec)
```
