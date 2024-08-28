---
displayed_sidebar: docs
---

# array_sum

## 功能

对数组中的所有元素求和。

从 2.5 版本开始，array_sum() 支持将 lambda 表达式作为输入参数，作为高阶函数使用。array_sum() 不直接支持 lambda，需要对 array_map() 转换后的 array 进行 array_sum() 操作。

有关 Lambda 表达式的详细信息，参见 [Lambda expression](../Lambda_expression.md)。

## 语法

```Haskell
array_sum(array(type))
array_sum(lambda_function, arr1,arr2...) = array_sum(array_map(lambda_function, arr1,arr2...))
```

## 参数说明

- `array(type)`：要进行求和的 array 数组。 数组元素支持如下类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2。

- `lambda_function`: Lambda 表达式。基于这个表达式对数组进行求和。

## 返回值说明

返回数值类型的值。

## 示例

### 不使用 Lambda 表达式

```plain text

select array_sum([11, 11, 12]);
+-----------------------+
| array_sum([11,11,12]) |
+-----------------------+
| 34                    |
+-----------------------+

select array_sum([11.33, 11.11, 12.324]);
+---------------------------------+
| array_sum([11.33,11.11,12.324]) |
+---------------------------------+
| 34.764                          |
+---------------------------------+
```

### 使用 Lambda 表达式

```plain text
-- 先将数组内每个位置对应的元素相乘，再相加。
select array_sum(array_map(x->x*x,[1,2,3]));
+---------------------------------------------+
| array_sum(array_map(x -> x * x, [1, 2, 3])) |
+---------------------------------------------+
|                                          14 |
+---------------------------------------------+
```
