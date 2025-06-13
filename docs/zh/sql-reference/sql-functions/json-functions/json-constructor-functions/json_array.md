---
displayed_sidebar: docs
---

# json_array

将 SQL 数组的每个元素转换为 JSON 值，并返回由这些 JSON 值组成的 JSON 数组。

:::tip
所有的 JSON 函数和运算符都列在导航栏和[概述页面](../overview-of-json-functions-and-operators.md)上

通过[生成列](../../../sql-statements/generated_columns.md)加速查询
:::

## 语法

```Haskell
json_array(value, ...)
```

## 参数

`value`：SQL 数组中的一个元素。仅支持 `NULL` 值和以下数据类型：STRING、VARCHAR、CHAR、JSON、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DOUBLE、FLOAT 和 BOOLEAN。

## 返回值

返回一个 JSON 数组。

## 示例

示例 1：构造一个由不同数据类型的值组成的 JSON 数组。

```plaintext
mysql> SELECT json_array(1, true, 'starrocks', 1.1);

       -> [1, true, "starrocks", 1.1]
```

示例 2：构造一个空的 JSON 数组。

```plaintext
mysql> SELECT json_array();

       -> []
```