---
displayed_sidebar: docs
---

# json_object

将一个或多个键值对转换为由这些键值对组成的 JSON 对象。键值对按字典顺序排序。

:::tip
所有的 JSON 函数和操作符都列在导航栏和[概述页面](../overview-of-json-functions-and-operators.md)

通过[生成列](../../../sql-statements/generated_columns.md)加速查询
:::

## 语法

```Haskell
json_object(key, value, ...)
```

## 参数

- `key`：JSON 对象中的键。仅支持 VARCHAR 数据类型。

- `value`：JSON 对象中的值。仅支持 `NULL` 值和以下数据类型：STRING、VARCHAR、CHAR、JSON、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DOUBLE、FLOAT 和 BOOLEAN。

## 返回值

返回一个 JSON 对象。

> 如果键和值的总数是奇数，JSON_OBJECT 函数将在最后一个字段填充 `NULL`。

## 示例

示例 1：构造一个由不同数据类型的值组成的 JSON 对象。

```plaintext
mysql> SELECT json_object('name', 'starrocks', 'active', true, 'published', 2020);

       -> {"active": true, "name": "starrocks", "published": 2020}            
```

示例 2：使用嵌套的 JSON_OBJECT 函数构造一个 JSON 对象。

```plaintext
mysql> SELECT json_object('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));

       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

示例 3：构造一个空的 JSON 对象。

```plaintext
mysql> SELECT json_object();

       -> {}
```