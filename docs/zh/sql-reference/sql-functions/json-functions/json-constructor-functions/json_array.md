---
displayed_sidebar: "Chinese"
---

# json_array

## 功能

接收 SQL 数组并返回一个 JSON 类型的数组（以下简称 JSON 数组）。

## 语法

```Plain Text
JSON_ARRAY(value, ...)
```

## 参数说明

`value`: 数组的元素。支持的数据类型为字符串类型 (STRING、VARCHAR、CHAR)、JSON、数字型 (TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DOUBLE、FLOAT)、BOOLEAN，以及 NULL 值。

## 返回值说明

返回一个 JSON 数组。

## 示例

示例一：构造一个由多种数据类型组成的 JSON 数组。

```Plain Text
mysql> SELECT JSON_ARRAY(1, true, 'starrocks', 1.1);
       -> [1, true, "starrocks", 1.1]
```

示例二：构造一个空的 JSON 数组。

```Plain Text
mysql> SELECT JSON_ARRAY();
       -> []
```
