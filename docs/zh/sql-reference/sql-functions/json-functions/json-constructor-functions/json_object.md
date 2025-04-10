---
displayed_sidebar: docs
---

# json_object

## 功能

接收键值集合，并返回一个包含这些键值对的 JSON 类型的对象（以下简称JSON对象），并按照键的字典序排列。

## 语法

```Plain Text
JSON_OBJECT(key, value, ...)
```

## 参数说明

- `key`: JSON 对象的键。支持的数据类型为 VARCHAR。

- `value`: JSON 对象的值。支持的数据类型为字符串型（STRING、VARCHAR、CHAR）、JSON、数字型（TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DOUBLE、FLOAT）、BOOLEAN、以及NULL值。

## 返回值说明

返回一个JSON 对象。

> 如果参数`key`和`value`的总数为奇数，则最后一个字段的值会补为 null。

## 示例

示例一：构造一个由多种数据类型组成的 JSON 对象。

```Plain Text
mysql> SELECT JSON_OBJECT('name', 'starrocks', 'active', true, 'published', 2020);
       -> {"active": true, "name": "starrocks", "published": 2020}            
```

示例二：构造一个多层嵌套的 JSON 对象。

```Plain Text
mysql> SELECT JSON_OBJECT('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));
       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

示例三：构造一个空的JSON对象。

```Plain Text
mysql> SELECT JSON_OBJECT();
       -> {}
```
