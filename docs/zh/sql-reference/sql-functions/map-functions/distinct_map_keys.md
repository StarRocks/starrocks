---
displayed_sidebar: docs
---

# distinct_map_keys

## 功能

删除 Map 中重复的 Key。从语义上讲，Map 中的 Key 不可以重复，Value 可以重复。对于 Key 相同的键值对，该函数仅保留最后一个出现的键值对 (LAST WIN 原则)。比如，`SELECT distinct_map_keys(map{1:3,1:'4'});` 返回 `{1:"4"}`。

该函数可用于在查询外部 MAP 数据时，对 Map 中的 Key 进行去重。StarRocks 原生表会自动对 Map 中重复的 Key 进行去重。

该函数从 3.1 版本开始支持。

## 语法

```Haskell
distinct_map_keys(any_map)
```

## 参数说明

`any_map`: Map 表达式。

## 返回值说明

返回 Key 值不重复的新 Map。

如果输入值为 NULL，则返回 NULL。

## 示例

示例一：简单用法。

```plain
select distinct_map_keys(map{"a":1,"a":2});
+-------------------------------------+
| distinct_map_keys(map{'a':1,'a':2}) |
+-------------------------------------+
| {"a":2}                             |
+-------------------------------------+
```

示例二：从外表中查询 Map 数据并删除 `col_map` 列中重复的 Key。`unique` 列为返回的结果。

```plain
select distinct_map_keys(col_map) as unique, col_map from external_table;
+---------------+---------------+
|      unique   | col_map       |
+---------------+---------------+
|       {"c":2} | {"c":1,"c":2} |
|           NULL|          NULL |
| {"e":4,"d":5} | {"e":4,"d":5} |
+---------------+---------------+
3 rows in set (0.05 sec)
```
