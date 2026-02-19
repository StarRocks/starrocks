---
displayed_sidebar: docs
---

# variant_query

查询 VARIANT 对象中可以通过路径表达式定位的元素的值,并返回 VARIANT 值。

此函数用于从 Parquet 格式的 Iceberg 表中读取的 VARIANT 数据中导航和提取子元素。

## 语法

```Haskell
variant_query(variant_expr, path)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列。

- `path`:表示 VARIANT 对象中元素路径的表达式。此参数的值是一个字符串。路径语法类似于 JSON 路径:
  - `$` 表示根元素
  - `.` 用于访问对象字段
  - `[index]` 用于访问数组元素(基于 0 的索引)
  - 包含特殊字符的字段名可以用引号括起来:`$."field.name"`

## 返回值

返回 VARIANT 值。

如果元素不存在或路径无效,函数返回 NULL。

## 示例

示例 1:查询 VARIANT 值的根元素。

```SQL
SELECT variant_query(data, '$') AS root_data
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------------------------------------------------------------------------------------+
| root_data                                                                                 |
+-------------------------------------------------------------------------------------------+
| {"commit":{"collection":"app.bsky.graph.follow","operation":"delete","rev":"3lcgs4mw...}  |
+-------------------------------------------------------------------------------------------+
```

示例 2:查询 VARIANT 对象中的嵌套字段。

```SQL
SELECT variant_query(data, '$.commit') AS commit_info
FROM bluesky
LIMIT 1;
```

```plaintext
+--------------------------------------------------------------------------------+
| commit_info                                                                    |
+--------------------------------------------------------------------------------+
| {"collection":"app.bsky.graph.follow","operation":"delete","rev":"3lcgs4mw...} |
+--------------------------------------------------------------------------------+
```

示例 3:使用 variant_typeof 检查嵌套字段的类型。

```SQL
SELECT
    variant_typeof(variant_query(data, '$.commit')) AS commit_type,
    variant_typeof(variant_query(data, '$.time_us')) AS time_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+-----------+
| commit_type | time_type |
+-------------+-----------+
| Object      | Int64     |
+-------------+-----------+
```

示例 4:将结果转换为 SQL 类型。

```SQL
SELECT CAST(variant_query(data, '$.commit') AS STRING) AS commit_info
FROM bluesky
LIMIT 1;
```

```plaintext
+-----------------------------------------------------------------------------------+
| commit_info                                                                       |
+-----------------------------------------------------------------------------------+
| {"cid":"bafyreia3k...","collection":"app.bsky.feed.repost","operation":"create"...} |
+-----------------------------------------------------------------------------------+
```

示例 5:使用 variant_query 结果进行过滤。

```SQL
SELECT COUNT(*) AS total
FROM bluesky
WHERE variant_query(data, '$.commit.record') IS NOT NULL;
```

```plaintext
+---------+
| total   |
+---------+
| 9500118 |
+---------+
```

## keyword

VARIANT_QUERY,VARIANT
