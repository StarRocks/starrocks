---
displayed_sidebar: docs
---

# get_variant_int

从指定路径的 VARIANT 对象中提取整数值。

此函数导航到 VARIANT 值中的指定路径,并将该值作为 BIGINT 返回。如果路径处的值不是整数或无法转换为整数,函数将返回 NULL。

## 语法

```Haskell
BIGINT get_variant_int(variant_expr, path)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列。

- `path`:表示 VARIANT 对象中元素路径的表达式。此参数的值是一个字符串。路径语法类似于 JSON 路径:
  - `$` 表示根元素
  - `.` 用于访问对象字段
  - `[index]` 用于访问数组元素(基于 0 的索引)
  - 包含特殊字符的字段名可以用引号括起来:`$."field.name"`

## 返回值

返回 BIGINT 值。

如果元素不存在、路径无效或值无法转换为整数,函数将返回 NULL。

## 示例

示例 1:从 VARIANT 列提取整数值。

```SQL
SELECT get_variant_int(data, '$.time_us') AS timestamp_us
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+
| timestamp_us     |
+------------------+
| 1733267476040329 |
| 1733267476040803 |
| 1733267476041472 |
+------------------+
```

示例 2:在计算中使用。

```SQL
SELECT
    get_variant_int(data, '$.time_us') / 1000000 AS timestamp_seconds
FROM bluesky
LIMIT 3;
```

```plaintext
+-------------------+
| timestamp_seconds |
+-------------------+
| 1733267476        |
| 1733267476        |
| 1733267476        |
+-------------------+
```

示例 3:使用 GROUP BY 按操作类型统计记录。

```SQL
SELECT
    get_variant_string(data, '$.commit.operation') AS operation,
    COUNT(*) AS count
FROM bluesky
GROUP BY operation;
```

```plaintext
+-----------+---------+
| operation | count   |
+-----------+---------+
| delete    | 420223  |
| update    | 40283   |
| NULL      | 39361   |
| create    | 9500118 |
+-----------+---------+
```

示例 4:在 WHERE 子句中用于过滤。

```SQL
SELECT COUNT(*) AS recent_count
FROM bluesky
WHERE get_variant_int(data, '$.time_us') > 1733267476000000;
```

```plaintext
+--------------+
| recent_count |
+--------------+
| 8234567      |
+--------------+
```

示例 5:当路径不存在或值不是整数时返回 NULL。

```SQL
SELECT
    get_variant_int(data, '$.nonexistent.field') AS missing,
    get_variant_int(data, '$.kind') AS not_an_int
FROM bluesky
LIMIT 1;
```

```plaintext
+---------+-------------+
| missing | not_an_int  |
+---------+-------------+
| NULL    | NULL        |
+---------+-------------+
```

## keyword

GET_VARIANT_INT,VARIANT
