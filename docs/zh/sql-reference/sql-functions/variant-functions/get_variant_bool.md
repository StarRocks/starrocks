---
displayed_sidebar: docs
---

# get_variant_bool

从指定路径的 VARIANT 对象中提取布尔值。

此函数导航到 VARIANT 值中的指定路径,并将该值作为 BOOLEAN 返回。如果路径处的值不是布尔值或无法转换为布尔值,函数将返回 NULL。

## 语法

```Haskell
BOOLEAN get_variant_bool(variant_expr, path)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列。

- `path`:表示 VARIANT 对象中元素路径的表达式。此参数的值是一个字符串。路径语法类似于 JSON 路径:
  - `$` 表示根元素
  - `.` 用于访问对象字段
  - `[index]` 用于访问数组元素(基于 0 的索引)
  - 包含特殊字符的字段名可以用引号括起来:`$."field.name"`

## 返回值

返回 BOOLEAN 值(1 或 0)。

如果元素不存在、路径无效或值无法转换为布尔值,函数将返回 NULL。

## 示例

示例 1:从嵌套字段提取布尔值。

```SQL
SELECT get_variant_bool(variant_col, '$.is_active') AS is_active
FROM iceberg_catalog.db.table_with_variants
LIMIT 3;
```

```plaintext
+-----------+
| is_active |
+-----------+
| 1         |
| 0         |
| 1         |
+-----------+
```

示例 2:从嵌套结构提取布尔值。

```SQL
SELECT
    get_variant_bool(variant_col, '$.settings.enabled') AS enabled,
    get_variant_bool(variant_col, '$.settings.debug') AS debug_mode
FROM iceberg_catalog.db.table_with_variants
LIMIT 3;
```

```plaintext
+---------+------------+
| enabled | debug_mode |
+---------+------------+
| 1       | 0          |
| 1       | 1          |
| 0       | 0          |
+---------+------------+
```

示例 3:在 WHERE 子句中用于过滤。

```SQL
SELECT COUNT(*) AS active_count
FROM iceberg_catalog.db.table_with_variants
WHERE get_variant_bool(variant_col, '$.is_active') = TRUE;
```

```plaintext
+--------------+
| active_count |
+--------------+
| 1234567      |
+--------------+
```

示例 4:与其他 variant 函数结合使用。

```SQL
SELECT
    get_variant_string(variant_col, '$.name') AS name,
    get_variant_bool(variant_col, '$.verified') AS verified
FROM iceberg_catalog.db.table_with_variants
WHERE get_variant_bool(variant_col, '$.verified') = TRUE
LIMIT 5;
```

```plaintext
+------------------+----------+
| name             | verified |
+------------------+----------+
| user123          | 1        |
| alice            | 1        |
| bob_verified     | 1        |
| charlie          | 1        |
| trusted_account  | 1        |
+------------------+----------+
```

示例 5:当路径不存在或值不是布尔值时返回 NULL。

```SQL
SELECT
    get_variant_bool(variant_col, '$.nonexistent.field') AS missing,
    get_variant_bool(variant_col, '$.name') AS not_a_bool
FROM iceberg_catalog.db.table_with_variants
LIMIT 1;
```

```plaintext
+---------+--------------+
| missing | not_a_bool   |
+---------+--------------+
| NULL    | NULL         |
+---------+--------------+
```

## keyword

GET_VARIANT_BOOL,VARIANT
