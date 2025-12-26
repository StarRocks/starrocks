---
displayed_sidebar: docs
---

# get_variant_double

从指定路径的 VARIANT 对象中提取双精度浮点值。

此函数导航到 VARIANT 值中的指定路径,并将该值作为 DOUBLE 返回。如果路径处的值不是数字或无法转换为双精度浮点数,函数将返回 NULL。

## 语法

```Haskell
DOUBLE get_variant_double(variant_expr, path)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列。

- `path`:表示 VARIANT 对象中元素路径的表达式。此参数的值是一个字符串。路径语法类似于 JSON 路径:
  - `$` 表示根元素
  - `.` 用于访问对象字段
  - `[index]` 用于访问数组元素(基于 0 的索引)
  - 包含特殊字符的字段名可以用引号括起来:`$."field.name"`

## 返回值

返回 DOUBLE 值。

如果元素不存在、路径无效或值无法转换为双精度浮点数,函数将返回 NULL。

## 示例

示例 1:从 VARIANT 列提取双精度浮点值。

```SQL
SELECT get_variant_double(variant_col, '$.temperature') AS temperature
FROM iceberg_catalog.db.measurements
LIMIT 3;
```

```plaintext
+-------------+
| temperature |
+-------------+
| 23.5        |
| 24.2        |
| 22.8        |
+-------------+
```

示例 2:从嵌套结构提取多个数值。

```SQL
SELECT
    get_variant_double(variant_col, '$.price') AS price,
    get_variant_double(variant_col, '$.discount') AS discount,
    get_variant_double(variant_col, '$.tax_rate') AS tax_rate
FROM iceberg_catalog.db.products
LIMIT 3;
```

```plaintext
+--------+----------+----------+
| price  | discount | tax_rate |
+--------+----------+----------+
| 99.99  | 0.15     | 0.08     |
| 149.50 | 0.20     | 0.08     |
| 79.95  | 0.10     | 0.08     |
+--------+----------+----------+
```

示例 3:对提取的值执行计算。

```SQL
SELECT
    get_variant_double(variant_col, '$.price') AS original_price,
    get_variant_double(variant_col, '$.price') *
    (1 - get_variant_double(variant_col, '$.discount')) AS final_price
FROM iceberg_catalog.db.products
LIMIT 3;
```

```plaintext
+----------------+-------------+
| original_price | final_price |
+----------------+-------------+
| 99.99          | 84.99       |
| 149.50         | 119.60      |
| 79.95          | 71.96       |
+----------------+-------------+
```

示例 4:在聚合查询中使用。

```SQL
SELECT
    AVG(get_variant_double(variant_col, '$.temperature')) AS avg_temp,
    MAX(get_variant_double(variant_col, '$.temperature')) AS max_temp,
    MIN(get_variant_double(variant_col, '$.temperature')) AS min_temp
FROM iceberg_catalog.db.measurements;
```

```plaintext
+----------+----------+----------+
| avg_temp | max_temp | min_temp |
+----------+----------+----------+
| 23.4     | 28.9     | 18.2     |
+----------+----------+----------+
```

示例 5:当路径不存在或值不是数字时返回 NULL。

```SQL
SELECT
    get_variant_double(variant_col, '$.nonexistent.field') AS missing,
    get_variant_double(variant_col, '$.name') AS not_a_number
FROM iceberg_catalog.db.table_with_variants
LIMIT 1;
```

```plaintext
+---------+---------------+
| missing | not_a_number  |
+---------+---------------+
| NULL    | NULL          |
+---------+---------------+
```

## keyword

GET_VARIANT_DOUBLE,VARIANT
