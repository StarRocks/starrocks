---
displayed_sidebar: docs
description: "判断 VARIANT 值是否为 VARIANT null（例如 JSON null）。"
---

# is_variant_null

判断 VARIANT 值是否为 VARIANT null（例如 JSON null）。

VARIANT null 是一个值：字段存在，且值为 `null`。它与 SQL NULL 不同：当路径不存在时，`variant_query` 返回 SQL NULL。可以使用此函数区分这两种情况。

## 语法

```Haskell
BOOLEAN is_variant_null(variant_expr)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列或由 `variant_query` 返回的 VARIANT 值。

## 返回值

返回 BOOLEAN 值：

- 值为 VARIANT null 时返回 `true`。
- 其他值以及 SQL NULL 返回 `false`。此函数不会返回 NULL。

| 路径上的值 | `variant_query(v, path)` | `variant_typeof(...)` | `is_variant_null(...)` |
| ---------- | ------------------------ | --------------------- | ---------------------- |
| `null`     | VARIANT null             | `Null`                | `true`                 |
| 其他值     | VARIANT 值               | 类型名称              | `false`                |
| 路径不存在 | SQL NULL                 | NULL                  | `false`                |

## 示例

示例 1：区分值为 `null` 的字段和不存在的字段。

```SQL
SELECT
    is_variant_null(variant_query(PARSE_JSON('{"a": null}'), '$.a')) AS json_null,
    is_variant_null(variant_query(PARSE_JSON('{"a": 1}'), '$.a')) AS has_value,
    is_variant_null(variant_query(PARSE_JSON('{"b": 1}'), '$.a')) AS missing;
```

```plaintext
+-----------+-----------+---------+
| json_null | has_value | missing |
+-----------+-----------+---------+
|         1 |         0 |       0 |
+-----------+-----------+---------+
```

示例 2：统计字段 `a` 存在且值为 `null` 的行数。

```SQL
SELECT COUNT(*)
FROM t
WHERE is_variant_null(variant_query(v, '$.a'));
```

## keyword

IS_VARIANT_NULL,VARIANT
