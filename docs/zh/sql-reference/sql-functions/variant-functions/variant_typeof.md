---
displayed_sidebar: docs
---

# variant_typeof

以字符串形式返回 VARIANT 值的类型名称。

此函数检查 VARIANT 值并以字符串形式返回其类型。这对于理解 VARIANT 数据的结构或基于数据类型进行条件处理非常有用。

## 语法

```Haskell
VARCHAR variant_typeof(variant_expr)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列或由 `variant_query` 返回的 VARIANT 值。

## 返回值

返回表示类型名称的 VARCHAR 值。

可能的返回值包括:
- `"Null"` - 对于 NULL 值
- `"Boolean(true)"` - 对于布尔值 true
- `"Boolean(false)"` - 对于布尔值 false
- `"Int8"`, `"Int16"`, `"Int32"`, `"Int64"` - 对于整数值
- `"Float"`, `"Double"` - 对于浮点值
- `"Decimal4"`, `"Decimal8"`, `"Decimal16"` - 对于不同精度的十进制值
- `"String"` - 对于字符串值
- `"Binary"` - 对于二进制数据
- `"Date"` - 对于日期值
- `"TimestampTz"`, `"TimestampNtz"` - 对于带时区或不带时区的时间戳值
- `"TimestampTzNanos"`, `"TimestampNtzNanos"` - 对于纳秒精度的时间戳值
- `"TimeNtz"` - 对于不带时区的时间值
- `"Uuid"` - 对于 UUID 值
- `"Object"` - 对于结构体或映射值
- `"Array"` - 对于数组值

## 示例

示例 1:获取根级别 VARIANT 值的类型。

```SQL
SELECT variant_typeof(data) AS type
FROM bluesky
LIMIT 1;
```

```plaintext
+--------+
| type   |
+--------+
| Object |
+--------+
```

示例 2:获取嵌套字段的类型。

```SQL
SELECT
    variant_typeof(variant_query(data, '$.kind')) AS kind_type,
    variant_typeof(variant_query(data, '$.did')) AS did_type,
    variant_typeof(variant_query(data, '$.commit')) AS commit_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-----------+----------+-------------+
| kind_type | did_type | commit_type |
+-----------+----------+-------------+
| String    | String   | Object      |
+-----------+----------+-------------+
```

示例 3:检查多个字段的类型。

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

示例 4:在条件逻辑中使用以处理不同类型。

```SQL
SELECT
    CASE variant_typeof(variant_query(data, '$.time_us'))
        WHEN 'Int64' THEN get_variant_int(data, '$.time_us')
        ELSE NULL
    END AS timestamp_value
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+
| timestamp_value  |
+------------------+
| 1733267476040329 |
| 1733267476040803 |
| 1733267476041472 |
+------------------+
```

示例 5:根据类型过滤行。

```SQL
SELECT COUNT(*) AS object_count
FROM bluesky
WHERE variant_typeof(variant_query(data, '$.commit')) = 'Object';
```

```plaintext
+--------------+
| object_count |
+--------------+
| 9960624      |
+--------------+
```

## keyword

VARIANT_TYPEOF,VARIANT
