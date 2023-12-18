---
displayed_sidebar: "Chinese"
---

# cast

## 功能

使用 CAST 函数，实现 JSON 类型数据与 SQL 类型间的相互转换。

## 语法

- 将 JSON 类型的数据转为 SQL 类型。

```sql
CAST(json_expr AS sql_data_type)
```

- 将 SQL 类型的数据转为 JSON 类型。

```sql
CAST(sql_expr AS JSON)
```

## 参数说明

- `json_expr`：待转换为 SQL 类型的 JSON 表达式。
- `sql_data_type`：转换后的 SQL 类型，可以是字符串类型（包括 STRING、VARCHAR、CHAR）、BOOLEAN、数值类型（包括 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DOUBLE、FLOAT）。
- `sql_expr`：待转换为 JSON 类型的 SQL 表达式。支持的数据类型同 `sql_data_type`。

## 返回值说明

- 使用 `CAST(json_expr AS sql_data_type)`，返回 `sql_data_type` 指定类型的值。
- 使用 `CAST(sql_expr AS JSON)`，返回 JSON 类型的值。

## 注意事项

将 SQL 类型的数据转为 JSON 类型时:

- 如果数值类型的值超出 JSON 支持的精度，为避免数值溢出，会返回 SQL 类型的 NULL。

- 对于 SQL 类型的 NULL ，不会转为 JSON 类型的 NULL，仍为 SQL 类型的 NULL。

将 JSON 类型的数据转为 SQL 类型时:

- 支持兼容类型的转换，例如 JSON 字符串转为 SQL 字符串。

- 如果类型转换不兼容，例如将 JSON 类型的数字转成 SQL 字符串，则返回 NULL。

- 如果数值类型转型溢出，则返回 SQL 类型的 NULL。

- JSON 类型的 NULL 转为 SQL 类型时，会返回 SQL NULL。

- 将 JSON 类型的数据转为 VARCHAR 类型时，如果转换前为 JSON string 类型，则返回结果为不带引号的 VARCHAR 类型的数据。

## 示例

示例一：将 JSON 类型的数据转为 SQL 类型。

```Plain Text
-- 将 JSON 转为 INT。
mysql> select cast(parse_json('{"a": 1}') -> 'a' as int);
+--------------------------------------------+
| CAST((parse_json('{"a": 1}')->'a') AS INT) |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+

-- 将 JSON 字符串转为 VARCHAR。
mysql> select cast(parse_json('"star"') as varchar);
+---------------------------------------+
| CAST(parse_json('"star"') AS VARCHAR) |
+---------------------------------------+
| star                                  |
+---------------------------------------+

-- 将 JSON 对象转为 VARCHAR。
mysql> select cast(parse_json('{"star": 1}') as varchar);
+--------------------------------------------+
| CAST(parse_json('{"star": 1}') AS VARCHAR) |
+--------------------------------------------+
| {"star": 1}                                |
+--------------------------------------------+

-- 将 JSON 数组转为 VARCHAR。
mysql> select cast(parse_json('[1,2,3]') as varchar);
+----------------------------------------+
| CAST(parse_json('[1,2,3]') AS VARCHAR) |
+----------------------------------------+
| [1, 2, 3]                              |
+----------------------------------------+
```

示例二：将 SQL 类型的数据转为 JSON 类型。

```Plain Text
-- 将INT转成JSON。
mysql> select cast(1 as json);
+-----------------+
| CAST(1 AS JSON) |
+-----------------+
| 1               |
+-----------------+

-- 将 VARCHAR 转为 JSON。
mysql> select cast("star" as json);
+----------------------+
| CAST('star' AS JSON) |
+----------------------+
| "star"               |
+----------------------+

-- 将 BOOLEAN 转为 JSON。
mysql> select cast(true as json);
+--------------------+
| CAST(TRUE AS JSON) |
+--------------------+
| true               |
+--------------------+
```
