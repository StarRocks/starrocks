---
displayed_sidebar: docs
---

# is_nan

判断浮点数是否为 NaN（Not a Number）。

## 语法

```Haskell
is_nan(x)
```

## 参数说明

- `x`：浮点类型表达式。支持类型：FLOAT、DOUBLE。

## 返回值说明

返回值类型为 BOOLEAN。

## 使用说明

- 当 `x` 为 `NULL` 时，返回 `NULL`。
- 本函数按照 IEEE 754 语义判断 FLOAT、DOUBLE 是否为 NaN。
- 许多内置数学函数在参数越界时返回 `NULL` 而非 NaN。NaN 更常见于外部数据源（如文件格式）中存在 NaN 的情况，可使用 `is_nan` 对此类数据进行过滤或排查。

## 示例

示例 1：检查普通浮点数。

```sql
mysql> SELECT is_nan(1.0);
+-------------+
| is_nan(1.0) |
+-------------+
|           0 |
+-------------+
```

示例 2：参数为 NULL。

```sql
mysql> SELECT is_nan(NULL);
+---------------+
| is_nan(NULL)  |
+---------------+
|          NULL |
+---------------+
```

